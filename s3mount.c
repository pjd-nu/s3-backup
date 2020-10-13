/*
 * Peter Desnoyers, August 2020
 *
 * - Geoff's notes on FUSE: https://www.cs.hmc.edu/~geoff/classes/hmc.cs135.201001/homework/fuse/fuse_doc.html
 * - argument parsing: https://www.cs.hmc.edu/~geoff/classes/hmc.cs135.201001/homework/fuse/fuse_doc.html
 */

#define FUSE_USE_VERSION 27
#define _FILE_OFFSET_BITS 64

#include <stdlib.h>
#include <stddef.h>
#include <unistd.h>
#include <fuse.h>
#include <fcntl.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <avl.h>
#include <argp.h>
#include <libs3.h>
#include <pthread.h>
#include <uuid/uuid.h>

#include "s3fs.h"

struct s3super s;
struct fuse_context _tmp;

/* constants.
 */
#define MAX_PATH_LEN 32

struct state {
    /* argument parsing
     */
    const char *target;         /* bucket/key or filename */
    const char *host;
    const char *access_key;
    const char *secret_key;
    const char *logfile;
    FILE       *log_fp;
    int         local;
    int         use_http;
    int         nocache;
    
    char      **names;          /* version history */
    off_t      *nsectors;       /* length of each  */
    int         nvers;
    
    /* locks
     */
    pthread_mutex_t s3_mutex;
    
    /* S3 stuff
     */
    const char *bucket;
    const char *object_key;
    S3Status    status;         /* from last operation */
    char       *msg;            /* on error */
    int         retries;        /* retry/backoff on S3 failure */
    int         sleep;
    S3BucketContext bkt_ctx;

} _state;

/* to allow multiple outstanding reads...
 */
struct s3recv {
    void         *recv_buf;       /* receive buffer */
    int           recv_wanted;
    int           recv_got;
    off_t        content_length; /* for HEAD */
    struct state *s;
};
    
/* ------------ argument parsing, global option data -------- */

/* usage: s3backfs [-o options] bucket/key directory
 * S3 parameters can be passed in S3_HOSTNAME, S3_ACCESS_KEY_ID,
 * and S3_SECRET_ACCESS_KEY, or in mount options:
 *       -o host=H,access=A,secret=S
 * other options:
 *       -o local - first argument specifies a local file
 */


static struct fuse_opt opts[] = {
    {"host=%s",   offsetof(struct state, host), 0 },       /* S3 host */
    {"access=%s", offsetof(struct state, access_key), 0 }, /* access key */
    {"secret=%s", offsetof(struct state, secret_key), 0 }, /* secret key */
    {"local",     offsetof(struct state, local), 1 },      /* file (debug) */
    {"http",      offsetof(struct state, use_http), 1 },   /* not https */
    {"log=%s",    offsetof(struct state, logfile), 0 },    /* stats etc. */
    {"nocache",   offsetof(struct state, nocache), 1 },   /* disable data cache */
    FUSE_OPT_END
};

/* the first non-option argument is bucket/key
 * (or filename if -o local)
 */
static int myfs_opt_proc(void *data, const char *arg, 
                         int key, struct fuse_args *outargs)
{
    struct state *s = data;
    if (key == FUSE_OPT_KEY_NONOPT && s->target == NULL) {
        s->target = arg;
        return 0;
    }
    return 1;
}

/* ------------ debug stuff ------------ */

/* for debug purposes, handle local filenames by translating name -> fd
 * total hack, will crash w/ over 128 filenames.
 * also not that useful without a VPATH-like mechanism
 */
struct name_fd {
    const char *name;
    int   fd;
} fd_cache[128];

int get_fd(struct state *s, const char *file)
{
    int i;

    for (i = 0; fd_cache[i].name != NULL; i++)
        if (!strcmp(file, fd_cache[i].name)) 
            return (fd_cache[i].fd);

    fd_cache[i].name = strdup(file);
    if ((fd_cache[i].fd = open(file, O_RDONLY)) < 0) {
        fprintf(stderr, "Can't open %s : %s\n", file, strerror(errno));
        exit(1);
    }
    return fd_cache[i].fd;
}

/* ------------ S3 functions ------------- */

static void print_error(struct state *s)
{
    if (s->status < S3StatusErrorAccessDenied) {
        fprintf(stderr, "\nERROR: %s\n", S3_get_status_name(s->status));
    }
    else {
        fprintf(stderr, "\nERROR: %s\n", S3_get_status_name(s->status));
        fprintf(stderr, "%s\n", s->msg);
    }
}

static void response_complete(S3Status status,
                              const S3ErrorDetails *error,
                              void *data)
{
    struct s3recv *r = data;
    struct state *s = r->s;
    s->status = status;

    if (error != NULL) {
        char *template = "Message: %s\n"
            "Resource: %s\n"
            "Further Details: %s\n";
        const char *m = error->message ? error->message : "";
        const char *r = error->resource ? error->resource : "";
        const char *f = error->furtherDetails ? error->furtherDetails : "";

        if (s->msg != NULL)
            free(s->msg);
        int len = strlen(template) + strlen(m) + strlen(r) + strlen(f);
        s->msg = malloc(len+20);
        sprintf(s->msg, template, m, r, f);
    }
}

int should_retry(struct state *s)
{
    if (s->retries--) {
        printf("retrying...\n");
        sleep(s->sleep);
        s->sleep++;
        return 1;
    }
    return 0;
}

S3Status response_properties(const S3ResponseProperties *p, void *data)
{
    struct s3recv *r = data;
    r->content_length = p->contentLength;
    return S3StatusOK;
}

S3Status recv_data_callback(int size, const char *buf, void *data)
{
    struct s3recv *r = data;

    /* don't overrun the buffer - should never happen 
     */
    if (size + r->recv_got > r->recv_wanted)
        return S3StatusAbortedByCallback;

    memcpy(r->recv_buf + r->recv_got, buf, size);
    r->recv_got += size;

    return S3StatusOK;
}

int s3_get_range(struct state *s, const char *key,
                 char *buf, size_t len, off_t offset)
{
    S3GetObjectHandler h = {
        .responseHandler.propertiesCallback = response_properties,
        .responseHandler.completeCallback = response_complete,
        .getObjectDataCallback = recv_data_callback
    };

    struct s3recv r = {.recv_buf = buf,
                       .recv_wanted = len,
                       .recv_got = 0, .s = s};
    
    do {
        S3_get_object(&s->bkt_ctx,
                      key,
                      NULL,     /* no conditions */
                      offset,
                      len,
                      0,        /* requestContext */
                      0,        /* timeoutMs */
                      &h,
                      &r);
    } while (S3_status_is_retryable(s->status) && should_retry(s));

    if (s->status != S3StatusOK) {
        print_error(s);
        exit(1);
    }
    
    return len;
}

/* ---------- functions used by the filesystem logic -------- */

//const char *_filename;

/* no need to return an error, as we exit on all errors.
 * TODO: handle transient errors
 */
void _do_read(struct state *s, const char *key, void *ptr, size_t len, off_t offset)
{
    if (len == 0)
        return;
#if 0
    if (s->log_fp)
        fprintf(s->log_fp, "%s %ld %ld\n", _filename, offset, len);
#endif
    
    if (s->local) {
        int fd = get_fd(s, key);
        if (pread(fd, ptr, len, offset) < 0) {
            fprintf(stderr, "Error reading %s : %s\n", key, strerror(errno));
            exit(1);
        }
    }
    else {
        //pthread_mutex_lock(&s->s3_mutex);
        s3_get_range(s, key, ptr, len, offset);
        //pthread_mutex_unlock(&s->s3_mutex);
    }
}

#define CACHE_SIZE 16
#define CACHE_BLOCK 16*1024*1024

struct data_cache {
    int    seq;                  /* for LRU */
    void  *buf;                  /* data */
    int    version;              /* in which object? */
    off_t  base;                 /* in bytes */
    size_t len;                  /* usually 1MB */
} dcache[CACHE_SIZE];
int seq;
pthread_mutex_t dcache_mutex;

/* returns a pointer to *@nbytes of data starting at @offset
 */
static void *find_block(struct state *s, const char *key, int version, off_t offset, size_t *nbytes)
{
    off_t base = (offset & ~(CACHE_BLOCK-1));
    off_t byte_offset = offset - base;
    int i;

    /* note that we always exit through the first 'if' below, so the
     * lock always gets released.
     */
    pthread_mutex_lock(&dcache_mutex);
again:
    /* is it there? if so, return
     */
    for (i = 0; i < CACHE_SIZE; i++)
        if (dcache[i].buf && dcache[i].base == base && dcache[i].version == version) {
            dcache[i].seq = seq++; /* for LRU */
            *nbytes = (base + dcache[i].len)  - offset;
            pthread_mutex_unlock(&dcache_mutex);
            return dcache[i].buf + byte_offset;
        }

    /* find a free spot
     */
    for (i = 0; i < CACHE_SIZE; i++)
        if (!dcache[i].buf) {
            dcache[i].buf = malloc(CACHE_BLOCK);
            break;
        }

    /* if none free, then LRU
     */
    if (i == CACHE_SIZE) {
        int j, min;
        for (i = j = 0, min = seq; i < CACHE_SIZE; i++)
            if (dcache[i].seq < min) {
                min = dcache[i].seq;
                j = i;
            }
        i = j;
    }
    dcache[i].seq = seq++;
    dcache[i].base = base;
    dcache[i].version = version;
    
    off_t file_max = s->nsectors[version] * 512;
    off_t len = CACHE_BLOCK;
    if (base + len > file_max)
        len = file_max - base;

    dcache[i].len = len;
    
    _do_read(s, key, dcache[i].buf, len, base);
    goto again;
}

void do_read(struct state *s, const char *key, int version, void *ptr, size_t len, off_t offset)
{
    if (s->nocache) {
        _do_read(s, key, ptr, len, offset);
        return;
    }
    
    while (len > 0) {
        size_t nbytes;
        void *cached_data = find_block(s, key, version, offset, &nbytes);
        int to_copy = (nbytes < len) ? nbytes : len;
        memcpy(ptr, cached_data, to_copy);
        ptr += to_copy;
        len -= to_copy;
        offset += to_copy;
    }
}


/* get the size of an object / file. Object name does not include bucket.
 */
off_t do_size(struct state *s, const char *key)
{
    if (s->local) {
        struct stat sb;
        if (stat(key, &sb) < 0) {
            fprintf(stderr, "can't access %s : %s\n", key, strerror(errno));
            exit(1);
        }
        return sb.st_size;
    }

    struct s3recv r = {.recv_buf = 0, .recv_wanted = 0, .recv_got = 0,
                       .content_length = 0, .s = s};
    S3ResponseHandler h = {.propertiesCallback = response_properties,
                           .completeCallback = response_complete};
    do {
        S3_head_object(&s->bkt_ctx,
                       key,
                       NULL,    /* RequestContext */
                       0,       /* timeout */
                       &h,
                       &r);
    } while (S3_status_is_retryable(s->status) && should_retry(s));

    return r.content_length;
}

/* object is specified as "bucket/key". (all linked objects must be in
 * same bucket)
 */
int split_slash(const char *path, const char **bucket, const char **key)
{
    char *tmp = strdup(path);
    *bucket = strsep(&tmp, "/");
    *key = strsep(&tmp, "");
    if (*bucket == NULL || *key == NULL || strsep(&tmp, "") != NULL)
        return 0;
    return 1;
}

/* initialize S3 logic
 */
int s3_init(struct state *s)
{
    S3Status status = S3_initialize("Multi-Version Backup V0.1",
                                    S3_INIT_ALL, s->host);
    if (status != S3StatusOK) {
        fprintf(stderr, "Failed to initialize libs3: %s\n",
                S3_get_status_name(status));
        exit(1);
    }

    if (!split_slash(s->target, &s->bucket, &s->object_key)) {
        fprintf(stderr, "Invalid bucket/key : %s\n", s->target);
        exit(1);
    }

    s->retries = 5;
    s->sleep = 1;
    int protocol = s->use_http ? S3ProtocolHTTP : S3ProtocolHTTPS;
    s->bkt_ctx = (S3BucketContext){ s->host,
                                    s->bucket,
                                    protocol,
                                    S3UriStylePath,
                                    s->access_key,
                                    s->secret_key,
                                    0,   /* security token */
                                    0 }; /* authRegion */
}


/* ------------  ------------ */


char _root[512];
struct s3dirent *root_de = (struct s3dirent*)_root;


/* Directory cache. We fill it at startup, so we don't bother with
 * locking it.
 */

struct dir {
    union s3offset off;
    size_t         nbytes;
    void          *data;
};

int N;

avl_tree_t *dir_cache;
pthread_mutex_t avl_mutex;

int dir_cmp(const void *_d1, const void *_d2)
{
    const struct dir *d1 = _d1, *d2 = _d2;
    int64_t diff = (int64_t)d1->off.n - (int64_t)d2->off.n;
    return (diff == 0) ? 0 : ((diff < 0) ? -1 : 1);
}

void dir_free(void *d)
{
    free(d);
}

void cache_dir(union s3offset off, size_t nbytes, void *data)
{
    if (nbytes == 0)
        return;
    
    struct dir *d = malloc(sizeof(*d));
    d->off = off;
    d->nbytes = nbytes;
    d->data = data;

    avl_node_t *node = avl_search(dir_cache, d);
    node = avl_insert(dir_cache, d);
}

void *find_dir(union s3offset off, size_t nbytes)
{
    if (nbytes == 0)
        return NULL;
    struct dir _d = {.off = off};
    avl_node_t *node = avl_search(dir_cache, &_d);
    if (node == NULL)
        return NULL;
    struct dir *d = node->item;
    return d->data;
}

void verify_uuid(struct state *s, uuid_t uuid, char *name)
{
    char buf[4096];
    struct s3super *super = (void*)buf;
    _do_read(s, name, buf, 4096, 0);
    if (uuid_compare(uuid, super->versions[0].uuid) != 0) {
        printf("ERROR: %s: UUID doesn't match\n", name);
        exit(1);
    }
}

/* init - this is called once by the FUSE framework at startup. Ignore
 * the 'conn' argument.
 * recommended actions:
 *   - read superblock
 *   - allocate memory, read bitmaps and inodes
 */
void* fs_init(struct fuse_conn_info *conn)
{
    struct state *s = &_state;

    if (s->local)
        s->object_key = s->target;
    if (s->logfile) {
        if ((s->log_fp = fopen(s->logfile, "w")) == NULL) {
            fprintf(stderr, "can't open %s : %s\n",
                    s->logfile, strerror(errno));
            exit(1);
        }
    }

    pthread_mutex_init(&s->s3_mutex, NULL);
    pthread_mutex_init(&avl_mutex, NULL);
    pthread_mutex_init(&dcache_mutex, NULL);

    /* read the superblock. 
     */
    char buf[4096];
    struct s3super *super = (void *)buf;
    _do_read(s, s->object_key, super, 4096, 0);

    int nvers = super->nvers;
    struct version *v = super->versions;
    s->names = malloc(nvers * sizeof(char*));
    s->nsectors = malloc(nvers * sizeof(off_t));
    
    for (int i = 0; i < nvers; i++) {
        char *name = calloc(v->namelen+1, 1);
        memcpy(name, v->name, v->namelen);
        verify_uuid(s, v->uuid, name);
        int j = nvers-1-i;
        s->names[j] = name;
        s->nsectors[j] = do_size(s, name) / 512;
        v = next_version(v);
    }
        
    /* initialize the directory cache
     */
    off_t len = do_size(s, s->object_key);
    _do_read(s, s->object_key, _root, 512, len-512);

    struct s3dirent *de = root_de;
    de = next_de(de);
    struct s3dirloc *loc = malloc(de->bytes);
    int ndirs = de->bytes / sizeof(struct s3dirloc);
    _do_read(s, s->object_key, loc, de->bytes, 512*de->off.s.sector);

    de = next_de(de);
    void *dirdata = malloc(de->bytes);
    _do_read(s, s->object_key, dirdata, de->bytes, 512*de->off.s.sector);

    dir_cache = avl_alloc_tree(dir_cmp, dir_free);

    for (size_t i = 0, offset = 0; i < ndirs; i++) {
        if (loc[i].bytes > 0) {
            cache_dir(loc[i].off, loc[i].bytes, dirdata + offset);
            offset += loc[i].bytes;
        }
    }
    free(loc);
    
    printf("init done\n");
    return NULL;
}

/* Note on path translation errors:
 * In addition to the method-specific errors listed below, almost
 * every method can return one of the following errors if it fails to
 * locate a file or directory corresponding to a specified path.
 *
 * ENOENT - a component of the path doesn't exist.
 * ENOTDIR - an intermediate component of the path (e.g. 'b' in
 *           /a/b/c) is not a directory
 */

/* note on splitting the 'path' variable:
 * the value passed in by the FUSE framework is declared as 'const',
 * which means you can't modify it. The standard mechanisms for
 * splitting strings in C (strtok, strsep) modify the string in place,
 * so you have to copy the string and then free the copy when you're
 * done. One way of doing this:
 *
 *    char *_path = strdup(path);
 *    int inum = translate(_path);
 *    free(_path);
 */

/* look up a single directory entry. Returns 0 for success, dirent in *result
 */
int lookup(struct s3dirent *de, char *name, struct s3dirent *result)
{
    int namelen = strlen(name);
    
    if (!S_ISDIR(de->mode))
        return -ENOTDIR;

    int dirlen = de->bytes;
    void *start = find_dir(de->off, de->bytes);

    for (struct s3dirent *_de = start; (void*)_de - start < dirlen; ) {
        if (_de->namelen == namelen && !memcmp(_de->name, name, namelen)) {
            *result = *_de;
            return 0;
        }
        _de = next_de(_de);
    }
    return -ENOENT;
}

int parse(char *path, char **argv)
{
    int i;
    for (i = 0; i < MAX_PATH_LEN; i++) {
        if ((argv[i] = strtok(path, "/")) == NULL)
            break;
        path = NULL;
    }
    return i;
}

/* convert path into dirent, return 0 on error
 */
int translate(char *path, struct s3dirent *result)
{
    struct s3dirent de = *root_de;
    char *names[MAX_PATH_LEN];
    int i, val, pathlen = parse(path, names);
    
    for (i = val = 0; i < pathlen && val >= 0; i++)
	val = lookup(&de, names[i], &de);

    if (val < 0)
        return val;

    /* TODO: hard links. (only on leaf - no hard links to directories)
     *     if (de.mode & S_IFMT) == S_IFHRD then <dereference>
     */

    *result = de;
    return 0;
}

#define FS_BLOCK_SIZE 512

void dirent2stat(struct stat *sb, struct s3dirent *de)
{
    memset(sb, 0, sizeof(*sb));
    sb->st_mode = de->mode;
    sb->st_nlink = 1;
    sb->st_uid = de->uid;
    sb->st_gid = de->gid;
    if (S_ISCHR(de->mode) || S_ISBLK(de->mode)) {
        sb->st_rdev = de->bytes;
        sb->st_size = 0;
    }
    else 
        sb->st_size = de->bytes;
    sb->st_blocks = (de->bytes + FS_BLOCK_SIZE - 1) / FS_BLOCK_SIZE;
    sb->st_atime = sb->st_mtime = sb->st_ctime = de->ctime;
#ifdef S3_USE_INUM
    sb->st_ino = de->ino;
#endif
}

/* getattr - get file or directory attributes. For a description of
 *  the fields in 'struct stat', see 'man lstat'.
 *
 * Note - for several fields in 'struct stat' there is no corresponding
 *  information in our file system:
 *    st_nlink - always set it to 1
 *    st_atime, st_ctime - set to same value as st_mtime
 *
 * errors - path translation, ENOENT
 */
int fs_getattr(const char *path, struct stat *sb)
{
    char *_path = strdup(path);
    struct s3dirent de;
    int val = translate(_path, &de);
    free(_path);

    if (val < 0)
        return val;
    dirent2stat(sb, &de);

    return 0;
}

/* use 'long' for stashing pointer in file handle, since it's the
 * same size as a pointer in 32 and 64 bit
 */

#define USE_FH
#ifdef USE_FH
int fs_opendir(const char *path, struct fuse_file_info *fi)
{
    struct s3dirent de;
    char *_path = strdup(path);
    int val = translate(_path, &de);
    free(_path);
    if (val < 0)
	return val;

    struct s3dirent *tmp = malloc(sizeof(*tmp));
    *tmp = de;
    fi->fh = (long)tmp;
    return 0;
}

int fs_releasedir(const char *path, struct fuse_file_info *fi)
{
    struct s3dirent *tmp = (void*)(long)fi->fh;
    free(tmp);
    return 0;
}
#endif

/* readdir - get directory contents.
 *
 * call the 'filler' function once for each valid entry in the 
 * directory, as follows:
 *     filler(buf, <name>, <statbuf>, 0)
 * where <statbuf> is a pointer to struct stat, just like in getattr.
 *
 * Errors - path resolution, ENOTDIR, ENOENT
 */
int fs_readdir(const char *path, void *ptr, fuse_fill_dir_t filler,
		       off_t offset, struct fuse_file_info *fi)
{
#ifdef USE_FH
    struct s3dirent *tmp = (struct s3dirent*)(long)fi->fh;
    struct s3dirent de = *tmp;
    int val, i, j;
#else
    char *_path = strdup(path);
    struct s3dirent de;
    int val = translate(_path, &de);
    free(_path);
    if (val < 0)
	return val;
    int i, j;
#endif
    
    if (!S_ISDIR(de.mode))
        return -ENOTDIR;

    int dirlen = de.bytes;
    void *start = find_dir(de.off, de.bytes);

    for (struct s3dirent *_de = start; (void*)_de - start < dirlen; ) {
        struct stat sb;
        dirent2stat(&sb, _de);
        char name[256];
        memcpy(name, _de->name, _de->namelen);
        name[_de->namelen] = 0;
        filler(ptr, name, &sb, 0);
        _de = (void*)(_de+1) + _de->namelen;
    }
    
    return 0;
}

#ifdef USE_FH
int fs_open(const char *path, struct fuse_file_info *fi)
{
    char *_path = strdup(path);
    struct s3dirent de;
    int val = translate(_path, &de);
    free(_path);
    if (val < 0)
	return val;

    struct s3dirent *tmp = malloc(sizeof(*tmp));
    *tmp = de;
    fi->fh = (long)tmp;
    return 0;
}

int fs_release(const char *path, struct fuse_file_info *fi)
{
    struct s3dirent *tmp = (struct s3dirent*)(long)fi->fh;
    free(tmp);
    return 0;
}
#endif

/* read - read data from an open file.
 * should return exactly the number of bytes requested, except:
 *   - if offset >= file len, return 0
 *   - if offset+len > file len, return bytes from offset to EOF
 *   - on error, return <0
 * Errors - path resolution, ENOENT, EISDIR
 */
int fs_read(const char *path, char *buf, size_t len, off_t offset,
	    struct fuse_file_info *fi)
{
    struct state *s = &_state;
#ifdef USE_FH
    struct s3dirent *tmp = (struct s3dirent*)(long)fi->fh;
    struct s3dirent de = *tmp;
#else
    char *_path = strdup(path);
    struct s3dirent de;
    int val = translate(_path, &de);
    free(_path);
    if (val < 0)
	return val;
#endif
    
    if (!S_ISREG(de.mode))
        return -EISDIR;

    if (offset >= de.bytes)
        return 0;

    if (offset + len > de.bytes)
        len = de.bytes - offset;
    
    long int base = de.off.s.sector*512;
    //_filename = path;
    do_read(s, s->names[de.off.s.object], de.off.s.object, buf, len, base+offset);

    return len;
}

int fs_readlink(const char *path, char *buf, size_t len)
{
    struct state *s = &_state;
    char *_path = strdup(path);
    struct s3dirent de;
    int val = translate(_path, &de);
    free(_path);
    if (val < 0)
	return val;

    if (!S_ISLNK(de.mode))
        return -EINVAL;
    int n = (len-1 < de.bytes) ? len-1 : de.bytes;

    do_read(s, s->names[de.off.s.object], de.off.s.object, buf, n, de.off.s.sector*512);
    buf[n] = 0;
    return 0;
}

/* statfs - get file system statistics
 * Errors - none. Needs to work.
 */
int fs_statfs(const char *path, struct statvfs *st)
{
    void *s = _root;
    struct s3statfs *st3 = (struct s3statfs*)(s + 512 - sizeof(*st3));

    memset(st, 0, sizeof(*st));
    st->f_bsize = st->f_frsize = 512;
    st->f_blocks = st3->total_sectors;
    st->f_blocks = 0;
    for (int i = 0; i < _state.nvers; i++)
        st->f_blocks += _state.nsectors[i];
    st->f_bfree = st->f_bavail = 0;
    st->f_files = st3->files + st3->dirs + st3->symlinks;
    st->f_ffree = st->f_favail = 0;
    st->f_namemax = 255;

    return 0;
}

/* operations vector
 * TODO: open, release, opendir, getxattr, listxattr
 */
struct fuse_operations fs_ops = {
    .init = fs_init,
    .getattr = fs_getattr,
    .readdir = fs_readdir,
    .read = fs_read,
#ifdef USE_FH
    .opendir = fs_opendir,
    .releasedir = fs_releasedir,
    .open = fs_open,
    .release = fs_release,
#endif
    .statfs = fs_statfs,
    .readlink = fs_readlink,
};

/* it would be nice to be able to update it to add a new incremental
 * backup without unmounting / remounting...
 * maybe use a unix-domain socket for commands?
 */


int main(int argc, char **argv)
{
    /* Argument processing and checking
     */
    _state.host = getenv("S3_HOSTNAME");
    _state.access_key = getenv("S3_ACCESS_KEY_ID");
    _state.secret_key = getenv("S3_SECRET_ACCESS_KEY");

    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
    if (fuse_opt_parse(&args, &_state, opts, myfs_opt_proc) == -1)
        exit(1);

    /* various options to (hopefully) get FUSE performance and
     * standard filesystem behavior.
     */
    fuse_opt_insert_arg(&args, 1, "-oallow_other");
    fuse_opt_insert_arg(&args, 1, "-odefault_permissions");
    fuse_opt_insert_arg(&args, 1, "-okernel_cache");
#ifdef S3_USE_INUM
    fuse_opt_insert_arg(&args, 1, "-ouse_ino");
#endif
    fuse_opt_insert_arg(&args, 1, "-oro,entry_timeout=1000,attr_timeout=1000");

    if (_state.local) {
        int fd = open(_state.target, O_RDONLY);
        if (fd < 0) {
            perror("image file open");
            exit(1);
        }
        fd_cache[0].name = _state.target;
        fd_cache[0].fd = fd;
    }
    else
        s3_init(&_state);

    /* TODO: add options:
     * allow_other, default_permissions, kernel_cache, ro
     */
    return fuse_main(args.argc, args.argv, &fs_ops, "this is a test");
}
