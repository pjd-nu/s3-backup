/*
 * file: s3backup.c
 * Peter Desnoyers, 2020 
 */

#define _GNU_SOURCE             /* O_PATH etc. */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/stat.h>
#include <assert.h>
#include <stdint.h>
#include <argp.h>
#include <sys/time.h>
#include <libs3.h>
#include <limits.h>
#include <uuid/uuid.h>
#include <avl.h>

#include "s3fs.h"

char zeros[512];

int   fds[64];
char *filenames[64];
int   me;

/* --------- argument parsing ---------- */

static char args_doc[] = "OBJECT DIR";
// TODO: exclude by pattern
// TODO: read config file

static struct argp_option options[] = {
    {"bucket",      'b', "NAME",   0, "bucket for all objects"},
    {"incremental", 'i', "NAME", 0, "incremental backup"},
    {"protocol",    'p', "http/https", 0, "S3 protocol"},
    {"tag",         't', "NAME",      0, "tag for root entry"},
    {"local",       'l', 0,        0, "in/out to files (not S3)"},
    {"verbose",     'v', 0,        0, "verbose messages"},
    {"max",         'm', "SIZE",   0, "stop writing after SIZE (K/M/G)"},
    {"hostname",    'h', "HOST",   0, "S3 hostname"},
    {"access-key",  'a', "KEY",    0, "S3 access key"},
    {"secret-key",  's', "KEY",    0, "S3 secret key"},
    {"noio",        'n', 0,        0, "no output (test only)"},
    {"exclude",     'e', "FILE",   0, "exclude file"},
    { 0 }
};

struct state {
    /* ---- */
    int    n;
    char **names;
    char  *__me;
    int    me;
    int    verbose;
    int    noio;
    
    /* -- LibS3 stuff -- */
    S3BucketContext bkt_ctx;
    S3PutProperties put_prop;

    int      retries;           /* retry/backoff on S3 failure */
    int      sleep;

    S3Status status;            /* from last operation */
    char    *msg;               /* on error */
    off_t    content_length;    /* for HEAD */
    
    char    *upload_id;         /* for multipart upload */
    int      part;              /* currentmultipart part # */

    char    *buf;               /* data buffer */
    int      bufsiz;            /* size of buffer */
    int      len;               /* amount of data in it */
    int      offset;            /* data already transmitted */
    off_t    total;             /* for progress messages */
    double   start;             /* for timing */
    
    char    *xml;               /* for CompleteMultipartUpload */
    char    *etags[10000];
    int      xml_len;
    int      xml_offset;

    void    *recv_buf;
    int      recv_wanted;
    int      recv_got;
    
    /* -- argp stuff -- */
    char       *bucket;
    char       *new_name;
    FILE       *out_fp;
    char       *old_name;
    int         in_fd;
    char       *dir;
    int         local;
    char       *exclude[16];
    
    char       *hostname;
    int         protocol;
    char       *access_key;
    char       *secret_key;
    char       *tag;

    int         incremental;

    off_t       stopafter;      /* in sectors */
    
    /* here's where we accumulate directory information
     */
    struct s3dirloc *dir_locs;
    int              n_dirs;
    int              max_dirs;
    int              tmpdir_fd;
};

off_t parseint(char *s)
{
    off_t val = strtol(s, &s, 0);
    if (toupper(*s) == 'G')
        val *= (1024*1024*1024);
    if (toupper(*s) == 'M')
        val *= (1024*1024);
    if (toupper(*s) == 'K')
        val *= 1024;
    return val;
}
    
static error_t parse_opt(int key, char *arg, struct argp_state *state)
{
    struct state *s = state->input;
    switch (key) {
    case ARGP_KEY_INIT:
        memset(s, 0, sizeof(*s));
        s->tag = "--root--";
        s->protocol = S3ProtocolHTTPS;
        s->access_key = getenv("S3_ACCESS_KEY_ID");
        s->secret_key = getenv("S3_SECRET_ACCESS_KEY");
        s->hostname = getenv("S3_HOSTNAME");
        s->names = &s->__me;
        s->stopafter = 1ULL << 50;
        break;
    case 'm':
        s->stopafter = parseint(arg) / 512;
        break;
    case 'n':
        s->noio = 1;
        break;
    case 'v':
        s->verbose = 1;
        break;
    case 'b':
        s->bucket = arg;
        break;
    case 'i':
        s->old_name = arg;
        s->incremental = 1;
        break;
    case 'h':
        s->hostname = arg;
        break;
    case 'p':
        if (!strcmp(arg, "http"))
            s->protocol = S3ProtocolHTTP;
        else if (!strcmp(arg, "https"))
            s->protocol = S3ProtocolHTTPS;
        else {
            fprintf(stderr, "Illegal S3 protocol: %s\n", arg);
            argp_usage(state);
        }
        break;
    case 'a':
        s->access_key = arg;
        break;
    case 's':
        s->secret_key = arg;
        break;
    case 't':
        s->tag = arg;
        break;
    case 'l':
        s->local = 1;
        break;
    case 'e':
        for (int i = 0; i < 16; i++) {
            if (s->exclude[i] == NULL) {
                s->exclude[i] = arg;
                break;
            }
        }
        break;
    case ARGP_KEY_ARG:
        if (state->arg_num == 0)
            s->new_name = arg;
        else if (state->arg_num == 1)
            s->dir = arg;
        else
            argp_usage(state);
        break;
    case ARGP_KEY_END:
        if (s->local) {
            if ((s->out_fp = fopen(s->new_name, "wb")) == NULL) {
                fprintf(stderr, "Failed to open %s: %s\n", s->new_name, strerror(errno));
                exit(1);
            }
            if ((s->in_fd = open(s->old_name, O_RDONLY)) < 0) {
                fprintf(stderr, "Failed to open %s: %s\n", s->old_name, strerror(errno));
                exit(1);
            }
        }
        break;
    }
    return 0;
}

static struct argp argp = { options, parse_opt, NULL, args_doc};

/* ---------------- caching directories ---------------------------*/

struct dir {
    union s3offset off;
    size_t         nbytes;
    void          *data;
};

avl_tree_t *dir_cache;

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
    assert(node == NULL);
    node = avl_insert(dir_cache, d);
}

struct dir *get_dir(union s3offset off, size_t nbytes)
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

/* ---------------- now we have the S3 read/write stuff ---------- */

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
    struct state *s = data;
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
    struct state *s = data;
    s->content_length = p->contentLength;
    return S3StatusOK;
}

double gettime(void)
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec/1.0e6;
}

S3Status response_properties_etag(const S3ResponseProperties *p, void *data)
{
    struct state *s = data;
    double now = gettime();
    double gb = s->total / (1024.0*1024*1024);
    double mb_s = s->total / (1e6 * (now - s->start));
    s->etags[s->part] = strdup(p->eTag); /* note - already quoted */
    printf("%llu bytes uploaded (%.3f GiB, %.3f MB/s)\n",
           (unsigned long long)s->total, gb, mb_s);
    return S3StatusOK;
}

int part_data_callback(int size, char *buf, void *data)
{
    struct state *s = data;
    int len = s->len - s->offset;
    if (size < len)
        len = size;
    memcpy(buf, s->buf + s->offset, len);
    s->offset += len;

    s->total += len;
    return len;
}

void upload_part(struct state *s)
{
    S3PutObjectHandler h = {
        .responseHandler.propertiesCallback = response_properties_etag,
        .responseHandler.completeCallback = response_complete,
        .putObjectDataCallback = part_data_callback
    };

    s->part++;
    do {
        S3_upload_part(&s->bkt_ctx,
                       s->new_name,
                       &s->put_prop,
                       &h,
                       s->part,
                       s->upload_id,
                       s->len,
                       NULL,       /* requestContext */
                       0,          /* timeout (ms) */
                       s);
    } while (S3_status_is_retryable(s->status) && should_retry(s));

    if (s->status != S3StatusOK) {
        print_error(s);
        exit(1);
    }

    if (s->bufsiz < 500 * 1024 * 1024) {
        s->bufsiz = s->bufsiz * 2;
        s->buf = realloc(s->buf, s->bufsiz);
    }
    s->len = s->offset = 0;
}

S3Status multipart_init_response(const char *upload_id, void *data)
{
    struct state *s = data;
    s->upload_id = strdup(upload_id);
    return S3StatusOK;
}

void put_init(struct state *s)
{
    char *key = s->new_name;

    s->bufsiz = 5 * 1024 * 1024;
    s->buf = malloc(s->bufsiz);
    s->part = 0;

    S3MultipartInitialHandler init_handler = {
        .responseHandler.propertiesCallback = response_properties,
        .responseHandler.completeCallback = response_complete,
        .responseXmlCallback = multipart_init_response};

    do {
        S3_initiate_multipart(&s->bkt_ctx,
                              key,
                              NULL,            /* putProperties */
                              &init_handler,
                              0,            /* requestContext */
                              0,            /* timeout (ms) */
                              s);
    } while (S3_status_is_retryable(s->status) && should_retry(s));

    if (s->upload_id == 0 || s->status != S3StatusOK) {
        print_error(s);
        exit(1);
    }
}

void put_write(struct state *s, char *buf, int len)
{
    static int init_done;
    if (!init_done) {
        put_init(s);
        init_done = 1;
    }
    
    if (s->len + len >= s->bufsiz) {
        int bytes = s->bufsiz - s->len;
        memcpy(s->buf + s->len, buf, bytes);
        s->len += bytes;
        upload_part(s);
        buf += bytes;
        len -= bytes;
    }
    memcpy(s->buf + s->len, buf, len);
    s->len += len;
}

S3Status recv_data_callback(int size, const char *buf, void *data)
{
    struct state *s = data;

    /* don't overrun the buffer - should never happen 
     */
    if (size + s->recv_got > s->recv_wanted)
        return S3StatusAbortedByCallback;

    memcpy(s->recv_buf + s->recv_got, buf, size);
    s->recv_got += size;

    return S3StatusOK;
}

int s3_get_range(struct state *s, char *key,
                 char *buf, uint64_t len, uint64_t offset)
{
    S3GetObjectHandler h = {
        .responseHandler.propertiesCallback = response_properties,
        .responseHandler.completeCallback = response_complete,
        .getObjectDataCallback = recv_data_callback
    };

    s->recv_buf = buf;
    s->recv_wanted = len;
    s->recv_got = 0;
    
    do {
        S3_get_object(&s->bkt_ctx,
                      key,
                      NULL,     /* no conditions */
                      offset,
                      len,
                      0,        /* requestContext */
                      0,        /* timeoutMs */
                      &h,
                      s);
    } while (S3_status_is_retryable(s->status) && should_retry(s));

    if (s->status != S3StatusOK) {
        print_error(s);
        exit(1);
    }
    
    return len;
}

int s3_init(struct state *s)
{
    S3Status status = S3_initialize("Multi-Version Backup V0.1",
                                    S3_INIT_ALL, s->hostname);
    if (status != S3StatusOK) {
        fprintf(stderr, "Failed to initialize libs3: %s\n",
                S3_get_status_name(status));
        exit(-1);
    }

    s->retries = 5;
    s->sleep = 1;
    s->bkt_ctx = (S3BucketContext){ s->hostname,
                                    s->bucket,
                                    s->protocol,
                                    S3UriStylePath,
                                    s->access_key,
                                    s->secret_key,
                                    0,   /* security token */
                                    0 }; /* authRegion */

    
    s->put_prop = (S3PutProperties) { NULL, /* binary/octet-stream */
                                      NULL, /* MD5 */
                                      NULL, /* cache control */
                                      NULL, /* content disposition */
                                      NULL, /* content encoding */
                                      -1,   /* expires (never) */
                                      S3CannedAclPrivate,
                                      0,    /* metaproperties count */
                                      NULL, /* metaproperty list */
                                      0};   /* use server encryption */
}

static int xml_data_callback(int size, char *buf, void *data)
{
    struct state *s = data;
    int remaining = s->xml_len - s->xml_offset;
    int bytes = (size < remaining) ? size : remaining;

    memcpy(buf, s->xml + s->xml_offset, bytes);
    s->xml_offset += bytes;

    return bytes;
}

int s3_finalize(struct state *s)
{
    if (s->len)
        upload_part(s);         /* write the last part */

    S3MultipartCommitHandler h = {
        .responseHandler.propertiesCallback = response_properties,
        .responseHandler.completeCallback = response_complete,
        .putObjectDataCallback = xml_data_callback,
        .responseXmlCallback = NULL
    };
    
    /* entry for each part looks like this:
     * "<Part><ETag>E</ETag><PartNumber>#</PartNumber></Part>\n"
     * or 52 characters + etag + part#
     * plus another 64 or so for <<CompleteMultipartUpload> ...
     */
    int etag_total = 0;
    for (int i = 0; i <= s->part; i++)
        if (s->etags[i])
            etag_total += strlen(s->etags[i]);
    int total = 150 + etag_total + (4 + 52) * s->part;
    char *msg = malloc(total), *ptr = msg;

    /* we seem to need all of this to get Minio to work...
     */
    ptr += sprintf(ptr, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                   "<CompleteMultipartUpload "
                   "xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
    for (int i = 0; i <= s->part; i++)
        if (s->etags[i])
            ptr += sprintf(ptr, "<Part><ETag>%s</ETag>"
                           "<PartNumber>%d</PartNumber></Part>\n",
                           s->etags[i], i);
    ptr += sprintf(ptr, "</CompleteMultipartUpload>");
    s->xml = msg;
    s->xml_len = strlen(msg);
    s->xml_offset = 0;

    do {
        S3_complete_multipart_upload(&s->bkt_ctx,
                                     s->new_name,
                                     &h,
                                     s->upload_id,
                                     s->xml_len, 
                                     NULL,        /* requestContext */
                                     0,           /* timeout (ms) */
                                     s);
    } while (S3_status_is_retryable(s->status) && should_retry(s));
    
    if (s->status != S3StatusOK) {
        print_error(s);
        exit(1);
    }
}

/* ------------ read local files (debug) -------------- */

/* Hack. Will crash w/ over 128 filenames
 */
struct name_fd {
    char *name;
    int   fd;
} fd_cache[128];

int get_fd(struct state *s, char *file)
{
    int i;

    if (fd_cache[0].name == NULL) { /* horrible hack */
        fd_cache[0].name = s->old_name;
        fd_cache[0].fd = s->in_fd;
    }
        
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
    
/* ------- functions for file system code to use ------- */
/* ------- write to / read from S3 or file as necessary ------ */
/* ------- exit on all errors --------*/

off_t total_written;

void do_write(struct state *s, void *ptr, off_t len)
{
    assert(len >= 0);
    total_written += len;
    if (len == 0 || s->noio)
        return;

    if (s->local) {
        if (len > 0 && fwrite(ptr, len, 1, s->out_fp) < 1) {
            fprintf(stderr, "Write %s: %s\n", s->new_name, strerror(errno));
            exit(1);
        }
    }
    else 
        put_write(s, ptr, len);
}


void do_read(struct state *s, char *key, void *ptr, off_t len, off_t offset)
{
    if (s->local) {
        int fd = get_fd(s, key);
        if (pread(fd, ptr, len, offset) < 0) {
            fprintf(stderr, "Error reading %s : %s\n", key, strerror(errno));
            exit(1);
        }
    }
    else {
        s3_get_range(s, key, ptr, len, offset);
        return;
    }        
}

off_t do_size(struct state *s, char *key)
{
    if (s->local) {
        struct stat sb;
        if (stat(key, &sb) < 0) {
            fprintf(stderr, "can't access %s : %s\n", key, strerror(errno));
            exit(1);
        }
        return sb.st_size;
    }
    else {
        s->content_length = 0;
        S3ResponseHandler h = {.propertiesCallback = response_properties,
                               .completeCallback = response_complete};
        do {
            S3_head_object(&s->bkt_ctx,
                           key,
                           NULL,    /* RequestContext */
                           0,       /* timeout */
                           &h,
                           s);
        } while (S3_status_is_retryable(s->status) && should_retry(s));

        return s->content_length;
    }
}

void do_done(struct state *s)
{
    if (!s->local)
        s3_finalize(s);
}

/* -------------- next we have the filesystem code itself ------------- */

off_t round_up(off_t a, off_t b)
{
    return b * ((a + b - 1) / b);
}

/* kind of a kludge, but it gives us reasonable error messages.
 */
char _path[PATH_MAX];
char *path;                     /* this is really a kludge */

/* another kludge - accumulate file system statistics here
 */
struct s3statfs stats;


/*
 * offset - current offset (in sectors) in output
 * fd - descriptor of link (opened with O_PATH | O_NOFOLLOW)
 * _d3e - place to put dirent for this object
 * _d3e_p - returns pointer to next dirent after we store this one
 * name - name of this entry
 * sb - stat results for fd
 */
int store_link(struct state *s, off_t offset, int fd, struct s3dirent *_d3e,
               struct s3dirent **_d3e_p, char *name, struct stat *sb)
{
    char buf[PATH_MAX];
    int nbytes;
    off_t _off0 = offset;
    
    /* man 2 readlinkat:
     * "Since Linux 2.6.39, pathname can be an empty string, in which case 
     * the call operates on the symbolic link referred to by dirfd (which 
     * should have been obtained using open(2) with the O_PATH and 
     * O_NOFOLLOW flags)."
     */
    memset(buf, 0, sizeof(buf));
    if ((nbytes = readlinkat(fd, "", buf, sizeof(buf))) < 0) {
        fprintf(stderr, "readlink error %s/%s : %s\n", path, name, strerror(errno));
        exit(1);
    }
    int n = round_up(nbytes, 512);
    do_write(s, buf, n);

    *_d3e = (struct s3dirent){.mode = sb->st_mode, .uid = sb->st_uid,
                              .gid = sb->st_gid,
                              .ctime = sb->st_ctime,
#ifdef S3_USE_INUM
                              .ino = sb->st_ino,
#endif
                              .off = {.s.sector = offset, .s.object = s->me},
                              .bytes = nbytes};
    strcpy(_d3e->name, name);
    _d3e->namelen = strlen(name);
    if (_d3e_p)
        *_d3e_p = next_de(_d3e);

    stats.symlinks++;
    stats.sym_sectors += n/512;

    assert((total_written & 511) == 0);
    offset += n/512;
    assert(offset*512 == total_written);

    if (s->verbose)
        printf("L %lld %lld %s/%s\n", (long long)(offset-_off0), (long long)sb->st_size,
               path, name);
    return offset;
}

/*
 * offset - current offset (in sectors) in output
 * fd - descriptor of current object (file / directory / ...)
 * _d3e - place to put dirent for this object
 * _d3e_p - returns pointer to next dirent after we store this one
 * name - name of this entry
 * sb - stat results for fd
 */
int store_file(struct state *s, off_t offset, int fd, struct s3dirent *_d3e,
               struct s3dirent **_d3e_p, char *name, struct stat *sb)
{
    off_t nbytes = 0, _off0 = offset;
    int len;
    char buf[16*1024];

    if (s->noio)
        nbytes = sb->st_size;
    else while ((len = read(fd, buf, sizeof(buf))) > 0) {
        do_write(s, buf, len);
        nbytes += len;
    }
    off_t total = round_up(nbytes, 512);
    do_write(s, zeros, total-nbytes);

    assert((total_written & 511) == 0);
    
    *_d3e = (struct s3dirent){.mode = sb->st_mode, .uid = sb->st_uid,
                              .gid = sb->st_gid,
                              .ctime = sb->st_ctime,
#ifdef S3_USE_INUM
                              .ino = sb->st_ino,
#endif
                              .off = {.s.sector = offset, .s.object = s->me},
                              .bytes = nbytes};
    strcpy(_d3e->name, name);
    _d3e->namelen = strlen(name);
    if (_d3e_p)
        *_d3e_p = next_de(_d3e);

    stats.files++;
    stats.file_sectors += total/512;

    offset += (total / 512);
    assert(offset*512 == total_written);
    if (s->verbose)
        printf("F %lld %lld %s/%s\n", (long long)(offset-_off0), (long long)sb->st_size,
               path, name);

    return offset;
}

/* Write data to a file. Used for storing metadata after directory 
 * traversal is finished.
 * TODO: can get rid of this if we stash dir locations in a temp file
 */
int store_data(struct state *s, off_t offset, struct s3dirent *_d3e,
               struct s3dirent **_d3e_p, char *name, void *data, size_t nbytes)
{
    off_t _off0 = offset;

    if (!s->noio)
        do_write(s, data, nbytes);

    off_t total = round_up(nbytes, 512);
    if (!s->noio)
        do_write(s, zeros, total-nbytes);

    assert((total_written & 511) == 0);
    
    *_d3e = (struct s3dirent){.mode = 0, .uid = 0, .gid = 0, .ctime = 0,
#ifdef S3_USE_INUM
                              .ino = 0,
#endif
                              .off = {.s.sector = offset, .s.object = s->me},
                              .bytes = nbytes};
    strcpy(_d3e->name, name);
    _d3e->namelen = strlen(name);
    if (_d3e_p)
        *_d3e_p = next_de(_d3e);

    stats.files++;
    stats.file_sectors += total/512;

    offset += (total / 512);
    assert(offset*512 == total_written);
    if (s->verbose)
        printf("d %lld %lld %s/%s\n", (long long)(offset-_off0),
               (long long)nbytes, path, name);

    return offset;
}

struct s3dirent *lookup(void *start, int dirlen, char *name)
{
    if (start == NULL || dirlen == 0)
        return NULL;

    int len = strlen(name);
    for (struct s3dirent *_de = start; (void*)_de - start < dirlen; ) {
        if (_de->namelen == len && !memcmp(_de->name, name, len))
            return _de;
        _de = next_de(_de);
    }
    return NULL;
}

int unchanged(struct s3dirent *old, struct stat *sb)
{
    return (old != NULL) &&
        old->mode == sb->st_mode &&
        old->bytes == sb->st_size &&
        old->ctime == sb->st_ctime &&
#ifdef S3_USE_INUM
        old->ino == sb->st_ino &&
#endif
        old->uid == sb->st_uid &&
        old->gid == sb->st_gid;
}

/* very much a kludge - stash directory locations. 
 * TODO: much cleaner to use a temp file like dir data
 */
void dirstash_init(struct state *s)
{
    s->max_dirs = 1000;
    s->dir_locs = malloc(s->max_dirs * sizeof(*(s->dir_locs)));
    s->n_dirs = 0;
}
void dirstash_loc(struct state *s, union s3offset off, size_t len)
{
    if (s->n_dirs >= s->max_dirs) {
        s->max_dirs *= 2;
        s->dir_locs = realloc(s->dir_locs, s->max_dirs * sizeof(*(s->dir_locs)));
    }
    s->dir_locs[s->n_dirs++] = (struct s3dirloc){.off = off, .bytes = len};
}

/* device node or empty directory. no contents
 */
int store_node(struct state *s, struct s3dirent *_d3e, struct s3dirent **_d3e_p,
               char *name, struct stat *sb, struct s3dirent *old_de)
{
    *_d3e = (struct s3dirent){.mode = sb->st_mode, .uid = sb->st_uid,
                              .gid = sb->st_gid,
                              .ctime = sb->st_ctime,
#ifdef S3_USE_INUM
                              .ino = sb->st_ino,
#endif
                              .off = {.n = 0},
                              .bytes = 0};

    /* if it's a device node, abuse de->bytes to hold device number
     */
    if (S_ISCHR(sb->st_mode) || S_ISBLK(sb->st_mode))
        _d3e->bytes = sb->st_rdev;

    strcpy(_d3e->name, name);
    _d3e->namelen = strlen(name);
    if (_d3e_p)
        *_d3e_p = next_de(_d3e);
}

int exclude(struct state *s, char *dir, char *file)
{
    char path[strlen(dir)+strlen(file)+10];
    sprintf(path, "%s/%s", dir, file);
    for (int i = 0; i < 16 && s->exclude[i]; i++)
        if (!strcmp(s->exclude[i], path))
            return 1;
    return 0;
}

/*
 * offset - current offset (in sectors) in output
 * fd - descriptor of current object (file / directory / ...)
 * _d3e - place to put dirent for this object
 * _d3e_p - returns pointer to next dirent after we store this one
 * sb - stat results for fd
 * name - name of this entry
 * old_de - dirent for this directory in old version (or NULL)
 */
int store_dir(struct state *s, off_t offset, int fd, struct s3dirent *_d3e,
              struct s3dirent **_d3e_p, char *name,
              struct stat *sb, struct s3dirent *old_de)
{
    int max = 128*1024;
    void *orig_d3e = malloc(max);
    struct s3dirent *d3e = orig_d3e;

    char *p = _path + strlen(_path);
    sprintf(p, "/%s", name);    /* extend path */
    
    void *start = NULL;
    int   len = 0;
    if (old_de) {
        if (old_de->off.s.object >= s->me) {        /* #ancestors */
            fprintf(stderr, "%s: corrupt ancester index %d\n",
                    _path, old_de->off.s.object);
            exit(1);
        }
        start = get_dir(old_de->off, old_de->bytes);
        len = old_de->bytes;
    }

    /* to do - keep track if we make it through the directory with everything 
     * unchanged, and if so store the old entry and return.
     * probably want to push that function down for store_file and store_link
     */
    DIR *d = fdopendir(fd);
    struct dirent *de;
    for (de = readdir(d); de != NULL; de = readdir(d)) {
        if (strcmp(de->d_name, ".") == 0 ||
            strcmp(de->d_name, "..") == 0)
            continue;

        if (exclude(s, path, de->d_name)) {
            fprintf(stderr, "excluding %s/%s\n", path, de->d_name);
            continue;
        }
        
        /* really big backups can be split into multiple increments
         */
        if (offset >= s->stopafter)
            break;
        
        /* handle really large directories
         */
        int de3_len = sizeof(*d3e) + strlen(de->d_name);
        int de3_offset = (void*)d3e - orig_d3e;
        if (de3_len + de3_offset >= max) {
            max = max * 2;
            orig_d3e = realloc(orig_d3e, max);
            d3e = orig_d3e + de3_offset;
        }
        
        struct s3dirent *old_p = lookup(start, len, de->d_name);

        /* this is the stat buffer for the directory entry
         */
        struct stat sb2;

        /* special case to avoid blocking on named pipes
         */
        if (fstatat(fd, de->d_name, &sb2, AT_SYMLINK_NOFOLLOW) == 0)
            if (S_ISFIFO(sb2.st_mode)) {
                fprintf(stderr, "skipping %s/%s: %s\n",
                        path, de->d_name, strerror(errno));
                goto skip;
            }

        /* see explanation of O_NOFOLLOW and O_PATH in 'man 2 open'
         */
        int fd2 = openat(fd, de->d_name, O_RDONLY | O_NOFOLLOW);
        if (fd2 < 0 && errno == ELOOP)
            fd2 = openat(fd, de->d_name, O_RDONLY | O_PATH | O_NOFOLLOW);
        if (fd2 < 0) {
            fprintf(stderr, "skipping %s/%s: %s\n",
                    path, de->d_name, strerror(errno));
            goto skip;
        }

        if (fstat(fd2, &sb2) < 0) {
            fprintf(stderr, "skipping %s/%s: %s\n",
                    path, de->d_name, strerror(errno));
            goto skip;
        }

        if (S_ISREG(sb2.st_mode)) {
            if (unchanged(old_p, &sb2)) {
                *d3e = *old_p;
                memcpy(d3e->name, old_p->name, old_p->namelen);
                d3e = next_de(d3e);
            }
            else
                offset = store_file(s, offset, fd2, d3e, &d3e, de->d_name, &sb2);
        }
        else if (S_ISDIR(sb2.st_mode)) {
            /* don't cross mountpoints
             */
            if (sb->st_dev != sb2.st_dev)
                store_node(s, d3e, &d3e, de->d_name, &sb2, old_p);
            else
                offset = store_dir(s, offset, fd2, d3e, &d3e, de->d_name, &sb2, old_p);
        }
        else if (S_ISLNK(sb2.st_mode)) {
            offset = store_link(s, offset, fd2, d3e, &d3e, de->d_name, &sb2);
        }
        else if (S_ISCHR(sb2.st_mode) || S_ISBLK(sb2.st_mode)) {
            store_node(s, d3e, &d3e, de->d_name, &sb2, old_p);
        }
    skip:
        close(fd2);
    }
    
    *p = 0;                     /* truncate path - we're done */
    
    int nbytes = (void*)d3e - orig_d3e;
    int total = round_up(nbytes, 512);
    off_t _off0 = offset;
    
    memset(d3e, 0, total-nbytes);

/* hmm, we're using results from stat possibly a while in the past...*/

    union s3offset loc = {.s.sector = offset, .s.object = s->me};
    *_d3e = (struct s3dirent){.mode = sb->st_mode, .uid = sb->st_uid,
                              .gid = sb->st_gid,
                              .ctime = sb->st_ctime,
#ifdef S3_USE_INUM
                              .ino = sb->st_ino,
#endif
                              .off = loc,
                              .bytes = nbytes};
    strcpy(_d3e->name, name);
    _d3e->namelen = strlen(name);
    if (_d3e_p)
        *_d3e_p = next_de(_d3e);

    /* save the directory location and data
     * note that we don't write the padding; when we read it back in, directories
     * are densely packed, using less memory (like 40%) when we cache them.
     */
    if (s->n_dirs >= s->max_dirs) {
        s->max_dirs *= 2;
        s->dir_locs = realloc(s->dir_locs, s->max_dirs * sizeof(*(s->dir_locs)));
    }
    s->dir_locs[s->n_dirs++] = (struct s3dirloc){.off = loc, .bytes = nbytes};
    if (write(s->tmpdir_fd, orig_d3e, nbytes) < 0) {
        fprintf(stderr, "tmp write failed: %s\n", strerror(errno));
        exit(1);
    }
    
    /* now write out the directory to the output
     */
    do_write(s, orig_d3e, total);
    offset += (total / 512);

    assert((total_written & 511) == 0);
    assert(offset*512 == total_written);
    
    stats.dirs++;
    stats.dir_sectors += total/512;
    stats.dir_bytes += nbytes;

    free(orig_d3e);
    closedir(d);
    
    if (s->verbose)
        printf("D %lld %lld %s/%s\n", (long long)(offset-_off0), (long long)sb->st_size,
               path, name);

    return offset;
}


/* ---------------- superblock functions ------------ */

/* for writing variable-length version fields
 */
struct version *add_version(struct version *v, uuid_t uuid, size_t namelen, char *name)
{
    memcpy(v->uuid, uuid, sizeof(uuid_t));
    v->namelen = namelen;
    memcpy(v->name, name, namelen);
    return next_version(v);
}

/* creates superblock with nvers+1 object names/uuids, first one
 * is 'name' and gets new UUID.
 * returns number of sectors written
 */
int make_superblock(void *buf, size_t len, /* buffer to write it to */
                    char *name,
                    int   nvers,
                    struct version *versions)
{
    struct s3super *su = buf;
    su->magic = S3BU_MAGIC;
    su->version = 1;
    su->flags = FLAG_DIR_LOC | FLAG_DIR_DATA | FLAG_DIR_PACKED;

    struct version *v = su->versions;
    uuid_t uuid;
    uuid_generate(uuid);

    su->nvers = 1;
    v = add_version(v, uuid, strlen(name), name);
    for (int i = 0; i < nvers; i++) {
        v = add_version(v, versions->uuid, versions->namelen, versions->name);
        versions = next_version(versions);
        su->nvers++;
    }

    size_t total = (void*)v - buf;
    assert(total <= len);
    
    su->len = (total + 511) / 512;
    return su->len;
}

/* previous version has names and UUIDs for all ancestors - 
 * verify that we can read all them and that UUIDs match.
 */
void check_version(struct state *s, char *name, uuid_t uuid)
{
    char buf[512];
    memset(buf, 0, sizeof(buf));
    
    if (s->verbose)
        printf("checking %s:\n", name);

    do_read(s, name, buf, sizeof(buf), 0);
    struct s3super *su = (void*)buf;
    
    if (uuid_compare(su->versions[0].uuid, uuid) != 0) {
        char u1[40], u2[40];
        uuid_unparse(su->versions[0].uuid, u1);
        uuid_unparse(uuid, u2);
        fprintf(stderr, "%s: UUID %s, should be %s\n", name, u1, u2);
    }

    if (s->verbose)
        printf(" OK\n");
}

/* ------------- and main() ----------------- */

int main(int argc, char **argv)
{
    struct state _s, *s = &_s;
    struct s3super *old = NULL;
    int nvers = 0;
    
    argp_parse (&argp, argc, argv, ARGP_LONG_ONLY, 0, s);

    s3_init(s);
    s->start = gettime();
    dir_cache = avl_alloc_tree(dir_cmp, dir_free);
    if (s->verbose)
        printf("init done\n");
    
    if (s->incremental) {
        char *buf = malloc(4096);
        old = (void*)buf;

        do_read(s, s->old_name, buf, 4096, 0);

        nvers = s->me = old->nvers;
        s->names = malloc((nvers+1)*sizeof(*s->names));

        /* Read superblock and check UUID for each old version
         */
        struct version *v = old->versions;
        for (int i = 0; i < nvers; i++) {
            char *name = calloc(v->namelen+1, 1);
            memcpy(name, v->name, v->namelen);
            s->names[(nvers-1) - i] = name;
            check_version(s, name, v->uuid);
        }

        /* get the previous version rootdir
         */
        char _root2[512];
        struct s3dirent *root2 = (struct s3dirent*)_root2;
        off_t len = do_size(s, s->old_name);
        do_read(s, s->old_name, _root2, 512, len-512);

        /* directory locations
         */
        root2 = next_de(root2);
        struct s3dirloc *locs = malloc(root2->bytes);
        do_read(s, s->old_name, locs, root2->bytes, root2->off.s.sector * 512);
        int ndirs = root2->bytes / sizeof(struct s3dirloc);
        
        /* directory data 
         */
        root2 = next_de(root2);
        void *dirdat = malloc(root2->bytes);
        do_read(s, s->old_name, dirdat, root2->bytes, root2->off.s.sector * 512);

        if (s->verbose)
            printf("retrieved prior directory\n");

        /* and stash it. Note that there's no padding to sector boundaries.
         */
        for (size_t i = 0, offset = 0; i < ndirs; i++) {
            if (locs[i].bytes > 0) {
                cache_dir(locs[i].off, locs[i].bytes, dirdat + offset);
                offset += locs[i].bytes;
            }
        }
    }

    /* open the directory now, so we can bail if necessary 
     * before sending anything to S3
     */
    int fd = open(s->dir, O_RDONLY | O_NOFOLLOW);
    if (fd < 0) {
        fprintf(stderr, "%s : %s\n", s->dir, strerror(errno));
        exit(1);
    }
    struct stat sb;
    fstat(fd, &sb);
    if (!S_ISDIR(sb.st_mode)) {
        fprintf(stderr, "%s : not a directory\n", s->dir);
        exit(1);
    }
    
    /* write the new superblock
     */
    s->names[nvers] = s->new_name;

    char sb_buf[4096];
    memset(sb_buf, 0, sizeof(sb_buf));
    
    off_t offset = make_superblock(sb_buf, sizeof(sb_buf), s->new_name,
                                   nvers, old->versions);

    do_write(s, sb_buf, offset*512);
    assert((total_written & 511) == 0);
    assert(offset*512 == total_written);
    
    /* read the root dir of the old version
     */
    struct s3dirent *old_de = NULL;
    char old_de_buf[512];
    if (s->incremental) {
        old_de = (void*)old_de_buf;
        off_t size = do_size(s, s->old_name);
        do_read(s, s->old_name, old_de_buf, sizeof(old_de_buf), size-512);
    }

    /* stuff for stashing directory locations and data
     */
    s->max_dirs = 1000;
    s->dir_locs = malloc(s->max_dirs * sizeof(*(s->dir_locs)));
    s->n_dirs = 0;
    char template[] = "/tmp/s3bu.XXXXXX";
    if ((s->tmpdir_fd = mkstemp(template)) < 0) {
        fprintf(stderr, "tmpfile open: %s\n", strerror(errno));
        exit(1);
    }
    unlink(template);

    /* TODO: read all directory info from all old versions
     */
    
    struct s3dirent *_de, *de = malloc(512); /* root dir */
    memset(de, 0, 512);

    /* and do it...
     */
    path = &_path[strlen(s->tag)+1];
    offset = store_dir(s, offset, fd, de, &_de, s->tag, &sb, old_de);

    offset = store_data(s, offset, _de, &_de, "_dirloc_", s->dir_locs,
                        s->n_dirs*sizeof(struct s3dirloc));

    lseek(s->tmpdir_fd, 0, SEEK_SET);
    offset = store_file(s, offset, s->tmpdir_fd, _de, &_de, "_dirdat_", &sb);

    /* write the statfs info
     */
    stats.total_sectors = offset+1;
    struct s3statfs *sfs = (void*)de + 512;
    sfs[-1] = stats;            /* god what a hack */
    
    do_write(s, de, 512);
    assert((total_written & 511) == 0);

    do_done(s);

    printf("%d files (%ld sectors)\n", stats.files, (long)stats.file_sectors);
    printf("%d directories (%lld sectors, %lld bytes)\n", stats.dirs, 
	(long long)stats.dir_sectors, (long long)stats.dir_bytes);
    printf("%d symlinks\n", stats.symlinks);
    printf("%lld total sectors (%lld bytes)\n", (long long)stats.total_sectors,
           (long long)total_written);
    printf("truncated: %s\n", offset >= s->stopafter ? "YES" : "NO");
}

