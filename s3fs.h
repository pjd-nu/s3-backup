/*
 * file:        s3fs.h
 * description: data structures for S3 backup
 */
#ifndef __S3FS_H__
#define __S3FS_H__

#define S3BU_MAGIC 'S' | ('3' << 8) | ('B' << 16) | ('U' << 24)

struct version {
    uuid_t   uuid;
    uint16_t namelen;
    char     name[0];
};

static inline struct version *next_version(struct version *v)
{
    return (void*)(v+1) + v->namelen;
}

/* variable-length superblock. 32 versions with version names of 20
 * bytes = 640 bytes; read 4K from front of object just to be sure.
 *
 * Note that versions are listed from newest to oldest in the
 * superblock, but numbered with 0 being the oldest.
 */
struct s3super {
    uint32_t       magic;       /* 'S3BU' */
    uint32_t       version;
    uint32_t       flags;
    uint32_t       len;         /* superblock length in sectors */
    uint32_t       nvers;
    struct version versions[0]; /* first version is this object */
};

/* TODO - need to handle multiple objects / versions
 */
union s3offset {
    struct {
        uint64_t sector : 48;
        uint64_t object : 16;
    } s;
    uint64_t n;
};

/* hard links are encoded as shortcuts to other dirents
 *  offset = offset of dest directory
 *  bytes = offset in directory
 *  uid/gid/ctime/ino should be set correctly (same as destination)
 * target must not be S_IFHRD
 * TODO: implement them
 */
#define S_IFHRD 0130000

/* extended attributes are stored in one or more sectors immediately
 * after the file data. Format is concatenated KV pairs:
 *   short len - total length of entry
 *   name      - with trailing nul
 *   value     - len - strlen(name) - 1 bytes
 */
struct s3xattr {
    uint16_t len;
    char     name[0];
};

#ifdef S3_USE_INUM
struct s3dirent {
    uint16_t mode;
    uint16_t uid;               /* it's 16 on ARM, i386 */
    uint16_t gid;
    uint32_t ctime;             /* use as mtime, too (ctime >= mtime) */
    uint32_t ino;               /* from origin filesystem */
    union s3offset off;
    uint64_t bytes : 52;
    uint64_t xattr : 12;        /* extended attributes length */
    uint8_t  namelen;
    char     name[];
} __attribute__((packed));
#else
struct s3dirent {
    uint16_t mode;
    uint16_t uid;               /* it's 16 on ARM, i386 */
    uint16_t gid;
    uint32_t ctime;             /* use as mtime, too (ctime >= mtime) */
    union s3offset off;
    uint64_t bytes : 52;
    uint64_t xattr : 12;        /* extended attributes length */
    uint8_t  namelen;
    char     name[];
} __attribute__((packed));
#endif

static inline struct s3dirent *next_de(struct s3dirent* de)
{
    return (void*)(de+1) + de->namelen;
}

/* this goes at the end of the last sector
 */
struct s3statfs {
    uint64_t total_sectors;
    uint32_t files;
    uint64_t file_sectors;
    uint32_t dirs;
    uint64_t dir_sectors;
    uint64_t dir_bytes;
    uint32_t symlinks;
    uint64_t sym_sectors;
};

/* format used for saving list of directory locations
 */
struct s3dirloc {
    union s3offset off;
    uint32_t       bytes;
} __attribute__((packed));

#define FLAG_USE_INUM   2       /* alternate directory format */
#define FLAG_DIR_LOC    4       /* 2nd dirent is array of s3dirloc */
#define FLAG_DIR_DATA   8       /* 3rd dirent is directory data */
#define FLAG_DIR_PACKED 16      /* directory data is packed */
#define FLAG_HARD_LINKS 32      /* includes hard links */

#endif
