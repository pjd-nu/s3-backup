# S3 incremental backup and FUSE client

**s3-backup** performs a full or incremental backup of a directory hierarchy to a single S3 object; **s3mount** mounts a backup as a read-only FUSE file system.

Peter Desnoyers, Northeastern University
Solid-State Storage Lab
p.desnoyers@northeastern.edu

## Usage

environment variables: S3\_HOSTNAME, S3\_ACCESS\_KEY\_ID, S3\_SECRET\_ACCESS\_KEY 
(also available as command line options)

s3-backup --bucket BUCKET [--incremental OBJECT] \[--max #\] OBJECT /path

s3mount -o bucket=BUCKET[,http] OBJECT /mountpoint

## Description

S3-backup stores a snapshot of a file system as a single S3 object, using a simplified log-structured file system.
It supports incremental backups by chaining a sequence of these objects.
Although it does not support point-in-time snapshots, the incremental model allows creation of a *fuzzy snapshot* by following a full backup with an incremental one - inconsistencies will be bounded by the time it takes to traverse the local file system during the incremental backup.
It is coupled with a FUSE client that aggressively caches data and directories, flags files as cacheable to the kernel, stores symbolic links, and preserves owners, timestamps, and permissions.

Since it was created as a backup tool, it does not traverse mount points, but instead behaves like the `-xdev` flag to `find` - empty directories are stored for any encountered mount points.

Additional features:

- S3 hostname, access key and secret key can be provided by parameters as well as environment variables
- the `local` option to each program forces object names to be interpreted as local file paths, mostly for debugging purposes
- the `--max #` indicates that backup should stop after a certain amount of data (in bytes, M and G prefixes allowed) is written, allowing large full backups to be created as a chain of somewhat smaller ones. (files will not be broken across backups, so the limit is a soft one)
- `--exclude` lets you exclude directories or files matching a pattern, e.g excluding `.ssh` so you don't archive anyone's SSH keys

## Requirements

To build it on Ubuntu you'll need the following (I think - I haven't tested on a clean system):

- libs3 - a simple C library for S3. You'll need to grab it from https://github.com/bji/libs3 and build it. (make; make install will put it in /usr/local)
**warning:** you need to apply the included patch to libs3 to make it 64-bit safe, at least until they accept my bug report.
- libcurl, libxml2, libssl-dev - for building libs3
- libavl-dev, libuuid-dev

[edit - Makefile now checks for requirements, also the `libs3` target fetches, patches and builds libs3]
On Alpine you'll need argp-standalone as well. (I use Alpine and alpinewall for gateway machines on a couple of my networks, so it was my first use case)

## Planned additions

- parse bucket/key
- exclusion rules. Currently mount points are skipped; it would be useful to be able to specify additional rules for skipping e.g. `/tmp`
- config file support. All parameters are currently passed on the command line or environment variables
- inode numbers and hard links. Support for inode numbers (using the original host numbers) is commented out; support for hard links is designed but the usefulness in a read-only file system is debatable.
- extended attributes. The first part of the design is there.
- UID mapping. Currently it assumes that a volume is mounted on the same machine that it was created on.
- Object remapping. Each backup contains a UUID, and Incremental backups have a list of all prior ones they depend on, including name and UUID. It might be useful to force it to look under a different name for one or more of them.
- Scripts. It needs a naming scheme and set of cron scripts to run periodic backups.
- Automount. It would be nice to have an automount setup so that the snapshots are readily available, like Time Machine or NetApp's `.snapshots` directory.

## Design / implementation

S3 is an inode-less file system, vaguely like the original CD-ROM format but with 512-byte sectors, user IDs, and long file names. Describing it from the inside out:

### Offsets
Objects are divided into 512-byte sectors, and sectors are addressed by an 8-byte address:

| object# : 16 | sector offset : 48 |
|------|----------|

### Directory entries

Variable-sized directory entries look like this:

| mode : 16 | uid : 16 | gid : 16 | ctime : 32 | offset : 64 | size : 64 | namelen : 8 | name |
|---------|-------|-------|---------|---------|--------|----|---|

Note that names are not null-terminated; `printf("%.*s", de->namelen, de->name)` is your friend. There's a helper function `next_de(struct s3_dirent*)` to iterate through a directory.

### Object header

All fields (except versions) are 4 bytes:
|magic | version | flags | len | nversions | <versions> |
|----|----|----|----|----|----|

`len` is the length of the header in sectors. (although technically I'm not sure it's needed)

Versions are ordered newest (this object) to oldest, and look like this:

|   uuid | namelen : 16 | name |
|----|----|-----|

again with no null termination, and a iterator function `next_version`. For constructing offsets they're numbered in reverse, with the oldest version numbered 0.

### Object trailer

The last sector of the object combines a directory (starting at offset 0) and file system statistics (at the end of the sector)

- first entry: this points to the root directory
- second entry: hidden file, directory offsets
- third entry: packed directory contents

The directory offsets table has entries of the form:

|    offset : 64 | nbytes : 32 |
|----|----|

If you add then up, you can figure out the byte offset of each directory copy in the packed directory contents. This lets us load all the directories into memory at the beginning so we don't have to go back to S3 for directory lookups. Among other things, this makes incrementals *way* faster.

## Implementation cleanup
- the `s3mount` directory could be cached in a file instead of memory, either `mmap`ed or just accessed via `pread`, which would reduce memory usage when it's not active.
- transforming the dirloc table to include a byte offset in the packed directories would allow getting rid of libavl - just use binary search to find a directory.
- dump the directory location table to a file, like the packed directories, rather than saving in a buffer

actually, we could get rid of libavl and use interpolation search: https://en.wikipedia.org/wiki/Interpolation_search
