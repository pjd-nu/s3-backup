# S3 incremental backup and FUSE client

**s3-backup** performs a full or incremental backup of a directory hierarchy to a single S3 object; **s3mount** mounts a backup as a read-only FUSE file system.

## Usage

environment variables: S3\_HOSTNAME, S3\_ACCESS\_KEY\_ID, S3\_SECRET\_ACCESS\_KEY 
(also available as command line options)

s3-backup --bucket BUCKET [--incremental OBJECT] \[--max #\] OBJECT /path
s3mount -o bucket=BUCKET[,http] OBJECT /mountpoint

*todo: parse bucket/key*

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

## Requirements

To build it on Ubuntu you'll need the following (I think - I haven't tested on a clean system):

- libs3 - a simple C library for S3. You'll need to grab it from https://github.com/bji/libs3 and build it. (make; make install will put it in /usr/local)
- libcurl, libxml2, libssl-dev - for building libs3
- libavl-dev, libuuid-dev

On Alpine you'll need argp-standalone as well. (I use Alpine and alpinewall for gateway machines on a couple of my networks, so it was my first use case)

## Planned additions

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

