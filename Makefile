#
# Makefile for s3backup / s3mount
#

ifdef NEED_ARGP
  EXTRA_LIBS := -largp
endif

S3DIR = $(PWD)/libs3

CC = gcc
LDLIBS := -lavl -ls3 -lcurl -luuid -lxml2 -lssl -lcrypto $(EXTRA_LIBS) 
CFLAGS = -O -ggdb3 -I $(S3DIR)/inc
LDFLAGS = -L $(S3DIR)/build/lib

all: deps s3backup s3mount

deps:
	@if [ ! -f /usr/include/uuid/uuid.h ] ; then \
		echo "Missing: uuid-dev"; false; fi
	@if [ ! -f /usr/include/avl.h ] ; then \
		echo "Missing: libavl-dev"; false; fi
	@if [ ! -f /usr/include/fuse.h ] ; then \
		echo "Missing: libfuse-dev"; false; fi

s3mount: LDLIBS += -lfuse

libs3-deps:
	@if [ ! -d /usr/include/libxml2 ] ; then \
		echo "Missing: libxml2-dev"; false; fi
	@if [ ! -d /usr/include/openssl ] ; then \
		echo "Missing: libssl-dev"; false; fi

libs3: libs3-deps
	git clone https://github.com/bji/libs3
	cd libs3; \
	patch -p1 < ../libs3-64bit.diff; \
	make

clean:
	rm -f s3backup s3mount
