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

s3mount: LDLIBS += -lfuse

