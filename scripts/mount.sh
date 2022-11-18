#!/bin/sh
set -o errexit -o noclobber

if [ -z "${NAME}" ]
then
	NAME=bcachefs.img
fi

if [ -z "${CONTENT_SRC}" ]
then
	CONTENT_SRC="${PWD}"
fi

if [ -z "${SCRIPTS_DIR}" ]
then
	SCRIPTS_DIR="${PWD}/scripts/"
fi

if [ -z "${TMP_DIR}" ]
then
	TMP_DIR="${PWD}/.tmp/"
fi

NAME=`readlink -f "${NAME}"`
CONTENT_SRC=`readlink -f "${CONTENT_SRC}"`
SCRIPTS_DIR=`readlink -f "${SCRIPTS_DIR}"`
TMP_DIR=`readlink -f "${TMP_DIR}"`

singularity instance stop bcachefs >/dev/null 2>&1 || echo -n
timeout --kill-after 30m --preserve-status 1h ${SCRIPTS_DIR}/bcachefs-tools.sif fsck -y "${NAME}"
singularity instance start --fakeroot -B "${PWD}/":"${PWD}/":ro \
	-B "${NAME}":/bch/disk.img:rw \
	-B "${NAME}".md5sums:/bch/disk.img.md5sums:ro \
	-B "${CONTENT_SRC}":/bch/content/:ro \
	-B "${SCRIPTS_DIR}":/bch/scripts/:ro \
	-B "${TMP_DIR}":/bch/tmp/:rw \
	bcachefs-tools.sif bcachefs
singularity run instance://bcachefs fusemount -s -f /bch/disk.img /bch/mount/
