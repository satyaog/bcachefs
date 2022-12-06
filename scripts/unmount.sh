#!/bin/sh
set -o errexit -o noclobber

if [ -z "${FORCE}" ]
then
	FORCE=0
fi

singularity exec instance://bcachefs fusermount3 -u /bch/mount/ || [ ${FORCE} -eq 1 ]
sleep 10
singularity instance stop bcachefs
