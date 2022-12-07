#!/usr/bin/env python3
import asyncio
import contextlib
import os
import subprocess
from time import sleep

_DIR = os.path.dirname(__file__)


async def _make_disk_image(env: dict):
    make_disk_image_sh = os.path.join(_DIR, "make_disk_image.sh")
    main_p = await asyncio.create_subprocess_exec(
        make_disk_image_sh,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
    )
    cp_p = None
    failed = False
    line = await main_p.stderr.readline()
    while line:
        if b"fuse_init: activating writeback" in line:
            cp_p = subprocess.Popen(
                ["./cp.sh", "."],
                cwd=_DIR,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                env={**os.environ, "UNMOUNT": "1", "RM_FAILED": "1"},
            )
            break
        line = await main_p.stderr.readline()
    try:
        while True:
            line = await asyncio.wait_for(
                main_p.stderr.readline(), timeout=5 * 60
            )
            if not line:
                break
    except asyncio.TimeoutError:
        failed = True
    finally:
        # make sure the image is unmounted before exiting
        force_unmount = False
        cp_p_retcode = None
        if cp_p is not None:
            if cp_p.poll() is None:
                sleep(5 * 60)

            if cp_p.poll() is None:
                try:
                    cp_p.kill()
                    force_unmount = True
                except ProcessLookupError:
                    # process already done
                    pass
            cp_p_retcode = cp_p.poll()
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(main_p.communicate(), timeout=60)
        if main_p.returncode is None:
            try:
                main_p.kill()
                force_unmount = True
            except ProcessLookupError:
                # process already done
                pass
        force_unmount = (
            force_unmount or main_p.returncode != 0 or cp_p_retcode != 0
        )
        if force_unmount:
            subprocess.run(
                ["./unmount.sh"],
                cwd=_DIR,
                env={**os.environ, "FORCE": "1"},
            )
    return (
        not failed
        and await main_p.wait() == 0
        and (cp_p is not None and cp_p.wait() == 0)
    )


def make_disk_image(
    name: str = None,
    content: str = None,
    size: int = None,
    tmpdir: str = None,
    retry=10,
):
    subprocess.run(
        ["git-annex", "get", "scripts/"],
        cwd=os.path.join(_DIR, ".."),
        check=True,
    )
    for _ in range(retry):
        env = {
            **os.environ,
            "NAME": name if name is not None else "",
            "CONTENT_SRC": content if content is not None else "",
            "SIZE": "-1"
            if os.path.isfile(name)
            else str(size)
            if size is not None
            else "",
            "RM_FAILED": "1",
            "TMP_DIR": tmpdir if tmpdir is not None else "",
        }
        if asyncio.run(_make_disk_image(env)):
            return True
    return False


if __name__ == "__main__":
    import argparse

    def isdir(path):
        assert os.path.isdir(path)
        return path

    def mkdir(path):
        os.makedirs(path, exist_ok=True)
        return path

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "name",
        metavar="DISK.img",
        type=str,
        help="name of the disk image to create or update",
    )
    parser.add_argument(
        "content",
        metavar="DIR",
        type=isdir,
        help="directory that hold the content to be copied to the disk image",
    )
    parser.add_argument(
        "--size",
        metavar="INT",
        type=int,
        default=None,
        help="size of the disk image to create",
    )
    parser.add_argument(
        "--tmpdir",
        metavar="DIR",
        type=mkdir,
        default=None,
        help="directory to hold temporary data",
    )
    parser.add_argument("--retry", type=int, default=10, help=argparse.SUPPRESS)

    make_disk_image(**vars(parser.parse_args()))
