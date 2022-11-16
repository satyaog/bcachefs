import os
from typing import Union

import bcachefs.bcachefs as bch


class ZipFileLikeMixin:
    """Open a disk image to access its files

    Parameters
    ----------
    file: str
        path to the disk image

    Notes
    -----
    This in fact opens the disk image file for reading operations.

    Examples
    --------
    >>> with Bcachefs(path_to_file) as image:
    ...     with image.open('dir/subdir/file2', 'rb') as f:
    ...         data = f.read()
    ...         print(data.decode('utf-8'))
    File content 2
    <BLANKLINE>
    """

    def __iter__(self):
        return self.namelist()

    @property
    def closed(self) -> bool:
        """Is current disk image closed"""
        return super().unmounted

    @property
    def filename(self) -> str:
        """Path of the current disk image"""
        return super().filename

    def namelist(self):
        """Returns a list of files contained by this archive

        Notes
        -----
        Added for parity with Zipfile interface

        Examples
        --------

        >>> with Bcachefs(path_to_file, 'r') as image:
        ...     print(image.namelist())
        ['file1', 'n09332890/n09332890_29876.JPEG', 'dir/subdir/file2', 'n04467665/n04467665_63788.JPEG', 'n02033041/n02033041_3834.JPEG', 'n02445715/n02445715_16523.JPEG', 'n04584207/n04584207_7936.JPEG']
        """
        for root, _, files in super().walk():
            for f in set(files):
                yield os.path.join(root, f.name)

    def open(
        self, name: Union[str, int], mode: str = "rb", encoding: str = "utf-8"
    ):
        return super().open(name, mode, encoding)

    def read(self, inode: Union[str, int]) -> memoryview:
        return super().read(inode)

    def close(self):
        """Close the disk image. This invalidates all open files objects"""
        super().umount()

    def cache_dir(self, path: Union[str, bch.DirEnt] = "") -> "Cursor":
        """Open a cursor to specified directory and cache its content

        Parameters
        ----------
        path: str, DirEnt
            Path or DirEnt of a file or directory
        """
        return super().cd(path)


class Cursor(ZipFileLikeMixin, bch.Cursor):
    def __init__(self, *args, **kwargs):
        ZipFileLikeMixin.__init__(self)
        bch.Cursor.__init__(self, *args, **kwargs)

    def close(self):
        return bch.Cursor.close(self)

    def namelist(self):
        prefix = os.path.dirname(super().pwd)
        for root, _, files in bch.Cursor.walk(self):
            for f in set(files):
                yield os.path.join(prefix, root, f.name)


class ZipFile(ZipFileLikeMixin, bch.Bcachefs):
    CursorCls = Cursor

    def __init__(self, *args, **kwargs):
        ZipFileLikeMixin.__init__(self)
        bch.Bcachefs.__init__(self, *args, **kwargs)
