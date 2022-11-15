import os
import multiprocessing as mp
import pytest

import bcachefs.zipfile.zipfile as bchzipf

from testing import filepath

MINI = "testdata/mini_bcachefs.img"
MANY = "testdata/many_bcachefs.img"

_TEST_IMAGES = [filepath(MINI), filepath(MANY)]


@pytest.fixture(scope="module", params=[MINI, MANY])
def zipfile(request) -> bchzipf.ZipFile:
    image = request.param
    assert os.path.exists(image)
    with bchzipf.ZipFile(image) as zipf:
        yield zipf


@pytest.fixture(scope="module")
def cursor(zipfile: bchzipf.ZipFile) -> bchzipf.Cursor:
    return zipfile.cache_dir()


@pytest.fixture(scope="module")
def cache_dir_cursor(zipfile: bchzipf.ZipFile) -> bchzipf.Cursor:
    for name in sorted(zipfile.namelist()):
        # skip lost+found
        if os.path.dirname(name) and "lost+found" not in name:
            return zipfile.cache_dir(os.path.dirname(name))


@pytest.fixture(params=["zipfile", "cursor"])
def bchzip(
    zipfile: bchzipf.ZipFile, cursor: bchzipf.Cursor, request
) -> bchzipf.ZipFileLikeMixin:
    zipfile, cursor
    kwargs = {**locals()}
    return kwargs[request.param]


@pytest.fixture(autouse=True)
def skip_if_not_image(request, bchzip: bchzipf.ZipFileLikeMixin):
    images_only = request.node.get_closest_marker("images_only")
    if images_only and bchzip.filename not in images_only.args[0]:
        pytest.skip(f"{images_only.args[0]} test only")


def test_cursor___iter__(zipfile: bchzipf.ZipFileLikeMixin):
    assert sorted(zipfile) == sorted(zipfile.namelist())


@pytest.mark.images_only([MINI])
def test_namelist(
    zipfile: bchzipf.ZipFileLikeMixin, cache_dir_cursor: bchzipf.Cursor
):
    assert sorted(zipfile.namelist()) == [
        "dir/subdir/file2",
        "file1",
        "n02033041/n02033041_3834.JPEG",
        "n02445715/n02445715_16523.JPEG",
        "n04467665/n04467665_63788.JPEG",
        "n04584207/n04584207_7936.JPEG",
        "n09332890/n09332890_29876.JPEG",
    ]
    assert sorted(cache_dir_cursor.namelist()) == sorted(
        name
        for name in zipfile.namelist()
        if name.startswith(cache_dir_cursor.pwd)
    )
    assert sorted(cache_dir_cursor.cache_dir("/").namelist()) == sorted(
        zipfile.namelist()
    )


@pytest.mark.parametrize("image", _TEST_IMAGES)
def test_open(image):
    image = filepath(image)
    assert os.path.exists(image)

    zipf = bchzipf.ZipFile(image)
    assert not zipf.closed
    assert list(zipf)
    zipf.close()
    assert zipf.closed
    assert not list(zipf)

    with bchzipf.ZipFile(image) as zipf:
        assert not zipf.closed
        assert list(zipf)


def test_read(zipfile: bchzipf.ZipFileLikeMixin):
    if zipfile.filename.endswith(MINI):
        assert zipfile.read("file1") == b"File content 1\n"
        assert zipfile.read("dir/subdir/file2") == b"File content 2\n"
    elif zipfile.filename.endswith(MANY):
        f0 = zipfile.read("0")
        assert f0 == b"test content\n"

        for ent in zipfile:
            assert zipfile.read(ent) == f0


@pytest.mark.images_only([MINI])
def test_cache_dir(zipfile: bchzipf.ZipFileLikeMixin):
    with zipfile.cache_dir() as cursor:
        assert cursor.pwd == ""
    with zipfile.cache_dir("dir/subdir") as cursor:
        assert cursor.pwd == "dir/subdir"
        with cursor.cache_dir() as c:
            assert c.pwd == ""
            assert c.read("dir/subdir/file2") == cursor.read("file2")
        with cursor.cd("/") as c:
            assert c.pwd == ""


def _count_size(zipf, name):
    import coverage

    coverage.process_startup()

    try:
        with zipf.open(name, "rb") as f:
            return len(f.read())
    except FileNotFoundError:
        return 0


def test_multiprocess(zipfile: bchzipf.ZipFileLikeMixin):
    files = zipfile.namelist()

    with mp.Pool(4) as p:
        sizes = p.starmap(_count_size, [(zipfile, n) for n in files])

    assert sum(sizes) > 1
