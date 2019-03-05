import os
from tornado.ioloop import IOLoop

QUARANTINE = os.getenv('S3_QUARANTINE', 'insights-upload-quarantine')
PERM = os.getenv('S3_PERM', 'insights-upload-perm-test')
REJECT = os.getenv('S3_REJECT', 'insights-upload-rejected')
WORKDIR = os.getenv('WORKDIR', '/tmp/uploads')
dirs = [WORKDIR,
        os.path.join(WORKDIR, QUARANTINE),
        os.path.join(WORKDIR, PERM),
        os.path.join(WORKDIR, REJECT)]


def stage():
    for dir_ in dirs:
        os.makedirs(dir_, exist_ok=True)


def _write(data, dest, uuid):
    dir_path = os.path.join(WORKDIR, dest)
    if dir_path in dirs and not os.path.isdir(dir_path):
        stage()
    with open(os.path.join(dir_path, uuid), 'w') as f:
        f.write(data)
        url = f
    return url.name


async def write(data, dest, uuid):
    return await IOLoop.current().run_in_executor(
        None, _write, data, dest, uuid
    )


def _ls(src, uuid):
    if os.path.isfile(os.path.join(WORKDIR, src, uuid)):
        return True


async def ls(src, uuid):
    return await IOLoop.current().run_in_executor(
        None, _ls, src, uuid
    )


def _copy(src, dest, uuid):
    os.rename(os.path.join(WORKDIR, src, uuid),
              os.path.join(WORKDIR, dest, uuid))
    return os.path.join(WORKDIR, dest, uuid)


async def copy(src, dest, uuid):
    return await IOLoop.current().run_in_executor(
        None, _copy, src, dest, uuid
    )
