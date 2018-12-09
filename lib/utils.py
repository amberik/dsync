import os, hashlib, zlib, shutil, math
from threading import Thread
from queue import Queue
from collections import defaultdict
from functools import partial
from xmlrpc.client import Binary

rdict = lambda: defaultdict(rdict)

DEFAULT_BLOCK_SIZE = 65536

DEBUG = False
def debug(func):
    def wrapper(*a, **kw):
        if DEBUG:
            return func(*a, **kw)
    return wrapper

def asinc_call(func):
    def worker():
        q = Queue()
        thread = Thread(target=lambda: q.put(func()))
        thread.daemon = True
        thread.start()
        yield
        thread.join()
        yield q.get_nowait()
    job = worker()
    next(job)
    return lambda: next(job)

def ui_message(msg):
    columns = shutil.get_terminal_size().columns
    size = min(columns, len(msg))
    print((msg[:size]+' '*(columns-len(msg))), end="\r", flush=True)

def convert_size(size_bytes):
    if size_bytes == 0:
        return "0B"
    size_name = ("B ", "KB", "MB", "GB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return "%5.2f %s" % (s, size_name[i])

def roller_gen():
    while True:
        for v in ['\\', '|', '/', '-']:
            yield v
roller = roller_gen()

def percent_gen(num):
    counter = 0
    if not num:
        num = 100
    unit = (num / 100.0)
    while True:
        yield int((counter / unit))
        counter +=1

join = os.path.join
split_file_and_dir_names = lambda path: (os.path.dirname(path), os.path.basename(path))

def rename_key_in_dict(o_key, n_key, dict_):
    dict_[n_key] = dict_.pop(o_key)

def _read_blocks(path, blocksize):
    if os.path.exists(path):
        with open(path, 'rb') as f:
            block = f.read(blocksize)
            while len(block) > 0:
                yield block
                block = f.read(blocksize)

def _get_blocks_info(path, blocksize):
    offset = 0
    for block in _read_blocks(path, blocksize):
        yield (hashlib.sha1(block).hexdigest(),
                dict(offset = offset,
                    size = len(block)))
        offset += len(block)

def fs_tree(path):
    fs_tree = {}
    for root, _, files in os.walk(path):
        files = ((join(root, f), f) for f in files)
        files = {f:get_blocks_info(f_path) for f_path, f in files}
        fs_tree[root] = files
    return fs_tree

def get_blocks_info(path, blocksize=DEFAULT_BLOCK_SIZE):
    return dict(_get_blocks_info(path, blocksize))

def read_block(path, offset, size):
    with open(path, 'rb') as f:
        f.seek(offset)
        return f.read(size)

def write_block(path, offset, block):
    if not os.path.exists(path):
        with open(path, 'w') as f:
            pass
    with open(path, 'r+b') as f:
        f.seek(offset)
        f.write(block)

def read_block_compr(path, offset, size):
    return zlib.compress(read_block(path, offset, size))

def write_block_compr(path, offset, block):
    write_block(path, offset, zlib.decompress(block))

def copy_block(s_path, s_offset, d_path, d_offset, size):
    block = read_block(s_path, s_offset, size)
    write_block(d_path, d_offset, block)

# This class is used by RPC ServerProxy
# to do FS operations on the server side
class ServerProvider(object):
    def fs_tree(self, path):
        return fs_tree(path)

    def mkdir(self, path):
        if not os.path.exists(path):
            os.makedirs(path)

    def truncate(self, path, size=0):
        self.mkfile(path, size)

    def mkfile(self, path, size=0):
        if not os.path.exists(path):
            with open(path, 'w') as f:
                pass
        else:
            with open(path, 'r+b') as f:
                f.truncate(size)

    def read_block(self, path, offset, size):
        return read_block(path, offset, size)

    def write_block(self, path, offset, block):
        return write_block(path, offset, block)

    def mv(self, s_path, d_path):
        os.rename(s_path, d_path)

    def rm(self, path):
        if os.path.isfile(path):
            os.remove(path)
        elif os.path.isdir(path):
            for root, dirs, files in os.walk(path, topdown=False):
                for file in files:
                    os.remove(join(root, file))
                for dir in dirs:
                    os.rmdir(join(root, dir))
            os.rmdir(path)

    def read_block_compr(self, path, offset, size):
        return read_block_compr(path, offset, size)

    def write_block_compr(self, path, offset, block):
        if isinstance(block, Binary):
            block = block.data
        write_block_compr(path, offset, block)

    def write_blocks_compr(self, files, block):
        if isinstance(block, Binary):
            block = block.data
        for path, block_info in files.items():
            write_block_compr(path, block_info['offset'], block)

    def write_blocks(self, files, block):
        if isinstance(block, Binary):
            block = block.data
        for path, block_info in files.items():
            write_block(path, block_info['offset'], block)

    def copy_block(self, s_path, s_offset, d_path, d_offset, size):
        copy_block(s_path, s_offset, d_path, d_offset, size)

    def ping(self):
        pass
