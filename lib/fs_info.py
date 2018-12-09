from functools import partial
from lib.utils import (join, rdict, rename_key_in_dict, split_file_and_dir_names)

def update_file_in_tree(fs_tree, dir, file, new_blocks):
    old_blocks = fs_tree[dir].get(file, {})
    fs_tree[dir][file] = new_blocks
    return old_blocks

def get_all_hashs_in_dir(dir):
    for file_name, blocks in dir.items():
        for hash in blocks:
            yield file_name, hash

def rename_dir(old_path, new_path, dir):
    return new_path+dir[len(old_path):]

def rename_sub_dirs(old_path, new_path, dirs):
    rename = partial(rename_dir, old_path, new_path)
    return [(dir,rename(dir)) for dir in dirs if dir.startswith(old_path)]

def update_file_in_hash_tbl(hash_tbl, dir, file, new_blocks={}, old_blocks = {}):
    for hash, blk in old_blocks.items():
        hash_blk = hash_tbl[hash]
        del hash_blk[join(dir, file)]
        if not hash_blk:
            del hash_tbl[hash]

    for hash, blk in new_blocks.items():
        hash_tbl[hash][join(dir, file)] = blk


class FsInfoMetaData(object):
    '''
    hash_tbl:
        {
            "<block_hash>": {
                "<file_path>" : {
                    "size":   <block_size>,
                    "offset": <block_offset_in_file>
                }, ...
            ]
        }, ...
    fs_tree:
        {
            "<directory_name>": {
                "<file_name>": {
                    "<block_hash>": {
                        "size":   <block_size>,
                        "offset": <block_offset_in_file>,
                    }, ...
                }, ....
            }, ....
        }
    '''
    def __init__(self, fs_tree):
        def build_hash_tbl(fs_tree):
            res = rdict()
            for dir, files in fs_tree.items():
                for file, blocks in files.items():
                    update_file_in_hash_tbl(res, dir, file, blocks)
            return res
        self.fs_tree  = fs_tree
        self.hash_tbl = build_hash_tbl(fs_tree)

    def show(self):
        print(json.dumps(self.fs_tree, indent=2))
        print(json.dumps(self.hash_tbl, indent=2))
        size = 0# sum(v.values()[0]['size'] for v in self.hash_tbl.values())
        total_size = sum(vv['size'] for v in self.hash_tbl.values() for vv in v.values())
        diff = total_size - size
        print(f'Reduced Size: {size}')
        print(f'Total size: {total_size}')
        print(f'Diff: {diff}')

    def get_all_files(self):
        sum_blocks_sizes = lambda blocks: sum(blk['size'] for blk in blocks.values())
        return {(join(dir,file), sum_blocks_sizes(blocks))
                for dir, files in self.fs_tree.items()
                for file, blocks in files.items()}


    def add_dir(self, path):
        self.fs_tree[path] = self.fs_tree.get(path, {})

    def add_file(self, path):
        dir, file = split_file_and_dir_names(path)
        dir = self.fs_tree[dir]
        dir[file] = dir.get(file, {})

    def rm(self, path):
        if path in self.fs_tree:
            for file_name, blocks in self.fs_tree.pop(path).items():
                update_file_in_hash_tbl(self.hash_tbl, path, file_name, old_blocks=blocks)
        else:
            path, file_name = split_file_and_dir_names(path)
            blocks = self.fs_tree[path].pop(file_name)
            update_file_in_hash_tbl(self.hash_tbl, path, file_name, old_blocks=blocks)

    def update_file(self, dir, file, blocks):
        old_blocks = update_file_in_tree(self.fs_tree, dir, file, blocks)
        update_file_in_hash_tbl(self.hash_tbl, dir, file, blocks, old_blocks)

    def rename(self, s_path, d_path):
        if s_path in self.fs_tree:
            dirs_to_rename = rename_sub_dirs(s_path, d_path, self.fs_tree)
            for old_path, new_path in dirs_to_rename:
                for file_name, hash in get_all_hashs_in_dir(self.fs_tree[old_path]):
                    rename_key_in_dict(join(old_path, file_name),
                                       join(new_path, file_name),
                                       self.hash_tbl[hash])
                rename_key_in_dict(old_path, new_path, self.fs_tree)
        else:
            old_path, old_file_name = split_file_and_dir_names(s_path)
            new_path, new_file_name = split_file_and_dir_names(d_path)
            rename_key_in_dict(old_path, new_path, self.fs_tree)
            rename_key_in_dict(old_file_name, new_file_name, self.fs_tree[new_path])
            for hash in self.fs_tree[new_path][new_file_name]:
                rename_key_in_dict(s_path, d_path, self.hash_tbl[hash])



