#!/usr/bin/env python3
import argparse, json, os
from itertools import takewhile
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy, MultiCall, Fault, Binary, Marshaller
from lib.fs_info import FsInfoMetaData
from lib.utils import (join, rdict, ServerProvider, Parallel, ParallelCall, read_block_compr, read_block, fs_tree,
                       convert_size, ui_message, percent_gen, roller, debug, asinc_call,
                       DEFAULT_BLOCK_SIZE, get_blocks_info, ThreadPool)

from lib.fs_monitor import (FSMonitor, InitialDiscoveryEvent, is_create_dir, is_create_file,
                            is_del_dir, is_del_file, is_mv_file, is_mv_dir, is_mod_file)


PORT = 12345

parser = argparse.ArgumentParser()
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('-c', '--client', nargs=2, metavar=('PATH', 'DST_IP'), help='The Directory path to sync and destination IP')
group.add_argument('-s', '--server', metavar='PATH', default='./', help='The Directory path to store')
Marshaller.dispatch[type(rdict())] = Marshaller.dispatch[dict]

class DSynch(FsInfoMetaData):
    @debug
    def _consistency_check(self):
        req = asinc_call(lambda: self.peer.fs_tree('.'))
        remote_fs_meta_data = FsInfoMetaData(req())
        local  = [join(dir,file) for dir, files in fs_tree('.').items() for file in files]
        remote = [join(dir,file) for dir, files in remote_fs_meta_data.fs_tree.items() for file in files]
        for l_dir in local:
            assert l_dir in remote
        for r_dir in remote:
            assert r_dir in local
        assert set(remote) == set(local)
        assert self.fs_tree == remote_fs_meta_data.fs_tree
        assert self.hash_tbl == remote_fs_meta_data.hash_tbl

    def __init__(self, peer):
        self.peer    = peer
        self.blocksize = DEFAULT_BLOCK_SIZE
        self.monitor = FSMonitor('.')

    # Collects FsInfoMetaData on local host
    def _index_soure_dir(self):
        is_init_disc = lambda x: type(x)is not InitialDiscoveryEvent
        for event in takewhile(is_init_disc, self.monitor.events()):
            ui_message("Initial Synch: Indexing source directory... {}".format(next(roller)))
            self._update_local_fs_info_mdata(event)

    # Creates directory tree on remote host (doesn't copy data), during initial synch.
    def _reconstruct_remote_tree(self, remote_fs_meta_data):
        dirs_to_create = [dir for dir in self.fs_tree if dir not in remote_fs_meta_data.fs_tree]
        files_to_create = self.get_all_files() - remote_fs_meta_data.get_all_files()
        multicall = MultiCall(self.peer)

        for dir in dirs_to_create:
            multicall.mkdir(dir)
        for file, size in files_to_create:
            multicall.mkfile(file, size)

        pc = percent_gen(len(dirs_to_create)+len(files_to_create))
        for _ in multicall():
            ui_message("Initial Synch: Reconstruction of tree...  {}%".format(next(pc)))

    # Copies data to remote host, during initial synch.
    def _copy_data(self, remote_fs_meta_data):
        hashs_to_copy =  [hash for hash in self.hash_tbl if hash not in remote_fs_meta_data.hash_tbl]
        self._copy_data_by_hash(hashs_to_copy)

    def _copy_data_by_hash(self, hashs_to_copy):
        commited_size  = 0
        sent_data_size = 0
        sent_compr_data_size = 0
        tasks = ParallelCall(self.peer)
        pc = percent_gen(len(hashs_to_copy)*2)
        for hash in hashs_to_copy:
            files = self.hash_tbl[hash]
            path, block = next(iter(files.items()))
            compr_block = read_block_compr(path, **block)
            tasks.write_blocks_compr(files, compr_block)
            sent_data_size       += block['size']*len(files)
            sent_compr_data_size += len(compr_block)
            ui_message("Initial Synch: Sending files data. "
                       "Not compressed: {} compressed: {} {}%".format(convert_size(sent_data_size),
                                                                convert_size(sent_compr_data_size),
                                                                next(pc)))
        for result in tasks():
            commited_size += result
            ui_message("Initial Synch: Commiting files data. "
                       "Commited: {}/{}     {}%".format(convert_size(commited_size),
                                                        convert_size(sent_data_size),
                                                        next(pc)))


    # Updates local FsInfoMetaData according to
    # events for file system.
    def _update_local_fs_info_mdata(self, event):
        if is_create_dir(event):
            self.add_dir(event.path)
        if is_create_file(event):
            self.add_file(event.path)
        if is_del_dir(event) or is_del_file(event):
            self.rm(event.path)
        if is_mv_file(event) or is_mv_dir(event):
            self.rename(event.src_path, event.dst_path)
        if is_mod_file(event):
            file = join(event.path, event.file_name)
            blocks = get_blocks_info(file, self.blocksize)
            self.update_file(event.path, event.file_name,
                             blocks)

    # Updates remote file system and local FsInfoMetaData
    # according to events coming from local file system.
    def _update_remote_fs(self, event):
        if is_create_dir(event):
            # Create directory on remote host
            self.peer.mkdir(event.path)
            self.add_dir(event.path)
        if is_del_dir(event) or is_del_file(event):
            # Move file/directory on remote host
            self.peer.rm(event.path)
            # Update local FsInfoMetaData
            self.rm(event.path)
        if is_mv_file(event) or is_mv_dir(event):
            # Move file on remote host
            self.peer.mv(event.src_path, event.dst_path)
            # Update local FsInfoMetaData
            self.rename(event.src_path, event.dst_path)
        if is_create_file(event):
            # Clreate file on remote host
            self.peer.mkfile(event.path)
            # Update local FsInfoMetaData
            self.add_file(event.path)
        if is_mod_file(event):
            file = join(event.path, event.file_name)
            file_blocks = get_blocks_info(file, self.blocksize)
            size = sum(block['size'] for block in file_blocks.values())
            # Truncate the file on remote host with the new size.
            self.peer.truncate(file, size)
            # Get current blocks for the file.
            blocks = self.fs_tree[event.path].get(event.file_name, {})
            # Find new blocks for the file.
            new_blocks = {hash for hash in file_blocks if hash not in blocks}
            # Check if we already have these blocks on peer. This allows to make a copy remotely.
            remote_copy_blocks = {hash for hash in new_blocks if hash in self.hash_tbl}
            # Here we calculate the blocks which we need to sent.
            blocks_to_send = new_blocks - remote_copy_blocks
            # Do remote copy
            for hash in remote_copy_blocks:
                # Check the case that we don't override file with itself
                file_to_copy_form = next(filter(lambda f: f != file, self.hash_tbl[hash]), None)
                if file_to_copy_form:
                    self.peer.copy_block(file_to_copy_form,
                                         self.hash_tbl[hash][file_to_copy_form]['offset'],
                                         file,
                                         file_blocks[hash]['offset'],
                                         file_blocks[hash]['size'])
                else:
                    blocks_to_send.add(hash)
            # Sending block over network
            for hash in blocks_to_send:
                block = file_blocks[hash]
                compr_block = read_block_compr(file, **block)
                self.peer.write_blocks_compr({file:block}, compr_block)
            self.update_file(event.path, event.file_name, file_blocks)

    def run(self):
        self.fs_tree  = rdict()
        self.hash_tbl = rdict()
        self.monitor.flush_all_events()
        # Check connectivity
        self.peer.ping()
        with self.monitor:
            self.do_initial_synch()
            #self.do_delta_synch()

    def do_initial_synch(self):
        req = asinc_call(lambda: self.peer.fs_tree('.'))
        self._index_soure_dir()
        remote_fs_meta_data = FsInfoMetaData(req())
        self._reconstruct_remote_tree(remote_fs_meta_data)
        self._copy_data(remote_fs_meta_data)
        self._consistency_check()
        ui_message('Initial Synch completed!')

    def do_delta_synch(self):
        ui_message('Delta Synch...')
        for event in self.monitor.events():
            self._update_remote_fs(event)

def run_client(root, ip_address):
    with ServerProxy(f'http://{ip_address}:{PORT}', allow_none=True) as client:
        try:
            os.chdir(root)
            dsynch = DSynch(client)
            dsynch.run()
        except Fault as err:
            print(err.faultString)
        except Exception as err:
            print(err)

def run_server(root):
    with SimpleXMLRPCServer(('', PORT), logRequests=False, allow_none=True) as server:
        try:
            os.chdir(root)
            with Parallel(ServerProvider(), 10) as server_provider:
                server.register_multicall_functions()
                server.register_instance(server_provider)
                server.serve_forever()
        except Exception as err:
            print(err)


def main(args: argparse.Namespace) -> None:
    if args.client:
        run_client(*args.client)
    else:
        run_server(args.server)

if __name__ == "__main__":
    args = parser.parse_args()
    try:
        main(args)
    except KeyboardInterrupt:
        pass
