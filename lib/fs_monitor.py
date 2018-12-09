import os, logging
from threading import Thread, Lock
from collections import namedtuple
from queue import Queue, Empty
from lib.inotify import INotify, flags, masks

InitialDiscoveryEvent     = namedtuple('InitialDiscoveryEvent', ['status'])
FileEventCreate           = namedtuple('FileEventCreate', ['path'])
FileEventRemove           = namedtuple('FileEventRemove', ['path'])
FileEventModify           = namedtuple('FileEventModify', ['path', 'file_name'])
FileEventMove             = namedtuple('FileEventMove'  , ['src_path', 'dst_path'])
DirectoryEventCreate      = namedtuple('DirectoryEventCreate', ['path'])
DirectoryEventRemove      = namedtuple('DirectoryEventRemove', ['path'])
DirectoryEventMove        = namedtuple('DirectoryEventMove'  , ['src_path', 'dst_path'])

is_create_dir   = lambda event: isinstance(event, DirectoryEventCreate)
is_create_file  = lambda event: isinstance(event, FileEventCreate)
is_del_dir      = lambda event: isinstance(event, DirectoryEventRemove)
is_del_file     = lambda event: isinstance(event, FileEventRemove)
is_mv_file      = lambda event: isinstance(event, FileEventMove)
is_mv_dir       = lambda event: isinstance(event, DirectoryEventMove)
is_mod_file     = lambda event: isinstance(event, FileEventModify)

class FSMonitorException(Exception):
    ...

class FSMonitor(object):
    watch_flags = (flags.CREATE | flags.DELETE | flags.MODIFY | flags.DELETE_SELF | flags.MOVED_FROM | flags.MOVED_TO)
    timeout = 10

    def _add_dir(self, path):
        if not os.path.isdir(path):
            raise FSMonitorException('The path: {} is not a directory'.format(path))
        self._add_wd(path)
        self._event_queue.put(DirectoryEventCreate(path))
        for root, dir_names, file_names in os.walk(path):
            for dir_name in dir_names:
                full_path = os.path.join(root, dir_name)
                self._add_wd(full_path)
                self._event_queue.put(DirectoryEventCreate(full_path))
            for file_name in file_names:
                full_path = os.path.join(root, file_name)
                self._event_queue.put(FileEventCreate(full_path))
                self._event_queue.put(FileEventModify(root, file_name))

    def _add_wd(self, path):
        wd = self._inotify.add_watch(path, self.watch_flags)
        self.wd_to_path[wd] = path
        self.path_to_wd[path] = wd

    def _del_dir(self, root_path):
        wds = [wd for wd, path in self.wd_to_path.items() if path.startswith(root_path)]
        for wd in wds:
            self._del_wd(wd)

    def _del_wd(self, wd):
        try:
            self._inotify.rm_watch(wd)
        except Exception:
            pass
        path = self.wd_to_path.get(wd, None)
        if path:
            del self.wd_to_path[wd]
            del self.path_to_wd[path]

    def _move_dirs(self, src_path, dst_path):
        src_paths = [path for path in self.wd_to_path.values() if path.startswith(src_path)]
        for src_path_ in src_paths:
            self._move_dir(src_path_, dst_path+src_path_[len(src_path):])

    def _move_dir(self, src_path, dst_path):
        wd = self.path_to_wd[src_path]
        del self.path_to_wd[src_path]
        self.wd_to_path[wd] = dst_path
        self.path_to_wd[dst_path] = wd

    def _monitor_fs_events(self):
        events      = []
        move_events = {}
        for event in self._inotify.read(self.timeout):
            if event.mask & masks.MOVE:
                if event.cookie in move_events:
                    move_events[event.cookie].append(event)
                    continue
                move_events[event.cookie] = [event]
                event = move_events[event.cookie]
            events.append(event)

        for event in events:
            if isinstance(event, list):
                if len(event) == 2:
                    for e in event:
                        if e.mask & flags.MOVED_FROM:
                            src_path = self.wd_to_path[e.wd]
                            src_path = os.path.join(src_path, e.name)
                        if e.mask & flags.MOVED_TO:
                            dst_path = self.wd_to_path[e.wd]
                            dst_path = os.path.join(dst_path, e.name)
                    if event[0].mask & flags.ISDIR:
                        self._move_dirs(src_path, dst_path)
                        self._event_queue.put(DirectoryEventMove(src_path, dst_path))
                    else:
                        self._event_queue.put(FileEventMove(src_path, dst_path))
                elif len(event) == 1:
                    event = event[0]
                    if event.mask & masks.MOVE:
                        path = self.wd_to_path.get(event.wd, None)
                        if path:
                            path = os.path.join(path, event.name)
                            if event.mask & flags.ISDIR:
                                if event.mask & flags.MOVED_FROM:
                                    self._del_dir(path)
                                    self._event_queue.put(DirectoryEventRemove(path))
                                if event.mask & flags.MOVED_TO:
                                    self._add_dir(path)
                            else:
                                if event.mask & flags.MOVED_FROM:
                                    self._event_queue.put(FileEventRemove(path))
                                if event.mask & flags.MOVED_TO:
                                    self._event_queue.put(FileEventCreate(path))
            else:
                root = self.wd_to_path.get(event.wd, None)
                if root:
                    path = os.path.join(root, event.name)
                    if event.mask & flags.ISDIR:
                        if event.mask & flags.CREATE:
                            self._add_dir(path)
                        if event.mask & flags.DELETE:
                            self._del_dir(path)
                            self._event_queue.put(DirectoryEventRemove(path))
                    else:
                        if event.mask & flags.CREATE:
                            self._event_queue.put(FileEventCreate(path))
                        if event.mask & flags.MODIFY:
                            self._event_queue.put(FileEventModify(root, event.name))
                        if event.mask & flags.DELETE:
                            self._event_queue.put(FileEventRemove(path))

    def __init__(self, path):
        self._event_queue = Queue()
        self._root        = path
        self._is_running  = False
        self._lock        = Lock()

    def events(self):
        event = True
        while event is not None:
            event = self._event_queue.get()
            if event:
                yield event
            self._event_queue.task_done()

    def flush_all_events(self):
        try:
            while True:
                self._event_queue.get_nowait()
        except Empty:
            pass

    def _worker(self):
        self.wd_to_path  = {}
        self.path_to_wd  = {}
        try:
            self._add_dir(self._root)
            self._event_queue.put(InitialDiscoveryEvent(True))
            while self.is_running:
                self._monitor_fs_events()
        except OSError as ex:
            if self.is_running:
                logging.exception(ex)
        except Exception as ex:
            logging.exception(ex)
        finally:
            self._event_queue.put(None)

    @property
    def is_running(self):
        with self._lock:
            return self._is_running

    @is_running.setter
    def is_running(self, value):
        with self._lock:
            self._is_running = value

    def is_running_set(self, value):
        with self._lock:
            old_val = self._is_running
            self._is_running = value
        return old_val

    def start(self):
        if not self.is_running_set(True):
            self._inotify = INotify()
            self._thread = Thread(target=self._worker)
            self._thread.daemon = True
            self._thread.start()
        else:
            raise FSMonitorException('The FSMonitor already started')

    def stop(self):
        if self.is_running_set(False):
            self._inotify.close()
            self._thread.join()
            self._inotify = None
            self._thread = None
        else:
            raise FSMonitorException('The FSMonitor already stopped')

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()


