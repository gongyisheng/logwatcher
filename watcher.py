import asyncio
import logging
import os
from typing import List, Set, Union

from glob import glob
from queue import Queue
from typing import Optional

FILE_PICK_TYPE = 1
DIR_PICK_TYPE = 2

class FileWatcher(object):
    def __init__(
        self, 
        path: Union[str, List[str]], 
        every: bool=False, 
        exclude: Union[str, List[str]]=None, 
        interval: int=60,
        qmaxsize: int=1000) -> None:
        """
        Initialize file watcher
        """ 
        self.files_included = self.parse_path(path) # file paths to watch
        self.files_excluded = self.parse_path(exclude)
        self.files = [f for f in self.files_included if f not in self.files_excluded]
        self.dirs = self.parse_path(path, DIR_PICK_TYPE) if every else [] # dir paths to watch for new files
        self.handlers = [] # a list of handlers

        self.fds = {p:self.open(p) for p in self.files} # file descriptors
        self.progress = {p:0 for p in self.files} # file watch progress
        
        self.queue = Queue(qmaxsize)
        self.interval = interval
        self.stop_watch_flag = False

    def parse_path(self, path: Union[str, List[str]], pick_type: int=FILE_PICK_TYPE) -> Set[str]:
        if isinstance(path, str):
            return self._parse_path(path, pick_type)
        elif isinstance(path, list):
            _path = set()
            for p in path:
                _path.update(self._parse_path(p, pick_type))
            return _path
    
    def _parse_path(self, path: str, pick_type: int=FILE_PICK_TYPE) -> Set[str]:
        """
        parse unix-style path to a list of paths,
        e.g. /var/log/*.log -> ["/var/log/a.log", "/var/log/b.log"]
        you can choose either file or dir as the result
        """
        _path = set()
        if path is None:
            return _path

        try:
            for p in glob(path):
                abspath = os.path.abspath(p)
                if pick_type==FILE_PICK_TYPE and os.path.isfile(abspath):
                    _path.add(abspath)
                elif pick_type==DIR_PICK_TYPE and os.path.isdir(abspath):
                    _path.add(abspath)
        except Exception as e:
            _path = []
            logging.warning(f"Parse path error: {str(e)}")

        return _path
    
    def add_path(self, path):
        """
        add file path to watch
        """
        _path = self.parse_path(path)
        for p in _path:
            if p not in self.fds.keys():
                self.fds[p] = self.open(p)
                self.progress[p] = 0
    
    def add_dir(self, path):
        """
        add dir path to watch
        """
        _path = self.parse_path(path, pick_type=DIR_PICK_TYPE)
        for p in _path:
            self.dirs.append(p)

    def open(self, path):
        """
        open file descriptor and seek to the beginning of the file
        """
        try:
            f = open(path, "r")
            f.seek(0)
            if not f.readable():
                f.close()
                logging.warning(f"File not readable: path={path}")
                return None
        except Exception as e:
            f = None
            logging.warning(f"Open file error: path={path}, msg={str(e)}")

        return f
    
    def close(self, path):
        """
        close file descriptor
        """
        if path in self.fds.keys():
            self.fds[path].close()
            self.fds[path] = None

    async def watch_file(self, path):
        """
        watch file and put new lines into queue
        """
        while not self.stop_watch_flag:
            try:
                f = self.fds[path]
                if f is not None:
                    msg = f.readline()
                    self.put(path, msg)   
                else:
                    self.fds.pop(path)
                    self.progress.pop(path)
            except Exception as e:
                logging.warning(f"Watch file error: path={path}, msg={str(e)}")
            await asyncio.sleep(self.interval)

    async def watch_dir(self, path):
        """
        watch dir and create new coroutines to watch new files
        """
        while not self.stop_watch_flag:
            _path = self.parse_path(path)
            for p in _path:
                if p not in self.fds.keys():
                    self.fds[p] = self.open(p)
                    self.progress[p] = 0
            await asyncio.sleep(self.interval)
    
    def stop(self):
        """
        stop watching
        """
        self.stop_watch_flag = True
        for p in self.fds.keys():
            self.close(p)

    def commit(self, path, offset):
        """
        commit progress
        """
        self.progress[path] += offset

    def put(self, path, msg):
        """
        put new message into queue
        """
        if msg:
            self.queue.put({"path": path, "msg": msg})

    def get(self):
        """
        get message from queue
        """
        _json = self.queue.get()
        path = _json["path"]
        msg = _json["msg"]
        self.commit(path, len(msg))
        return msg.strip()

    def seek(self, path, offset, whence=0):
        """
        set file pointer to a specific position
        """
        _path = self.parse_path(path)
        for p in _path:
            if p in self.fds.keys():
                try:
                    self.fds[p].seek(offset, whence)
                except Exception as e:
                    logging.error(f"Seek file error: path={path}, offset={offset}, \
                                    whence={whence}, msg={str(e)}")
    
    def register_handler(self, handler):
        """
        register a handler to handle messages
        """
        self.handlers.append(handler)

    async def handle_msg(self, msg):
        """
        handle message
        """
        for handler in self.handlers:
            if handler.match(msg):
                for func in handler.funcs:
                    await func(msg)
    
    async def run(self):
        """
        run file watcher
        """
        tasks = [asyncio.create_task(self.watch_file(p)) for p in self.paths]
        tasks += [asyncio.create_task(self.watch_dir(p)) for p in self.dirs]
        tasks += [asyncio.create_task(self.handle_msg(self.get()))]
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    path = "./test.txt"
    watcher = FileWatcher(path)
    async def run():
        watcher.run()
