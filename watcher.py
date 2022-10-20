import logging
import os
import time
from glob import glob
from queue import Queue

class FileWatcher(object):
    def __init__(self, path, interval=60, qmaxsize=1000):
        self.paths = self.parse_path(path)
        self.fds = {p:self.open(p) for p in self.paths}
        self.progress = {p:0 for p in self.paths}
        
        self.queue = Queue(qmaxsize)
        self.interval = interval
        self.stop_watch_flag = False
    
    def parse_path(self, path):
        _path = []
        try:
            for p in glob(path):
                abspath = os.path.abspath(p)
                if os.path.isfile(abspath):
                    _path.append(abspath)
        except Exception as e:
            _path = []
            logging.warning(f"Parse path error: {str(e)}")

        return _path
    
    def add_path(self, path):
        _path = self.parse_path(path)
        for p in _path:
            if p not in self.fds.keys():
                self.fds[p] = self.open(p)
                self.progress[p] = 0

    def open(self, path):
        try:
            f = open(path, "r")
            f.seek(0, os.SEEK_END)
            if not f.readable():
                f.close()
                logging.warning(f"File not readable: path={path}")
                return None
        except Exception as e:
            f = None
            logging.warning(f"Open file error: path={path}, msg={str(e)}")

        return f
    
    def close(self, path):
        if path in self.fds.keys():
            self.fds[path].close()
            self.fds[path] = None

    def watch(self):
        while not self.stop_watch_flag:
            try:
                for p,f in self.fds.items():
                    if f is not None:
                        msg = self.f.readline()
                        self.put(p, msg)   
                    else:
                        self.fds.pop(p)
                        self.progress.pop(p)
            except Exception as e:
                logging.warning(f"Watch file error: path={p}, msg={str(e)}")
            finally:
                time.sleep(self.interval)
    
    def watch_directory(self, path):
        while not self.stop_watch_flag:
            _path = self.parse_path(path)
            for p in _path:
                if p not in self.fds.keys():
                    self.fds[p] = self.open(p)
                    self.progress[p] = 0
    
    def stop(self):
        self.stop_watch_flag = True
        for p in self.fds.keys():
            self.close(p)

    def commit(self, path, offset):
        self.progress[path] += offset

    def put(self, path, msg):
        if msg:
            self.queue.put({"path": path, "msg": msg.strip()})

    def get(self):
        _json = self.queue.get()
        path = _json["path"]
        msg = _json["msg"]
        self.commit(path, len(msg))
        return msg

    def seek(self, path, offset, whence=0):
        _path = self.parse_path(path)
        for p in _path:
            if p in self.fds.keys():
                try:
                    self.fds[p].seek(offset, whence)
                except Exception as e:
                    logging.error(f"Seek file error: path={path}, offset={offset}, \
                                    whence={whence}, msg={str(e)}")
