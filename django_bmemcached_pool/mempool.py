# -*- coding: utf-8 -

import os
import sys
import time
import weakref
import threading
import collections
from functools import wraps
import logging
import traceback

from django.conf import settings
from django_bmemcached.memcached import BMemcached

import bmemcached


def autoclose(ofun):
    @wraps(ofun)
    def fun(*args, **kwargs):
        try:
            ret = ofun(*args, **kwargs)
            return ret
        except Exception,e:
            args[0].starttime -= 10000000
            raise e
    return fun


class Client(bmemcached.Client):
    def __init__(self,*args, **kwargs):
        super(Client, self).__init__(*args, **kwargs)
        self.starttime = 0
    
    @autoclose
    def get(self, *args, **kwargs):
        return super(Client, self).get(*args, **kwargs)

    @autoclose
    def get_multi(self, *args, **kwargs):
        return super(Client, self).get_multi(*args, **kwargs)

    @autoclose
    def set(self, *args, **kwargs):
        return super(Client, self).set(*args, **kwargs)

    @autoclose
    def set_multi(self, *args, **kwargs):
        return super(Client, self).set_multi(*args, **kwargs)

    @autoclose
    def add(self, *args, **kwargs):
        return super(Client, self).add(*args, **kwargs)

    @autoclose
    def replace(self, *args, **kwargs):
        return super(Client, self).replace(*args, **kwargs)

    @autoclose
    def delete(self, *args, **kwargs):
        return super(Client, self).delete(*args, **kwargs)

    @autoclose
    def incr(self, *args, **kwargs):
        return super(Client, self).incr(*args, **kwargs)

    @autoclose
    def decr(self, *args, **kwargs):
        return super(Client, self).decr(*args, **kwargs)

    @autoclose
    def flush_all(self, *args, **kwargs):
        return super(Client, self).flush_all(*args, **kwargs)

    @autoclose
    def stats(self, *args, **kwargs):
        return super(Client, self).stats(*args, **kwargs)

        

class PoolBMemcached(BMemcached):
    """
    An implementation of a cache binding using python-binary-memcached
    A.K.A BMemcached and use pool.
    """
    def __init__(self, server, params):
        if not params.get('OPTIONS', None):
            params['OPTIONS'] = {}
        params['OPTIONS']['pool_conn_max_size'] = params['OPTIONS'].get('POOL_CONN_MAX_SIZE', 10)
        params['OPTIONS']['pool_conn_timeout'] = params['OPTIONS'].get('POOL_CONN_TIMEOUT', 600)
        params['OPTIONS']['pool_wait_timeout'] = params['OPTIONS'].get('POOL_WAIT_TIMEOUT', 30)    
        super(PoolBMemcached, self).__init__(server, params)
        self._lib = sys.modules[__name__]
        self.poolin = collections.deque()
        self.poolout = weakref.WeakSet()
        self.poollocal = threading.local()
        self.poollocal.client = None
        self.poollock = threading.Condition()
        self._pool_conn_max_size = self._options.get('pool_conn_max_size', 10)
        self._pool_conn_timeout = self._options.get('pool_conn_timeout', 600)
        self._pool_wait_timeout = self._options.get('pool_wait_timeout', 30)

    def _findconn(self):
        deadline = time.time() - self._pool_conn_timeout
        while True:
            try:
                item = self.poolin.popleft()
                if item.starttime > deadline:
                    self.poolout.add(item)
                    return item
                try:item.disconnect_all()
                except:pass
            except IndexError, e:
                break
    
    def _findout(self):
        deadline = time.time() - self._pool_conn_timeout
        nout = weakref.WeakSet()
        for item in self.poolout:
            if item.starttime > deadline - self._pool_wait_timeout - 10:
                nout.add(item)
            else:
                try:item.disconnect_all()
                except:pass
        self.poolout = nout
    
    def _newin(self):
        client = self._newclient()
        self.poolout.add(client)
        self.poollocal.client = client
        return client
      
    def _connsize(self):
        return len(self.poolin) + len(self.poolout)
    
    def _newclient(self):
        if self._options:
            client = self._lib.Client(self._servers,
                self._options.get('username', None),
                self._options.get('password', None))
        else:
            client = self._lib.Client(self._servers,)
        client.starttime = time.time()
        return client

    @property
    def _cache(self):
        if hasattr(self.poollocal, 'client') and self.poollocal.client:
            return self.poollocal.client
        try:
            self.poollock.acquire()
            item = self._findconn()
            if item:
                self.poollocal.client = item
                return item
            if len(self.poolin):
                logging.warn('pool in not 0 %s', len(self.poolin))
            if len(self.poolout) >= self._pool_conn_max_size:
                self.poollock.wait(self._pool_wait_timeout)
#                self._findout()
#                return
                item = self._findconn()
                if item:
                    self.poollocal.client = item
                    return item
                self._findout()
                if self._connsize() >= self._pool_conn_max_size:
                    logging.warn('waiting a connection in pool timeout %s %s', len(self.poolin), len(self.poolout))
                    return
            return self._newin()
        finally:
            try:
                print 'active item %s %s' % (len(self.poolout), len(self.poolin))
                self.poollock.release()
            except:pass
        
    
    def close(self, **kwargs):
        if not hasattr(self.poollocal, 'client'):
            return
        try:
            self.poollock.acquire()
            if not self.poollocal.client:
                return
            item = self.poollocal.client
            try:self.poolout.remove(item)
            except:pass
            self.poollocal.client = None
            if time.time() - item.starttime > self._pool_conn_timeout:
                try:item.disconnect_all()
                except:pass
            else:
                self.poolin.append(item)
            self.poollock.notify()
        finally:
            self.poollock.release()
