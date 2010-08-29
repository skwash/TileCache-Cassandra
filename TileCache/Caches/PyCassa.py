# BSD Licensed, Copyright (c) 2010 - Josh Hansen
# Only works with PyCassa 0.3 & Cassandra 0.6 for now.

import pycassa
from TileCache.Cache import Cache
import time
from pycassa import NotFoundException

class PyCassa(Cache):
    def __init__ (self, servers = ['127.0.0.1:9160'], keyspace = "TileCache", column_family = "TileCache", use_ttl="no", **kwargs):
        Cache.__init__(self, **kwargs)
        
        self.keyspace=keyspace
        self.column_family=column_family
        
        if type(servers) is str: 
            servers = map(str.strip, servers.split(","))
            
        self.client = pycassa.connect(servers)

        self.cf = pycassa.ColumnFamily(self.client, self.keyspace, self.column_family, super=False)
        
    def getKey(self, tile):
        
        return [tile.layer.name, "_".join(map(str, [tile.x, tile.y, tile.z]))]
        
    def get(self, tile):
        
        rowkey,colkey = self.getKey(tile)
            
        try:
            tile.data = self.cf.get(rowkey, [colkey])
        except NotFoundException:
            return None
        
        return tile.data
    
    def set(self, tile, data):
        if self.readonly:
            return data
            
        rowkey,colkey = self.getKey(tile)
        
        self.cf.insert(rowkey, { colkey : data })
        
        return data
    
    def delete(self, tile):
        rowkey,colkey = self.getKey(tile)
        
        self.cf.remove(rowkey, [colkey])

    def getLockName (self, tile):
        rowkey,colkey = self.getKey(tile)
        
        return self.getKey(tile) + ".lck"

    # @todo this
    def attemptLock (self, tile):
        print "attemptLock Not Implemented"
        #self.cf.insert('locks', { self.getLockName(tile) : "0" })
        return True
    
    # @todo and this
    def unlock (self, tile):
        print "unlock Not Implemented"
        #self.cache.delete( self.getLockName(tile) )
        return True
        


