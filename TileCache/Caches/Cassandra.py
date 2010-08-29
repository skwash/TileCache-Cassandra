# BSD Licensed, Copyright (c) 2010 - Josh Hansen
# Only works with Cassandra 0.6 for now.

from TileCache.Cache import Cache
from pycassa.connection import create_client_transport
import time, random

from cassandra.ttypes import Column, ColumnOrSuperColumn, ColumnParent, \
    ColumnPath, ConsistencyLevel, NotFoundException, SlicePredicate, \
    SliceRange, SuperColumn, Mutation, Deletion

class Cassandra(Cache):
    def __init__ (self, servers = ['127.0.0.1:9160'], keyspace = "TileCache", column_family = "TileCacheStd", **kwargs):
        Cache.__init__(self, **kwargs)
        
        self.keyspace=keyspace
        self.column_family=column_family
        
        if type(servers) is str: 
            servers = map(str.strip, servers.split(","))
            
        server = random.choice(servers)

        self.client, self.transport = create_client_transport(server, False, None, None)
        
        self.dict_class=dict
        
    def getKey(self, tile):
        
        return [tile.layer.name, "_".join(map(str, [tile.x, tile.y, tile.z]))]
        
    def get(self, tile):
        
        rowkey,colkey = self.getKey(tile)
        
        cp = ColumnParent(column_family=self.column_family, super_column=None)
        sp = self.create_SlicePredicate([colkey], rowkey, rowkey, False, 1)
        
        list_col_or_super = self.client.get_slice(self.keyspace, rowkey, cp, sp, ConsistencyLevel.ONE)
        
        if len(list_col_or_super) == 0:
            return None
        
        tile.data = self._convert_ColumnOrSuperColumns_to_dict_class(list_col_or_super, include_timestamp)        
        return tile.data
    
    def set(self, tile, data):
        if self.readonly:
            return data
            
        rowkey,colkey = self.getKey(tile)
        
        return self._insert(colkey, rowkey, data)
    
    def delete(self, tile):
        rowkey,colkey = self.getKey(tile)

        ts = time.time()
        
        sp = SlicePredicate(column_names=[colkey])
        deletion = Deletion(timestamp=ts, super_column=None, predicate=sp)
        mutation = Mutation(deletion=deletion)
        self.client.batch_mutate(self.keyspace, {rowkey: {self.column_family: [mutation]}}, ConsistencyLevel.ONE)

    def getLockName (self, tile):
        rowkey,colkey = self.getKey(tile)
        
        return self.getKey(tile) + ".lck"

    # @todo this
    def attemptLock (self, tile):
        #self.cf.insert('locks', { self.getLockName(tile) : "0" })
        return True
    
    # @todo and this
    def unlock (self, tile):
        #self.cache.delete( self.getLockName(tile) )
        return True
        
    def _insert (self, rowkey, colkey, data):
        ts = time.time()

        cols=[]

        column = Column(name=colkey, value=data, timestamp=ts)
        cols.append(Mutation(column_or_supercolumn=ColumnOrSuperColumn(column=column)))

        self.client.batch_mutate( self.keyspace, { rowkey: { self.column_family: cols } }, ConsistencyLevel.ZERO )
        
        return data


    # "Borrowed" from pycassa!
    
    def _convert_Column_to_base(self, column, include_timestamp):
        if include_timestamp:
            return (column.value, column.timestamp)
        return column.value

    def _convert_SuperColumn_to_base(self, super_column, include_timestamp):
        ret = self.dict_class()
        for column in super_column.columns:
            ret[column.name] = self._convert_Column_to_base(column, include_timestamp)
        return ret

    def _convert_ColumnOrSuperColumns_to_dict_class(self, list_col_or_super, include_timestamp):
        ret = self.dict_class()
        for col_or_super in list_col_or_super:
            if col_or_super.super_column is not None:
                col = col_or_super.super_column
                ret[col.name] = self._convert_SuperColumn_to_base(col, include_timestamp)
            else:
                col = col_or_super.column
                ret[col.name] = self._convert_Column_to_base(col, include_timestamp)
        return ret

    def create_SlicePredicate(self, columns, column_start, column_finish, column_reversed, column_count):
        if columns is not None:
            return SlicePredicate(column_names=columns)
        sr = SliceRange(start=column_start, finish=column_finish,
                        reversed=column_reversed, count=column_count)
        return SlicePredicate(slice_range=sr)
