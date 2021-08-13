/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.index.shard.ShardId;

/**
 * A simple field data cache abstraction on the *index* level.
 */
public interface IndexFieldDataCache {

    <FD extends LeafFieldData, IFD extends IndexFieldData<FD>> FD load(LeafReaderContext context, IFD indexFieldData) throws Exception;

    <FD extends LeafFieldData, IFD extends IndexFieldData.Global<FD>> IFD load(DirectoryReader indexReader, IFD indexFieldData)
        throws Exception;

    /**
     * Clears all the field data stored cached in on this index.
     */
    void clear();

    /**
     * Clears all the field data stored cached in on this index for the specified field name.
     */
    void clear(String fieldName);

    interface Listener {

        /**
         * Called after the fielddata is loaded during the cache phase
         */
        default void onCache(ShardId shardId, String fieldName, Accountable ramUsage){}

        /**
         * Called after the fielddata is unloaded
         */
        default void onRemoval(ShardId shardId, String fieldName, boolean wasEvicted, long sizeInBytes){}
    }

    class None implements IndexFieldDataCache {

        @Override
        public <FD extends LeafFieldData, IFD extends IndexFieldData<FD>> FD load(LeafReaderContext context, IFD indexFieldData)
            throws Exception {
            return indexFieldData.loadDirect(context);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <FD extends LeafFieldData, IFD extends IndexFieldData.Global<FD>> IFD load(DirectoryReader indexReader,
                                                                                          IFD indexFieldData) throws Exception {
            return (IFD) indexFieldData.loadGlobalDirect(indexReader);
        }

        @Override
        public void clear() {
        }

        @Override
        public void clear(String fieldName) {
        }
    }
}
