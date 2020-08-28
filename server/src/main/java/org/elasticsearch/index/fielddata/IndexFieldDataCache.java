/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
