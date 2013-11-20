/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardUtils;
import org.elasticsearch.index.shard.service.IndexShard;

import java.util.concurrent.Callable;

/**
 * A simple field data cache abstraction on the *index* level.
 */
public interface IndexFieldDataCache {

    <FD extends AtomicFieldData, IFD extends IndexFieldData<FD>> FD load(AtomicReaderContext context, IFD indexFieldData) throws Exception;

    /**
     * Clears all the field data stored cached in on this index.
     */
    void clear();

    /**
     * Clears all the field data stored cached in on this index for the specified field name.
     */
    void clear(String fieldName);

    void clear(Object coreCacheKey);

    interface Listener {

        void onLoad(FieldMapper.Names fieldNames, FieldDataType fieldDataType, AtomicFieldData fieldData);

        void onUnload(FieldMapper.Names fieldNames, FieldDataType fieldDataType, boolean wasEvicted, long sizeInBytes, @Nullable AtomicFieldData fieldData);
    }

    /**
     * The resident field data cache is a *per field* cache that keeps all the values in memory.
     */
    static abstract class FieldBased implements IndexFieldDataCache, SegmentReader.CoreClosedListener, RemovalListener<FieldBased.Key, AtomicFieldData> {
        @Nullable
        private final IndexService indexService;
        private final FieldMapper.Names fieldNames;
        private final FieldDataType fieldDataType;
        private final Cache<Key, AtomicFieldData> cache;

        protected FieldBased(@Nullable IndexService indexService, FieldMapper.Names fieldNames, FieldDataType fieldDataType, CacheBuilder cache) {
            this.indexService = indexService;
            this.fieldNames = fieldNames;
            this.fieldDataType = fieldDataType;
            cache.removalListener(this);
            //noinspection unchecked
            this.cache = cache.build();
        }

        @Override
        public void onRemoval(RemovalNotification<Key, AtomicFieldData> notification) {
            Key key = notification.getKey();
            if (key == null || key.listener == null) {
                return; // we can't do anything here...
            }
            AtomicFieldData value = notification.getValue();
            long sizeInBytes = key.sizeInBytes;
            if (sizeInBytes == -1 && value != null) {
                sizeInBytes = value.getMemorySizeInBytes();
            }
            key.listener.onUnload(fieldNames, fieldDataType, notification.wasEvicted(), sizeInBytes, value);
        }

        @Override
        public <FD extends AtomicFieldData, IFD extends IndexFieldData<FD>> FD load(final AtomicReaderContext context, final IFD indexFieldData) throws Exception {
            final Key key = new Key(context.reader().getCoreCacheKey());
            //noinspection unchecked
            return (FD) cache.get(key, new Callable<AtomicFieldData>() {
                @Override
                public AtomicFieldData call() throws Exception {
                    if (context.reader() instanceof SegmentReader) {
                        ((SegmentReader) context.reader()).addCoreClosedListener(FieldBased.this);
                    }

                    AtomicFieldData fieldData = indexFieldData.loadDirect(context);
                    key.sizeInBytes = fieldData.getMemorySizeInBytes();

                    if (indexService != null) {
                        ShardId shardId = ShardUtils.extractShardId(context.reader());
                        if (shardId != null) {
                            IndexShard shard = indexService.shard(shardId.id());
                            if (shard != null) {
                                key.listener = shard.fieldData();
                            }
                        }
                    }

                    if (key.listener != null) {
                        key.listener.onLoad(fieldNames, fieldDataType, fieldData);
                    }

                    return fieldData;
                }
            });
        }

        @Override
        public void clear() {
            cache.invalidateAll();
        }

        @Override
        public void clear(String fieldName) {
            cache.invalidateAll();
        }

        @Override
        public void clear(Object coreCacheKey) {
            cache.invalidate(new Key(coreCacheKey));
        }

        @Override
        public void onClose(Object coreCacheKey) {
            cache.invalidate(new Key(coreCacheKey));
        }

        static class Key {
            final Object readerKey;
            @Nullable
            Listener listener; // optional stats listener
            long sizeInBytes = -1; // optional size in bytes (we keep it here in case the values are soft references)

            Key(Object readerKey) {
                this.readerKey = readerKey;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                Key key = (Key) o;
                if (!readerKey.equals(key.readerKey)) return false;
                return true;
            }

            @Override
            public int hashCode() {
                return readerKey.hashCode();
            }
        }
    }

    static class Resident extends FieldBased {

        public Resident(@Nullable IndexService indexService, FieldMapper.Names fieldNames, FieldDataType fieldDataType) {
            super(indexService, fieldNames, fieldDataType, CacheBuilder.newBuilder());
        }
    }

    static class Soft extends FieldBased {

        public Soft(@Nullable IndexService indexService, FieldMapper.Names fieldNames, FieldDataType fieldDataType) {
            super(indexService, fieldNames, fieldDataType, CacheBuilder.newBuilder().softValues());
        }
    }
}
