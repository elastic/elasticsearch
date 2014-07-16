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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.lucene.SegmentReaderUtils;
import org.elasticsearch.index.fielddata.ordinals.GlobalOrdinalsIndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardUtils;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCacheListener;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * A simple field data cache abstraction on the *index* level.
 */
public interface IndexFieldDataCache {

    <FD extends AtomicFieldData, IFD extends IndexFieldData<FD>> FD load(AtomicReaderContext context, IFD indexFieldData) throws Exception;

    <IFD extends IndexFieldData.WithOrdinals<?>> IFD load(final IndexReader indexReader, final IFD indexFieldData) throws Exception;

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

        void onLoad(FieldMapper.Names fieldNames, FieldDataType fieldDataType, Accountable ramUsage);

        void onUnload(FieldMapper.Names fieldNames, FieldDataType fieldDataType, boolean wasEvicted, long sizeInBytes);
    }

    class None implements IndexFieldDataCache {

        @Override
        public <FD extends AtomicFieldData, IFD extends IndexFieldData<FD>> FD load(AtomicReaderContext context, IFD indexFieldData) throws Exception {
            return indexFieldData.loadDirect(context);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <IFD extends IndexFieldData.WithOrdinals<?>> IFD load(IndexReader indexReader, IFD indexFieldData) throws Exception {
            return (IFD) indexFieldData.localGlobalDirect(indexReader);
        }

        @Override
        public void clear() {
        }

        @Override
        public void clear(String fieldName) {
        }

        @Override
        public void clear(Object coreCacheKey) {

        }
    }

    /**
     * The resident field data cache is a *per field* cache that keeps all the values in memory.
     */
    static abstract class FieldBased implements IndexFieldDataCache, SegmentReader.CoreClosedListener, RemovalListener<FieldBased.Key, Accountable>, IndexReader.ReaderClosedListener {
        private final IndexService indexService;
        private final FieldMapper.Names fieldNames;
        private final FieldDataType fieldDataType;
        private final Cache<Key, Accountable> cache;
        private final IndicesFieldDataCacheListener indicesFieldDataCacheListener;
        private final ESLogger logger;

        protected FieldBased(ESLogger logger, IndexService indexService, FieldMapper.Names fieldNames, FieldDataType fieldDataType, CacheBuilder cache, IndicesFieldDataCacheListener indicesFieldDataCacheListener) {
            assert indexService != null;
            this.logger = logger;
            this.indexService = indexService;
            this.fieldNames = fieldNames;
            this.fieldDataType = fieldDataType;
            this.indicesFieldDataCacheListener = indicesFieldDataCacheListener;
            cache.removalListener(this);
            //noinspection unchecked
            this.cache = cache.build();
        }

        @Override
        public void onRemoval(RemovalNotification<Key, Accountable> notification) {
            final Key key = notification.getKey();
            assert key != null && key.listeners != null;

            final Accountable value = notification.getValue();
            long sizeInBytes = key.sizeInBytes;
            assert sizeInBytes >= 0 || value != null : "Expected size [" + sizeInBytes + "] to be positive or value [" + value + "] to be non-null";
            if (sizeInBytes == -1 && value != null) {
                sizeInBytes = value.ramBytesUsed();
            }
            for (Listener listener : key.listeners) {
                try {
                    listener.onUnload(fieldNames, fieldDataType, notification.wasEvicted(), sizeInBytes);
                } catch (Throwable e) {
                    logger.error("Failed to call listener on field data cache unloading", e);
                }
            }
        }

        @Override
        public <FD extends AtomicFieldData, IFD extends IndexFieldData<FD>> FD load(final AtomicReaderContext context, final IFD indexFieldData) throws Exception {
            final Key key = new Key(context.reader().getCoreCacheKey());
            //noinspection unchecked
            return (FD) cache.get(key, new Callable<AtomicFieldData>() {
                @Override
                public AtomicFieldData call() throws Exception {
                    SegmentReaderUtils.registerCoreListener(context.reader(), FieldBased.this);

                    key.listeners.add(indicesFieldDataCacheListener);
                    final ShardId shardId = ShardUtils.extractShardId(context.reader());
                    if (shardId != null) {
                        final IndexShard shard = indexService.shard(shardId.id());
                        if (shard != null) {
                            key.listeners.add(shard.fieldData());
                        }
                    }
                    final AtomicFieldData fieldData = indexFieldData.loadDirect(context);
                    key.sizeInBytes = fieldData.ramBytesUsed();
                    for (Listener listener : key.listeners) {
                        try {
                            listener.onLoad(fieldNames, fieldDataType, fieldData);
                        } catch (Throwable e) {
                            // load anyway since listeners should not throw exceptions
                            logger.error("Failed to call listener on atomic field data loading", e);
                        }
                    }
                    return fieldData;
                }
            });
        }

        public <IFD extends IndexFieldData.WithOrdinals<?>> IFD load(final IndexReader indexReader, final IFD indexFieldData) throws Exception {
            final Key key = new Key(indexReader.getCoreCacheKey());
            //noinspection unchecked
            return (IFD) cache.get(key, new Callable<Accountable>() {
                @Override
                public GlobalOrdinalsIndexFieldData call() throws Exception {
                    indexReader.addReaderClosedListener(FieldBased.this);

                    key.listeners.add(indicesFieldDataCacheListener);
                    final ShardId shardId = ShardUtils.extractShardId(indexReader);
                    if (shardId != null) {
                        IndexShard shard = indexService.shard(shardId.id());
                        if (shard != null) {
                            key.listeners.add(shard.fieldData());
                        }
                    }
                    GlobalOrdinalsIndexFieldData ifd = (GlobalOrdinalsIndexFieldData) indexFieldData.localGlobalDirect(indexReader);
                    key.sizeInBytes = ifd.ramBytesUsed();
                    for (Listener listener : key.listeners) {
                        try {
                            listener.onLoad(fieldNames, fieldDataType, ifd);
                        } catch (Throwable e) {
                            // load anyway since listeners should not throw exceptions
                            logger.error("Failed to call listener on global ordinals loading", e);
                        }
                    }

                    return ifd;
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

        @Override
        public void onClose(IndexReader reader) {
            cache.invalidate(reader.getCoreCacheKey());
        }

        static class Key {
            final Object readerKey;
            final List<Listener> listeners = new ArrayList<>();
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

        public Resident(ESLogger logger, IndexService indexService, FieldMapper.Names fieldNames, FieldDataType fieldDataType, IndicesFieldDataCacheListener indicesFieldDataCacheListener) {
            super(logger, indexService, fieldNames, fieldDataType, CacheBuilder.newBuilder(), indicesFieldDataCacheListener);
        }
    }

    static class Soft extends FieldBased {

        public Soft(ESLogger logger, IndexService indexService, FieldMapper.Names fieldNames, FieldDataType fieldDataType, IndicesFieldDataCacheListener indicesFieldDataCacheListener) {
            super(logger, indexService, fieldNames, fieldDataType, CacheBuilder.newBuilder().softValues(), indicesFieldDataCacheListener);
        }
    }
}
