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

package org.elasticsearch.indices.fielddata.cache;

import com.google.common.cache.*;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SegmentReader;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.lucene.SegmentReaderUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.ordinals.GlobalOrdinalsIndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardUtils;
import org.elasticsearch.index.shard.service.IndexShard;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 */
public class IndicesFieldDataCache extends AbstractComponent implements RemovalListener<IndicesFieldDataCache.Key, RamUsage> {

    private final IndicesFieldDataCacheListener indicesFieldDataCacheListener;
    private final Cache<Key, RamUsage> cache;

    @Inject
    public IndicesFieldDataCache(Settings settings, IndicesFieldDataCacheListener indicesFieldDataCacheListener) {
        super(settings);
        this.indicesFieldDataCacheListener = indicesFieldDataCacheListener;
        String size = componentSettings.get("size", "-1");
        long sizeInBytes = componentSettings.getAsMemory("size", "-1").bytes();
        if (sizeInBytes > ByteSizeValue.MAX_GUAVA_CACHE_SIZE.bytes()) {
            logger.warn("reducing requested field data cache size of [{}] to the maximum allowed size of [{}]", new ByteSizeValue(sizeInBytes),
                    ByteSizeValue.MAX_GUAVA_CACHE_SIZE);
            sizeInBytes = ByteSizeValue.MAX_GUAVA_CACHE_SIZE.bytes();
            size = ByteSizeValue.MAX_GUAVA_CACHE_SIZE.toString();
        }
        final TimeValue expire = componentSettings.getAsTime("expire", null);
        CacheBuilder<Key, RamUsage> cacheBuilder = CacheBuilder.newBuilder()
                .removalListener(this);
        if (sizeInBytes > 0) {
            cacheBuilder.maximumWeight(sizeInBytes).weigher(new FieldDataWeigher());
        }
        // defaults to 4, but this is a busy map for all indices, increase it a bit
        cacheBuilder.concurrencyLevel(16);
        if (expire != null && expire.millis() > 0) {
            cacheBuilder.expireAfterAccess(expire.millis(), TimeUnit.MILLISECONDS);
        }
        logger.debug("using size [{}] [{}], expire [{}]", size, new ByteSizeValue(sizeInBytes), expire);
        cache = cacheBuilder.build();
    }

    public void close() {
        cache.invalidateAll();
    }

    public IndexFieldDataCache buildIndexFieldDataCache(IndexService indexService, Index index, FieldMapper.Names fieldNames, FieldDataType fieldDataType) {
        return new IndexFieldCache(logger, cache, indicesFieldDataCacheListener, indexService, index, fieldNames, fieldDataType);
    }

    public Cache<Key, RamUsage> getCache() {
        return cache;
    }

    @Override
    public void onRemoval(RemovalNotification<Key, RamUsage> notification) {
        Key key = notification.getKey();
        assert key != null && key.listeners != null;
        IndexFieldCache indexCache = key.indexCache;
        long sizeInBytes = key.sizeInBytes;
        final RamUsage value = notification.getValue();
        assert sizeInBytes >= 0 || value != null : "Expected size [" + sizeInBytes + "] to be positive or value [" + value + "] to be non-null";
        if (sizeInBytes == -1 && value != null) {
            sizeInBytes = value.getMemorySizeInBytes();
        }
        for (IndexFieldDataCache.Listener listener : key.listeners) {
            try {
                listener.onUnload(indexCache.fieldNames, indexCache.fieldDataType, notification.wasEvicted(), sizeInBytes);
            } catch (Throwable e) {
                // load anyway since listeners should not throw exceptions
                logger.error("Failed to call listener on field data cache unloading", e);
            }
        }
    }

    public static class FieldDataWeigher implements Weigher<Key, RamUsage> {

        @Override
        public int weigh(Key key, RamUsage ramUsage) {
            int weight = (int) Math.min(ramUsage.getMemorySizeInBytes(), Integer.MAX_VALUE);
            return weight == 0 ? 1 : weight;
        }
    }

    /**
     * A specific cache instance for the relevant parameters of it (index, fieldNames, fieldType).
     */
    static class IndexFieldCache implements IndexFieldDataCache, SegmentReader.CoreClosedListener, IndexReader.ReaderClosedListener {
        private final ESLogger logger;
        private final IndexService indexService;
        final Index index;
        final FieldMapper.Names fieldNames;
        final FieldDataType fieldDataType;
        private final Cache<Key, RamUsage> cache;
        private final IndicesFieldDataCacheListener indicesFieldDataCacheListener;

        IndexFieldCache(ESLogger logger,final Cache<Key, RamUsage> cache, IndicesFieldDataCacheListener indicesFieldDataCacheListener, IndexService indexService, Index index, FieldMapper.Names fieldNames, FieldDataType fieldDataType) {
            this.logger = logger;
            this.indexService = indexService;
            this.index = index;
            this.fieldNames = fieldNames;
            this.fieldDataType = fieldDataType;
            this.cache = cache;
            this.indicesFieldDataCacheListener = indicesFieldDataCacheListener;
            assert indexService != null;
        }

        @Override
        public <FD extends AtomicFieldData, IFD extends IndexFieldData<FD>> FD load(final AtomicReaderContext context, final IFD indexFieldData) throws Exception {
            final Key key = new Key(this, context.reader().getCoreCacheKey());
            //noinspection unchecked
            return (FD) cache.get(key, new Callable<AtomicFieldData>() {
                @Override
                public AtomicFieldData call() throws Exception {
                    SegmentReaderUtils.registerCoreListener(context.reader(), IndexFieldCache.this);

                    key.listeners.add(indicesFieldDataCacheListener);
                    final ShardId shardId = ShardUtils.extractShardId(context.reader());
                    if (shardId != null) {
                        final IndexShard shard = indexService.shard(shardId.id());
                        if (shard != null) {
                            key.listeners.add(shard.fieldData());
                        }
                    }
                    final AtomicFieldData fieldData = indexFieldData.loadDirect(context);
                    for (Listener listener : key.listeners) {
                        try {
                            listener.onLoad(fieldNames, fieldDataType, fieldData);
                        } catch (Throwable e) {
                            // load anyway since listeners should not throw exceptions
                            logger.error("Failed to call listener on atomic field data loading", e);
                        }
                    }
                    key.sizeInBytes = fieldData.getMemorySizeInBytes();
                    return fieldData;
                }
            });
        }

        public <IFD extends IndexFieldData.WithOrdinals<?>> IFD load(final IndexReader indexReader, final IFD indexFieldData) throws Exception {
            final Key key = new Key(this, indexReader.getCoreCacheKey());

            //noinspection unchecked
            return (IFD) cache.get(key, new Callable<RamUsage>() {
                @Override
                public RamUsage call() throws Exception {
                    indexReader.addReaderClosedListener(IndexFieldCache.this);
                    key.listeners.add(indicesFieldDataCacheListener);
                    final ShardId shardId = ShardUtils.extractShardId(indexReader);
                    if (shardId != null) {
                        IndexShard shard = indexService.shard(shardId.id());
                        if (shard != null) {
                            key.listeners.add(shard.fieldData());
                        }
                    }
                    final GlobalOrdinalsIndexFieldData ifd = (GlobalOrdinalsIndexFieldData) indexFieldData.localGlobalDirect(indexReader);
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
        public void onClose(Object coreKey) {
            cache.invalidate(new Key(this, coreKey));
        }

        @Override
        public void onClose(IndexReader reader) {
            cache.invalidate(new Key(this, reader.getCoreCacheKey()));
        }

        @Override
        public void clear() {
            for (Key key : cache.asMap().keySet()) {
                if (key.indexCache.index.equals(index)) {
                    cache.invalidate(key);
                }
            }
        }

        @Override
        public void clear(String fieldName) {
            for (Key key : cache.asMap().keySet()) {
                if (key.indexCache.index.equals(index)) {
                    if (key.indexCache.fieldNames.fullName().equals(fieldName)) {
                        cache.invalidate(key);
                    }
                }
            }
        }

        @Override
        public void clear(Object coreCacheKey) {
            cache.invalidate(new Key(this, coreCacheKey));
        }
    }

    public static class Key {
        public final IndexFieldCache indexCache;
        public final Object readerKey;

        public final List<IndexFieldDataCache.Listener> listeners = new ArrayList<>();
        long sizeInBytes = -1; // optional size in bytes (we keep it here in case the values are soft references)


        Key(IndexFieldCache indexCache, Object readerKey) {
            this.indexCache = indexCache;
            this.readerKey = readerKey;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            Key key = (Key) o;
            if (!indexCache.equals(key.indexCache)) return false;
            if (!readerKey.equals(key.readerKey)) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result = indexCache.hashCode();
            result = 31 * result + readerKey.hashCode();
            return result;
        }
    }
}
