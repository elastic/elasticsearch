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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardUtils;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;

/**
 */
public class IndicesFieldDataCache extends AbstractComponent implements RemovalListener<IndicesFieldDataCache.Key, Accountable> {

    public static final String FIELDDATA_CLEAN_INTERVAL_SETTING = "indices.fielddata.cache.cleanup_interval";
    public static final String FIELDDATA_CACHE_CONCURRENCY_LEVEL = "indices.fielddata.cache.concurrency_level";
    public static final String INDICES_FIELDDATA_CACHE_SIZE_KEY = "indices.fielddata.cache.size";


    private final IndicesFieldDataCacheListener indicesFieldDataCacheListener;
    private final Cache<Key, Accountable> cache;
    private final TimeValue cleanInterval;
    private final ThreadPool threadPool;
    private volatile boolean closed = false;

    @Inject
    public IndicesFieldDataCache(Settings settings, IndicesFieldDataCacheListener indicesFieldDataCacheListener, ThreadPool threadPool) {
        super(settings);
        this.threadPool = threadPool;
        this.indicesFieldDataCacheListener = indicesFieldDataCacheListener;
        final String size = settings.get(INDICES_FIELDDATA_CACHE_SIZE_KEY, "-1");
        final long sizeInBytes = settings.getAsMemory(INDICES_FIELDDATA_CACHE_SIZE_KEY, "-1").bytes();
        CacheBuilder<Key, Accountable> cacheBuilder = CacheBuilder.newBuilder()
                .removalListener(this);
        if (sizeInBytes > 0) {
            cacheBuilder.maximumWeight(sizeInBytes).weigher(new FieldDataWeigher());
        }
        // defaults to 4, but this is a busy map for all indices, increase it a bit by default
        final int concurrencyLevel =  settings.getAsInt(FIELDDATA_CACHE_CONCURRENCY_LEVEL, 16);
        if (concurrencyLevel <= 0) {
            throw new IllegalArgumentException("concurrency_level must be > 0 but was: " + concurrencyLevel);
        }
        cacheBuilder.concurrencyLevel(concurrencyLevel);

        logger.debug("using size [{}] [{}]", size, new ByteSizeValue(sizeInBytes));
        cache = cacheBuilder.build();

        this.cleanInterval = settings.getAsTime(FIELDDATA_CLEAN_INTERVAL_SETTING, TimeValue.timeValueMinutes(1));
        // Start thread that will manage cleaning the field data cache periodically
        threadPool.schedule(this.cleanInterval, ThreadPool.Names.SAME,
                new FieldDataCacheCleaner(this.cache, this.logger, this.threadPool, this.cleanInterval));
    }

    public void close() {
        cache.invalidateAll();
        this.closed = true;
    }

    public IndexFieldDataCache buildIndexFieldDataCache(IndexFieldDataCache.Listener listener, Index index, MappedFieldType.Names fieldNames, FieldDataType fieldDataType) {
        return new IndexFieldCache(logger, cache, index, fieldNames, fieldDataType, indicesFieldDataCacheListener, listener);
    }

    public Cache<Key, Accountable> getCache() {
        return cache;
    }

    @Override
    public void onRemoval(RemovalNotification<Key, Accountable> notification) {
        Key key = notification.getKey();
        assert key != null && key.listeners != null;
        IndexFieldCache indexCache = key.indexCache;
        final Accountable value = notification.getValue();
        for (IndexFieldDataCache.Listener listener : key.listeners) {
            try {
                listener.onRemoval(key.shardId, indexCache.fieldNames, indexCache.fieldDataType, notification.wasEvicted(), value.ramBytesUsed());
            } catch (Throwable e) {
                // load anyway since listeners should not throw exceptions
                logger.error("Failed to call listener on field data cache unloading", e);
            }
        }
    }

    public static class FieldDataWeigher implements Weigher<Key, Accountable> {

        @Override
        public int weigh(Key key, Accountable ramUsage) {
            int weight = (int) Math.min(ramUsage.ramBytesUsed(), Integer.MAX_VALUE);
            return weight == 0 ? 1 : weight;
        }
    }

    /**
     * A specific cache instance for the relevant parameters of it (index, fieldNames, fieldType).
     */
    static class IndexFieldCache implements IndexFieldDataCache, SegmentReader.CoreClosedListener, IndexReader.ReaderClosedListener {
        private final ESLogger logger;
        final Index index;
        final MappedFieldType.Names fieldNames;
        final FieldDataType fieldDataType;
        private final Cache<Key, Accountable> cache;
        private final Listener[] listeners;

        IndexFieldCache(ESLogger logger,final Cache<Key, Accountable> cache, Index index, MappedFieldType.Names fieldNames, FieldDataType fieldDataType, Listener... listeners) {
            this.logger = logger;
            this.listeners = listeners;
            this.index = index;
            this.fieldNames = fieldNames;
            this.fieldDataType = fieldDataType;
            this.cache = cache;
        }

        @Override
        public <FD extends AtomicFieldData, IFD extends IndexFieldData<FD>> FD load(final LeafReaderContext context, final IFD indexFieldData) throws Exception {
            final ShardId shardId = ShardUtils.extractShardId(context.reader());
            final Key key = new Key(this, context.reader().getCoreCacheKey(), shardId);
            //noinspection unchecked
            final Accountable accountable = cache.get(key, () -> {
                context.reader().addCoreClosedListener(IndexFieldCache.this);
                for (Listener listener : this.listeners) {
                    key.listeners.add(listener);
                }
                final AtomicFieldData fieldData = indexFieldData.loadDirect(context);
                for (Listener listener : key.listeners) {
                    try {
                        listener.onCache(shardId, fieldNames, fieldDataType, fieldData);
                    } catch (Throwable e) {
                        // load anyway since listeners should not throw exceptions
                        logger.error("Failed to call listener on atomic field data loading", e);
                    }
                }
                return fieldData;
            });
            return (FD) accountable;
        }

        @Override
        public <FD extends AtomicFieldData, IFD extends IndexFieldData.Global<FD>> IFD load(final IndexReader indexReader, final IFD indexFieldData) throws Exception {
            final ShardId shardId = ShardUtils.extractShardId(indexReader);
            final Key key = new Key(this, indexReader.getCoreCacheKey(), shardId);
            //noinspection unchecked
            final Accountable accountable = cache.get(key, () -> {
                indexReader.addReaderClosedListener(IndexFieldCache.this);
                for (Listener listener : this.listeners) {
                    key.listeners.add(listener);
                }
                final Accountable ifd = (Accountable) indexFieldData.localGlobalDirect(indexReader);
                for (Listener listener : key.listeners) {
                    try {
                        listener.onCache(shardId, fieldNames, fieldDataType, ifd);
                    } catch (Throwable e) {
                        // load anyway since listeners should not throw exceptions
                        logger.error("Failed to call listener on global ordinals loading", e);
                    }
                }
                return ifd;
            });
            return (IFD) accountable;
        }

        @Override
        public void onClose(Object coreKey) {
            cache.invalidate(new Key(this, coreKey, null));
            // don't call cache.cleanUp here as it would have bad performance implications
        }

        @Override
        public void onClose(IndexReader reader) {
            cache.invalidate(new Key(this, reader.getCoreCacheKey(), null));
            // don't call cache.cleanUp here as it would have bad performance implications
        }

        @Override
        public void clear() {
            for (Key key : cache.asMap().keySet()) {
                if (key.indexCache.index.equals(index)) {
                    cache.invalidate(key);
                }
            }
            // Note that cache invalidation in Guava does not immediately remove
            // values from the cache. In the case of a cache with a rare write or
            // read rate, it's possible for values to persist longer than desired.
            //
            // Note this is intended by the Guava developers, see:
            // https://code.google.com/p/guava-libraries/wiki/CachesExplained#Eviction
            // (the "When Does Cleanup Happen" section)

            // We call it explicitly here since it should be a "rare" operation, and
            // if a user runs it he probably wants to see memory returned as soon as
            // possible
            cache.cleanUp();
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
            // we call cleanUp() because this is a manual operation, should happen
            // rarely and probably means the user wants to see memory returned as
            // soon as possible
            cache.cleanUp();
        }

        @Override
        public void clear(IndexReader indexReader) {
            cache.invalidate(new Key(this, indexReader.getCoreCacheKey(), null));
            // don't call cache.cleanUp here as it would have bad performance implications
        }
    }

    public static class Key {
        public final IndexFieldCache indexCache;
        public final Object readerKey;
        public final ShardId shardId;

        public final List<IndexFieldDataCache.Listener> listeners = new ArrayList<>();

        Key(IndexFieldCache indexCache, Object readerKey, @Nullable ShardId shardId) {
            this.indexCache = indexCache;
            this.readerKey = readerKey;
            this.shardId = shardId;
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

    /**
     * FieldDataCacheCleaner is a scheduled Runnable used to clean a Guava cache
     * periodically. In this case it is the field data cache, because a cache that
     * has an entry invalidated may not clean up the entry if it is not read from
     * or written to after invalidation.
     */
    public class FieldDataCacheCleaner implements Runnable {

        private final Cache<Key, Accountable> cache;
        private final ESLogger logger;
        private final ThreadPool threadPool;
        private final TimeValue interval;

        public FieldDataCacheCleaner(Cache cache, ESLogger logger, ThreadPool threadPool, TimeValue interval) {
            this.cache = cache;
            this.logger = logger;
            this.threadPool = threadPool;
            this.interval = interval;
        }

        @Override
        public void run() {
            long startTimeNS = System.nanoTime();
            if (logger.isTraceEnabled()) {
                logger.trace("running periodic field data cache cleanup");
            }
            try {
                this.cache.cleanUp();
            } catch (Exception e) {
                logger.warn("Exception during periodic field data cache cleanup:", e);
            }
            if (logger.isTraceEnabled()) {
                logger.trace("periodic field data cache cleanup finished in {} milliseconds", TimeValue.nsecToMSec(System.nanoTime() - startTimeNS));
            }
            // Reschedule itself to run again if not closed
            if (closed == false) {
                threadPool.schedule(interval, ThreadPool.Names.SAME, this);
            }
        }
    }
}
