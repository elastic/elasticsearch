/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.fielddata.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.CacheKey;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.cache.RemovalListener;
import org.elasticsearch.common.cache.RemovalNotification;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.ToLongBiFunction;

public class IndicesFieldDataCache implements RemovalListener<IndicesFieldDataCache.Key, Accountable>, Releasable {

    private static final Logger logger = LogManager.getLogger(IndicesFieldDataCache.class);

    public static final Setting<ByteSizeValue> INDICES_FIELDDATA_CACHE_SIZE_KEY = Setting.memorySizeSetting(
        "indices.fielddata.cache.size",
        ByteSizeValue.MINUS_ONE,
        Property.NodeScope
    );
    private final IndexFieldDataCache.Listener indicesFieldDataCacheListener;
    private final Cache<Key, Accountable> cache;

    public IndicesFieldDataCache(Settings settings, IndexFieldDataCache.Listener indicesFieldDataCacheListener) {
        this.indicesFieldDataCacheListener = indicesFieldDataCacheListener;
        final long sizeInBytes = INDICES_FIELDDATA_CACHE_SIZE_KEY.get(settings).getBytes();
        CacheBuilder<Key, Accountable> cacheBuilder = CacheBuilder.<Key, Accountable>builder().removalListener(this);
        if (sizeInBytes > 0) {
            cacheBuilder.setMaximumWeight(sizeInBytes).weigher(new FieldDataWeigher());
        }
        cache = cacheBuilder.build();
    }

    @Override
    public void close() {
        cache.invalidateAll();
    }

    public IndexFieldDataCache buildIndexFieldDataCache(IndexFieldDataCache.Listener listener, Index index, String fieldName) {
        return new IndexFieldCache(logger, cache, index, fieldName, indicesFieldDataCacheListener, listener);
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
                listener.onRemoval(
                    key.shardId,
                    indexCache.fieldName,
                    notification.getRemovalReason() == RemovalNotification.RemovalReason.EVICTED,
                    value.ramBytesUsed()
                );
            } catch (Exception e) {
                // load anyway since listeners should not throw exceptions
                logger.error("Failed to call listener on field data cache unloading", e);
            }
        }
    }

    public static class FieldDataWeigher implements ToLongBiFunction<Key, Accountable> {
        @Override
        public long applyAsLong(Key key, Accountable ramUsage) {
            int weight = (int) Math.min(ramUsage.ramBytesUsed(), Integer.MAX_VALUE);
            return weight == 0 ? 1 : weight;
        }
    }

    /**
     * A specific cache instance for the relevant parameters of it (index, fieldNames, fieldType).
     */
    static class IndexFieldCache implements IndexFieldDataCache, IndexReader.ClosedListener {
        private final Logger logger;
        final Index index;
        final String fieldName;
        private final Cache<Key, Accountable> cache;
        private final Listener[] listeners;

        IndexFieldCache(Logger logger, final Cache<Key, Accountable> cache, Index index, String fieldName, Listener... listeners) {
            this.logger = logger;
            this.listeners = listeners;
            this.index = index;
            this.fieldName = fieldName;
            this.cache = cache;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <FD extends LeafFieldData, IFD extends IndexFieldData<FD>> FD load(final LeafReaderContext context, final IFD indexFieldData)
            throws Exception {
            final ShardId shardId = ShardUtils.extractShardId(context.reader());
            final IndexReader.CacheHelper cacheHelper = context.reader().getCoreCacheHelper();
            if (cacheHelper == null) {
                throw new IllegalArgumentException("Reader " + context.reader() + " does not support caching");
            }
            final Key key = new Key(this, cacheHelper.getKey(), shardId);
            final Accountable accountable = cache.computeIfAbsent(key, k -> {
                cacheHelper.addClosedListener(IndexFieldCache.this);
                Collections.addAll(k.listeners, this.listeners);
                final LeafFieldData fieldData = indexFieldData.loadDirect(context);
                for (Listener listener : k.listeners) {
                    try {
                        listener.onCache(shardId, fieldName, fieldData);
                    } catch (Exception e) {
                        // load anyway since listeners should not throw exceptions
                        logger.error("Failed to call listener on atomic field data loading", e);
                    }
                }
                return fieldData;
            });
            return (FD) accountable;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <FD extends LeafFieldData, IFD extends IndexFieldData.Global<FD>> IFD load(
            final DirectoryReader indexReader,
            final IFD indexFieldData
        ) throws Exception {
            final ShardId shardId = ShardUtils.extractShardId(indexReader);
            final IndexReader.CacheHelper cacheHelper = indexReader.getReaderCacheHelper();
            if (cacheHelper == null) {
                throw new IllegalArgumentException("Reader " + indexReader + " does not support caching");
            }
            final Key key = new Key(this, cacheHelper.getKey(), shardId);
            final Accountable accountable = cache.computeIfAbsent(key, k -> {
                ElasticsearchDirectoryReader.addReaderCloseListener(indexReader, IndexFieldCache.this);
                Collections.addAll(k.listeners, this.listeners);
                final Accountable ifd = (Accountable) indexFieldData.loadGlobalDirect(indexReader);
                for (Listener listener : k.listeners) {
                    try {
                        listener.onCache(shardId, fieldName, ifd);
                    } catch (Exception e) {
                        // load anyway since listeners should not throw exceptions
                        logger.error("Failed to call listener on global ordinals loading", e);
                    }
                }
                return ifd;
            });
            return (IFD) accountable;
        }

        @Override
        public void onClose(CacheKey key) {
            cache.invalidate(new Key(this, key, null));
            // don't call cache.cleanUp here as it would have bad performance implications
        }

        @Override
        public void clear() {
            for (Key key : cache.keys()) {
                if (key.indexCache.index.equals(index)) {
                    cache.invalidate(key);
                }
            }
            // force eviction
            cache.refresh();
        }

        @Override
        public void clear(String fieldName) {
            for (Key key : cache.keys()) {
                if (key.indexCache.index.equals(index)) {
                    if (key.indexCache.fieldName.equals(fieldName)) {
                        cache.invalidate(key);
                    }
                }
            }
            // we call refresh because this is a manual operation, should happen
            // rarely and probably means the user wants to see memory returned as
            // soon as possible
            cache.refresh();
        }
    }

    public static class Key {
        public final IndexFieldCache indexCache;
        public final IndexReader.CacheKey readerKey;
        public final ShardId shardId;

        public final List<IndexFieldDataCache.Listener> listeners = new ArrayList<>();

        Key(IndexFieldCache indexCache, IndexReader.CacheKey readerKey, @Nullable ShardId shardId) {
            this.indexCache = indexCache;
            this.readerKey = readerKey;
            this.shardId = shardId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key key = (Key) o;
            if (indexCache.equals(key.indexCache) == false) return false;
            if (readerKey.equals(key.readerKey) == false) return false;
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
