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

package org.elasticsearch.indices.cache.filter;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReader.CoreClosedListener;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilterCache;
import org.apache.lucene.search.FilterCachingPolicy;
import org.apache.lucene.search.LRUFilterCache;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.MemorySizeValue;
import org.elasticsearch.index.cache.filter.FilterCacheStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardUtils;
import org.elasticsearch.node.settings.NodeSettingsService;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;

public class IndicesFilterCache extends AbstractComponent implements FilterCache, Closeable {

    public static final String INDICES_CACHE_FILTER_SIZE = "indices.cache.filter.size";
    public static final String INDICES_CACHE_FILTER_COUNT = "indices.cache.filter.count";

    private volatile LRUFilterCache cache;

    private volatile String size;
    private volatile int count;

    private final ShardMap shards;
    private volatile long sharedRamBytesUsed;

    class ApplySettings implements NodeSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            // TODO: updating the settings discards the entire cache, should we really
            // keep these settings dynamic?
            boolean replace = false;
            String size = settings.get(INDICES_CACHE_FILTER_SIZE, IndicesFilterCache.this.size);
            if (!size.equals(IndicesFilterCache.this.size)) {
                logger.info("updating [{}] from [{}] to [{}]",
                        INDICES_CACHE_FILTER_SIZE, IndicesFilterCache.this.size, size);
                IndicesFilterCache.this.size = size;
                replace = true;
            }
            int count = settings.getAsInt(INDICES_CACHE_FILTER_COUNT, IndicesFilterCache.this.count);
            if (count != IndicesFilterCache.this.count) {
                logger.info("updating [{}] from [{}] to [{}]",
                        INDICES_CACHE_FILTER_COUNT, IndicesFilterCache.this.count, count);
                IndicesFilterCache.this.count = count;
                replace = true;
            }
            if (replace) {
                final LRUFilterCache oldCache = cache;
                cache = buildFilterCache(size, count);
                oldCache.clear();
            }
        }
    }

    @Inject
    public IndicesFilterCache(Settings settings, NodeSettingsService nodeSettingsService) {
        super(settings);
        this.size = settings.get(INDICES_CACHE_FILTER_SIZE, "10%");
        this.count = settings.getAsInt(INDICES_CACHE_FILTER_COUNT, 100000);
        logger.debug("using [node] weighted filter cache with size [{}], actual_size [{}], max filter count [{}]",
                size, MemorySizeValue.parseBytesSizeValueOrHeapRatio(size), count);
        cache = buildFilterCache(size, count);
        nodeSettingsService.addListener(new ApplySettings());
        shards = new ShardMap();
        sharedRamBytesUsed = 0;
    }

    private LRUFilterCache buildFilterCache(String size, int count) {
        return new LRUFilterCache(count, MemorySizeValue.parseBytesSizeValueOrHeapRatio(size).bytes()) {
            // It's ok to not protect these callbacks by a lock since it is
            // done in LRUFilterCache
            @Override
            protected void onClear() {
                super.onClear();
                shards.clearStats();
                sharedRamBytesUsed = 0;
            }
            @Override
            protected void onFilterCache(Filter filter, long ramBytesUsed) {
                super.onFilterCache(filter, ramBytesUsed);
                sharedRamBytesUsed += ramBytesUsed;
            }
            @Override
            protected void onFilterEviction(Filter filter, long ramBytesUsed) {
                super.onFilterEviction(filter, ramBytesUsed);
                sharedRamBytesUsed -= ramBytesUsed;
            }
            @Override
            protected void onDocIdSetCache(Object readerCoreKey, long ramBytesUsed) {
                super.onDocIdSetCache(readerCoreKey, ramBytesUsed);
                final Stats shardStats = shards.getStats(readerCoreKey);
                if (shardStats != null) {
                    shardStats.cacheSize += 1;
                    shardStats.cacheCount += 1;
                    shardStats.ramBytesUsed += ramBytesUsed;
                }
            }
            @Override
            protected void onDocIdSetEviction(Object readerCoreKey, int numEntries, long sumRamBytesUsed) {
                super.onDocIdSetEviction(readerCoreKey, numEntries, sumRamBytesUsed);
                final Stats shardStats = shards.getStats(readerCoreKey);
                if (shardStats != null) {
                    shardStats.cacheSize -= numEntries;
                    shardStats.ramBytesUsed -= sumRamBytesUsed;
                }
            }
            @Override
            protected void onHit(Object readerCoreKey, Filter filter) {
                super.onHit(readerCoreKey, filter);
                final Stats shardStats = shards.getStats(readerCoreKey);
                if (shardStats != null) {
                    shardStats.hitCount += 1;
                }
            }
            @Override
            protected void onMiss(Object readerCoreKey, Filter filter) {
                super.onMiss(readerCoreKey, filter);
                final Stats shardStats = shards.getStats(readerCoreKey);
                if (shardStats != null) {
                    shardStats.missCount += 1;
                }
            }
        };
    }

    public FilterCacheStats getStats(ShardId shard) {
        final Map<ShardId, Stats> stats = shards.stats();
        final Stats shardStats = stats.get(shard);
        if (shardStats == null) {
            return new FilterCacheStats();
        }
        // there is some shard ram usage that we dispatch to shard depending
        // on the cache size versus the total size
        long totalSize = 0;
        for (Stats s : stats.values()) {
            totalSize += s.cacheSize;
        }
        final float weight = totalSize == 0 ? 1f / stats.size() : (float) shardStats.cacheSize / totalSize;
        final long ramBytesUsed = (long) (shardStats.ramBytesUsed + weight * sharedRamBytesUsed);
        return new FilterCacheStats(ramBytesUsed, shardStats.hitCount, shardStats.missCount, shardStats.cacheCount, shardStats.cacheSize);
    }

    @Override
    public Filter doCache(Filter filter, FilterCachingPolicy policy) {
        return new CachingWrapperFilter(filter, policy);
    }

    public void clearCoreCacheKeys(Collection<Object> coreCacheKeys) {
        for (Object coreKey : coreCacheKeys) {
            cache.clearCoreCacheKey(coreKey);
        }
        // This cache stores two things: filters, and doc id sets. Calling
        // clear only removes the doc id sets, but if we reach the situation
        // that the cache does not contain any DocIdSet anymore, then it
        // probably means that the user wanted to remove everything.
        if (cache.getCacheSize() == 0) {
            cache.clear();
        }
    }

    @Override
    public void close() {
        final LRUFilterCache cache = this.cache;
        if (cache != null) {
            cache.clear();
            this.cache = null;
        }
    }

    private class CachingWrapperFilter extends Filter {

        private final Filter uncached;
        private final FilterCachingPolicy policy;

        CachingWrapperFilter(Filter uncached, FilterCachingPolicy policy) {
            this.uncached = uncached;
            this.policy = policy;
        }

        @Override
        public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) throws IOException {
            shards.add(context.reader());

            // Because we support changing the cache size dynamically, the cache reference can
            // change over time. So we do not want cached filters to keep a hard reference on
            // the cache, or we might have a memory leak. This is why we have this wrapper which
            // polls the current cache on each segment.
            return cache.doCache(uncached, policy).getDocIdSet(context, acceptDocs);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof CachingWrapperFilter == false) {
                return false;
            }
            return uncached.equals(((CachingWrapperFilter) obj).uncached);
        }

        @Override
        public int hashCode() {
            return uncached.hashCode() ^ getClass().hashCode();
        }

        @Override
        public String toString() {
            return "cache(" + uncached + ")";
        }
    }

    private static class Stats implements Cloneable {

        volatile long ramBytesUsed;
        volatile long hitCount;
        volatile long missCount;
        volatile long cacheCount;
        volatile long cacheSize;

        void reset() {
            ramBytesUsed = 0;
            hitCount = 0;
            missCount = 0;
            cacheCount = 0;
            cacheSize = 0;
        }

        @Override
        public Stats clone() {
            Stats clone = new Stats();
            clone.ramBytesUsed = ramBytesUsed;
            clone.hitCount = hitCount;
            clone.missCount = missCount;
            clone.cacheCount = cacheCount;
            clone.cacheSize = cacheSize;
            return clone;
        }
    }

    // A map between shards and core keys
    // We keep two maps coreKey <-> shardId in sync in order to be able to reclaim
    // memory when all segments of the same shard are closed
    private static class ShardMap {

        private final Map<Object, ShardId> coreKeyToShard;
        private final Multimap<ShardId, Object> shardToCoreKey;
        private final Map<ShardId, Stats> shardStats;

        ShardMap() {
            coreKeyToShard = new IdentityHashMap<>();
            shardToCoreKey = HashMultimap.create();
            shardStats = new HashMap<>();
        }

        synchronized void add(LeafReader reader) {
            final Object coreKey = reader.getCoreCacheKey();
            if (coreKeyToShard.containsKey(coreKey)) {
                return;
            }
            final ShardId shardId = ShardUtils.extractShardId(reader);
            reader.addCoreClosedListener(new CoreClosedListener() {
                @Override
                public void onClose(Object ownerCoreCacheKey) throws IOException {
                    synchronized (ShardMap.this) {
                        final ShardId shard = coreKeyToShard.remove(ownerCoreCacheKey);
                        if (shard != null) {
                            shardToCoreKey.remove(shard, ownerCoreCacheKey);
                            shardStats.remove(shard);
                        }
                    }
                }
            });
            coreKeyToShard.put(coreKey, shardId);
            shardToCoreKey.put(shardId, coreKey);
            shardStats.put(shardId, new Stats());
        }

        synchronized Stats getStats(Object coreKey) {
            final ShardId shard = coreKeyToShard.get(coreKey);
            if (shard != null) {
                return shardStats.get(shard);
            }
            return null;
        }

        synchronized void clearStats() {
            for (Stats stats : shardStats.values()) {
                stats.reset();
            }
        }

        // Not an actual snapshot since we do not use the same lock here
        // and inside the cache, but good enough for what we want to do
        // with it...
        Map<ShardId, Stats> stats() {
            Map<ShardId, Stats> stats;
            synchronized (this) {
                stats = new HashMap<>(shardStats);
            }
            for (Map.Entry<ShardId, Stats> entry : stats.entrySet()) {
                entry.setValue(entry.getValue().clone());
            }
            return stats;
        }
    }
}