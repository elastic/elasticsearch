/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.LRUQueryCache;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.lucene.ShardCoreKeyMap;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.cache.query.QueryCacheStats;
import org.elasticsearch.index.shard.ShardId;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

public class IndicesQueryCache implements QueryCache, Closeable {

    private static final Logger logger = LogManager.getLogger(IndicesQueryCache.class);

    public static final Setting<ByteSizeValue> INDICES_CACHE_QUERY_SIZE_SETTING = Setting.memorySizeSetting(
        "indices.queries.cache.size",
        "10%",
        Property.NodeScope
    );
    // mostly a way to prevent queries from being the main source of memory usage
    // of the cache
    public static final Setting<Integer> INDICES_CACHE_QUERY_COUNT_SETTING = Setting.intSetting(
        "indices.queries.cache.count",
        10_000,
        1,
        Property.NodeScope
    );
    // enables caching on all segments instead of only the larger ones, for testing only
    public static final Setting<Boolean> INDICES_QUERIES_CACHE_ALL_SEGMENTS_SETTING = Setting.boolSetting(
        "indices.queries.cache.all_segments",
        false,
        Property.NodeScope
    );

    private final LRUQueryCache cache;
    private final ShardCoreKeyMap shardKeyMap = new ShardCoreKeyMap();
    private final Map<ShardId, Stats> shardStats = new ConcurrentHashMap<>();
    private volatile long sharedRamBytesUsed;

    // This is a hack for the fact that the close listener for the
    // ShardCoreKeyMap will be called before onDocIdSetEviction
    // See onDocIdSetEviction for more info
    private final Map<Object, StatsAndCount> stats2 = Collections.synchronizedMap(new IdentityHashMap<>());

    public IndicesQueryCache(Settings settings) {
        final ByteSizeValue size = INDICES_CACHE_QUERY_SIZE_SETTING.get(settings);
        final int count = INDICES_CACHE_QUERY_COUNT_SETTING.get(settings);
        logger.debug("using [node] query cache with size [{}] max filter count [{}]", size, count);
        if (INDICES_QUERIES_CACHE_ALL_SEGMENTS_SETTING.get(settings)) {
            // Use the default skip_caching_factor (i.e., 10f) in Lucene
            cache = new ElasticsearchLRUQueryCache(count, size.getBytes(), context -> true, 10f);
        } else {
            cache = new ElasticsearchLRUQueryCache(count, size.getBytes());
        }
        sharedRamBytesUsed = 0;
    }

    /** Get usage statistics for the given shard. */
    public QueryCacheStats getStats(ShardId shard) {
        final Map<ShardId, QueryCacheStats> stats = new HashMap<>();
        for (Map.Entry<ShardId, Stats> entry : shardStats.entrySet()) {
            stats.put(entry.getKey(), entry.getValue().toQueryCacheStats());
        }
        QueryCacheStats shardStats = new QueryCacheStats();
        QueryCacheStats info = stats.get(shard);
        if (info == null) {
            info = new QueryCacheStats();
        }
        shardStats.add(info);

        // We also have some shared ram usage that we try to distribute
        // proportionally to their number of cache entries of each shard.
        // Sometimes it's not possible to do this when there are no shard entries at all,
        // which can happen as the shared ram usage can extend beyond the closing of all shards.
        if (stats.isEmpty() == false) {
            long totalSize = 0;
            for (QueryCacheStats s : stats.values()) {
                totalSize += s.getCacheSize();
            }
            final double weight = totalSize == 0 ? 1d / stats.size() : ((double) shardStats.getCacheSize()) / totalSize;
            final long additionalRamBytesUsed = Math.round(weight * sharedRamBytesUsed);
            assert additionalRamBytesUsed >= 0L : additionalRamBytesUsed;
            shardStats.add(new QueryCacheStats(additionalRamBytesUsed, 0, 0, 0, 0));
        }
        return shardStats;
    }

    @Override
    public Weight doCache(Weight weight, QueryCachingPolicy policy) {
        while (weight instanceof CachingWeightWrapper) {
            weight = ((CachingWeightWrapper) weight).in;
        }
        final Weight in = cache.doCache(weight, policy);
        // We wrap the weight to track the readers it sees and map them with
        // the shards they belong to
        return new CachingWeightWrapper(in);
    }

    private class CachingWeightWrapper extends Weight {

        private final Weight in;

        protected CachingWeightWrapper(Weight in) {
            super(in.getQuery());
            this.in = in;
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            shardKeyMap.add(context.reader());
            return in.explain(context, doc);
        }

        @Override
        public int count(LeafReaderContext context) throws IOException {
            shardKeyMap.add(context.reader());
            return in.count(context);
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
            shardKeyMap.add(context.reader());
            return in.scorer(context);
        }

        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
            shardKeyMap.add(context.reader());
            return in.scorerSupplier(context);
        }

        @Override
        public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
            shardKeyMap.add(context.reader());
            return in.bulkScorer(context);
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            return in.isCacheable(ctx);
        }
    }

    /** Clear all entries that belong to the given index. */
    public void clearIndex(String index) {
        final Set<Object> coreCacheKeys = shardKeyMap.getCoreKeysForIndex(index);
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
        assert shardKeyMap.size() == 0 : shardKeyMap.size();
        assert shardStats.isEmpty() : shardStats.keySet();
        assert stats2.isEmpty() : stats2;

        // This cache stores two things: filters, and doc id sets. At this time
        // we only know that there are no more doc id sets, but we still track
        // recently used queries, which we want to reclaim.
        cache.clear();
    }

    private static class Stats implements Cloneable {

        final ShardId shardId;
        volatile long ramBytesUsed;
        volatile long hitCount;
        volatile long missCount;
        volatile long cacheCount;
        volatile long cacheSize;

        Stats(ShardId shardId) {
            this.shardId = shardId;
        }

        QueryCacheStats toQueryCacheStats() {
            return new QueryCacheStats(ramBytesUsed, hitCount, missCount, cacheCount, cacheSize);
        }

        @Override
        public String toString() {
            return "{shardId="
                + shardId
                + ", ramBytedUsed="
                + ramBytesUsed
                + ", hitCount="
                + hitCount
                + ", missCount="
                + missCount
                + ", cacheCount="
                + cacheCount
                + ", cacheSize="
                + cacheSize
                + "}";
        }
    }

    private static class StatsAndCount {
        volatile int count;
        final Stats stats;

        StatsAndCount(Stats stats) {
            this.stats = stats;
            this.count = 0;
        }

        @Override
        public String toString() {
            return "{stats=" + stats + " ,count=" + count + "}";
        }
    }

    private static boolean empty(Stats stats) {
        if (stats == null) {
            return true;
        }
        return stats.cacheSize == 0 && stats.ramBytesUsed == 0;
    }

    public void onClose(ShardId shardId) {
        assert empty(shardStats.get(shardId));
        shardStats.remove(shardId);
    }

    private class ElasticsearchLRUQueryCache extends LRUQueryCache {

        ElasticsearchLRUQueryCache(int maxSize, long maxRamBytesUsed, Predicate<LeafReaderContext> leavesToCache, float skipFactor) {
            super(maxSize, maxRamBytesUsed, leavesToCache, skipFactor);
        }

        ElasticsearchLRUQueryCache(int maxSize, long maxRamBytesUsed) {
            super(maxSize, maxRamBytesUsed);
        }

        private Stats getStats(Object coreKey) {
            final ShardId shardId = shardKeyMap.getShardId(coreKey);
            if (shardId == null) {
                return null;
            }
            return shardStats.get(shardId);
        }

        private Stats getOrCreateStats(Object coreKey) {
            return shardStats.computeIfAbsent(shardKeyMap.getShardId(coreKey), Stats::new);
        }

        // It's ok to not protect these callbacks by a lock since it is
        // done in LRUQueryCache
        @Override
        protected void onClear() {
            super.onClear();
            for (Stats stats : shardStats.values()) {
                // don't throw away hit/miss
                stats.cacheSize = 0;
                stats.ramBytesUsed = 0;
            }
            stats2.clear();
            sharedRamBytesUsed = 0;
        }

        @Override
        protected void onQueryCache(Query filter, long ramBytesUsed) {
            super.onQueryCache(filter, ramBytesUsed);
            sharedRamBytesUsed += ramBytesUsed;
        }

        @Override
        protected void onQueryEviction(Query filter, long ramBytesUsed) {
            super.onQueryEviction(filter, ramBytesUsed);
            sharedRamBytesUsed -= ramBytesUsed;
        }

        @Override
        protected void onDocIdSetCache(Object readerCoreKey, long ramBytesUsed) {
            super.onDocIdSetCache(readerCoreKey, ramBytesUsed);
            final Stats shardStats = getOrCreateStats(readerCoreKey);
            shardStats.cacheSize += 1;
            shardStats.cacheCount += 1;
            shardStats.ramBytesUsed += ramBytesUsed;

            StatsAndCount statsAndCount = stats2.get(readerCoreKey);
            if (statsAndCount == null) {
                statsAndCount = new StatsAndCount(shardStats);
                stats2.put(readerCoreKey, statsAndCount);
            }
            statsAndCount.count += 1;
        }

        @Override
        protected void onDocIdSetEviction(Object readerCoreKey, int numEntries, long sumRamBytesUsed) {
            super.onDocIdSetEviction(readerCoreKey, numEntries, sumRamBytesUsed);
            // onDocIdSetEviction might sometimes be called with a number
            // of entries equal to zero if the cache for the given segment
            // was already empty when the close listener was called
            if (numEntries > 0) {
                // We can't use ShardCoreKeyMap here because its core closed
                // listener is called before the listener of the cache which
                // triggers this eviction. So instead we use use stats2 that
                // we only evict when nothing is cached anymore on the segment
                // instead of relying on close listeners
                final StatsAndCount statsAndCount = stats2.get(readerCoreKey);
                final Stats shardStats = statsAndCount.stats;
                shardStats.cacheSize -= numEntries;
                shardStats.ramBytesUsed -= sumRamBytesUsed;
                statsAndCount.count -= numEntries;
                if (statsAndCount.count == 0) {
                    stats2.remove(readerCoreKey);
                }
            }
        }

        @Override
        protected void onHit(Object readerCoreKey, Query filter) {
            super.onHit(readerCoreKey, filter);
            final Stats shardStats = getStats(readerCoreKey);
            shardStats.hitCount += 1;
        }

        @Override
        protected void onMiss(Object readerCoreKey, Query filter) {
            super.onMiss(readerCoreKey, filter);
            final Stats shardStats = getOrCreateStats(readerCoreKey);
            shardStats.missCount += 1;
        }
    }
}
