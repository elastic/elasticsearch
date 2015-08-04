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

package org.elasticsearch.indices.cache.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.LRUQueryCache;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.ShardCoreKeyMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.MemorySizeValue;
import org.elasticsearch.index.cache.query.QueryCacheStats;
import org.elasticsearch.index.shard.ShardId;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class IndicesQueryCache extends AbstractComponent implements QueryCache, Closeable {

    public static final String INDICES_CACHE_QUERY_SIZE = "indices.queries.cache.size";
    @Deprecated
    public static final String DEPRECATED_INDICES_CACHE_QUERY_SIZE = "indices.cache.filter.size";
    public static final String INDICES_CACHE_QUERY_COUNT = "indices.queries.cache.count";

    private final LRUQueryCache cache;
    private final ShardCoreKeyMap shardKeyMap = new ShardCoreKeyMap();
    private final Map<ShardId, Stats> shardStats = new ConcurrentHashMap<>();
    private volatile long sharedRamBytesUsed;

    // This is a hack for the fact that the close listener for the
    // ShardCoreKeyMap will be called before onDocIdSetEviction
    // See onDocIdSetEviction for more info
    private final Map<Object, StatsAndCount> stats2 = new IdentityHashMap<>();

    @Inject
    public IndicesQueryCache(Settings settings) {
        super(settings);
        String sizeString = settings.get(INDICES_CACHE_QUERY_SIZE);
        if (sizeString == null) {
            sizeString = settings.get(DEPRECATED_INDICES_CACHE_QUERY_SIZE);
            if (sizeString != null) {
                deprecationLogger.deprecated("The [" + DEPRECATED_INDICES_CACHE_QUERY_SIZE
                        + "] settings is now deprecated, use [" + INDICES_CACHE_QUERY_SIZE + "] instead");
            }
        }
        if (sizeString == null) {
            sizeString = "10%";
        }
        final ByteSizeValue size = MemorySizeValue.parseBytesSizeValueOrHeapRatio(sizeString, INDICES_CACHE_QUERY_SIZE);
        final int count = settings.getAsInt(INDICES_CACHE_QUERY_COUNT, 1000);
        logger.debug("using [node] query cache with size [{}], actual_size [{}], max filter count [{}]",
                sizeString, size, count);
        cache = new LRUQueryCache(count, size.bytes()) {

            private Stats getStats(Object coreKey) {
                final ShardId shardId = shardKeyMap.getShardId(coreKey);
                if (shardId == null) {
                    return null;
                }
                return shardStats.get(shardId);
            }

            private Stats getOrCreateStats(Object coreKey) {
                final ShardId shardId = shardKeyMap.getShardId(coreKey);
                Stats stats = shardStats.get(shardId);
                if (stats == null) {
                    stats = new Stats();
                    shardStats.put(shardId, stats);
                }
                return stats;
            }

            // It's ok to not protect these callbacks by a lock since it is
            // done in LRUQueryCache
            @Override
            protected void onClear() {
                assert Thread.holdsLock(this);
                super.onClear();
                for (Stats stats : shardStats.values()) {
                    // don't throw away hit/miss
                    stats.cacheSize = 0;
                    stats.ramBytesUsed = 0;
                }
                sharedRamBytesUsed = 0;
            }

            @Override
            protected void onQueryCache(Query filter, long ramBytesUsed) {
                assert Thread.holdsLock(this);
                super.onQueryCache(filter, ramBytesUsed);
                sharedRamBytesUsed += ramBytesUsed;
            }

            @Override
            protected void onQueryEviction(Query filter, long ramBytesUsed) {
                assert Thread.holdsLock(this);
                super.onQueryEviction(filter, ramBytesUsed);
                sharedRamBytesUsed -= ramBytesUsed;
            }

            @Override
            protected void onDocIdSetCache(Object readerCoreKey, long ramBytesUsed) {
                assert Thread.holdsLock(this);
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
                assert Thread.holdsLock(this);
                super.onDocIdSetEviction(readerCoreKey, numEntries, sumRamBytesUsed);
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

            @Override
            protected void onHit(Object readerCoreKey, Query filter) {
                assert Thread.holdsLock(this);
                super.onHit(readerCoreKey, filter);
                final Stats shardStats = getStats(readerCoreKey);
                shardStats.hitCount += 1;
            }

            @Override
            protected void onMiss(Object readerCoreKey, Query filter) {
                assert Thread.holdsLock(this);
                super.onMiss(readerCoreKey, filter);
                final Stats shardStats = getOrCreateStats(readerCoreKey);
                shardStats.missCount += 1;
            }
        };
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

        // We also have some shared ram usage that we try to distribute to
        // proportionally to their number of cache entries of each shard
        long totalSize = 0;
        for (QueryCacheStats s : stats.values()) {
            totalSize += s.getCacheSize();
        }
        final double weight = totalSize == 0
                ? 1d / stats.size()
                : shardStats.getCacheSize() / totalSize;
        final long additionalRamBytesUsed = Math.round(weight * sharedRamBytesUsed);
        shardStats.add(new QueryCacheStats(additionalRamBytesUsed, 0, 0, 0, 0));
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
        public void extractTerms(Set<Term> terms) {
            in.extractTerms(terms);
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            shardKeyMap.add(context.reader());
            return in.explain(context, doc);
        }

        @Override
        public float getValueForNormalization() throws IOException {
            return in.getValueForNormalization();
        }

        @Override
        public void normalize(float norm, float topLevelBoost) {
            in.normalize(norm, topLevelBoost);
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
            shardKeyMap.add(context.reader());
            return in.scorer(context);
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
        cache.clear();
    }

    private static class Stats implements Cloneable {

        volatile long ramBytesUsed;
        volatile long hitCount;
        volatile long missCount;
        volatile long cacheCount;
        volatile long cacheSize;

        QueryCacheStats toQueryCacheStats() {
            return new QueryCacheStats(ramBytesUsed, hitCount, missCount, cacheCount, cacheSize);
        }
    }

    private static class StatsAndCount {
        int count;
        final Stats stats;

        StatsAndCount(Stats stats) {
            this.stats = stats;
            this.count = 0;
        }
    }

    private boolean empty(Stats stats) {
        if (stats == null) {
            return true;
        }
        return stats.cacheSize == 0 && stats.ramBytesUsed == 0;
    }

    public void onClose(ShardId shardId) {
        assert empty(shardStats.get(shardId));
        shardStats.remove(shardId);
    }
}
