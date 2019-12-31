/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.accesscontrol;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.set.Sets;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This is a cache for {@link BitSet} instances that are used with the {@link DocumentSubsetReader}.
 * It is bounded by memory size and access time.
 *
 * DLS uses {@link BitSet} instances to track which documents should be visible to the user ("live") and which should not ("dead").
 * This means that there is a bit for each document in a Lucene index (ES shard).
 * Consequently, an index with 10 million document will use more than 1Mb of bitset memory for every unique DLS query, and an index
 * with 1 billion documents will use more than 100Mb of memory per DLS query.
 * Because DLS supports templating queries based on user metadata, there may be many distinct queries in use for each index, even if
 * there is only a single active role.
 *
 * The primary benefit of the cache is to avoid recalculating the "live docs" (visible documents) when a user performs multiple
 * consecutive queries across one or more large indices. Given the memory examples above, the cache is only useful if it can hold at
 * least 1 large (>100Mb) {@code BitSet} during a user's active session, and ideally should be capable of support multiple simultaneous
 * users with distinct DLS queries.
 *
 * For this reason the default memory usage (weight) for the cache set to 10% of JVM heap ({@link #CACHE_SIZE_SETTING}), so that it
 * automatically scales with the size of the Elasticsearch deployment, and can provide benefit to most use cases without needing
 * customisation. On a 32Gb heap, a 10% cache would be 3.2Gb which is large enough to store BitSets representing 25 billion docs.
 *
 * However, because queries can be templated by user metadata and that metadata can change frequently, it is common for the
 * effetively lifetime of a single DLS query to be relatively short. We do not want to sacrifice 10% of heap to a cache that is storing
 * BitSets that are not longer needed, so we set the TTL on this cache to be 2 hours ({@link #CACHE_TTL_SETTING}). This time has been
 * chosen so that it will retain BitSets that are in active use during a user's session, but not be an ongoing drain on memory.
 *
 * @see org.elasticsearch.index.cache.bitset.BitsetFilterCache
 */
public final class DocumentSubsetBitsetCache implements IndexReader.ClosedListener, Closeable, Accountable {

    /**
     * The TTL defaults to 2 hours. We default to a large cache size ({@link #CACHE_SIZE_SETTING}), and aggressively
     * expire unused entries so that the cache does not hold on to memory unnecessarily.
     */
    static final Setting<TimeValue> CACHE_TTL_SETTING =
        Setting.timeSetting("xpack.security.dls.bitset.cache.ttl", TimeValue.timeValueHours(2), Property.NodeScope);

    /**
     * The size defaults to 10% of heap so that it automatically scales up with larger node size
     */
    static final Setting<ByteSizeValue> CACHE_SIZE_SETTING = Setting.memorySizeSetting("xpack.security.dls.bitset.cache.size",
            "10%", Property.NodeScope);

    private static final BitSet NULL_MARKER = new FixedBitSet(0);

    private final Logger logger;
    private final long maxWeightBytes;
    private final Cache<BitsetCacheKey, BitSet> bitsetCache;
    private final Map<IndexReader.CacheKey, Set<BitsetCacheKey>> keysByIndex;
    private final AtomicLong cacheFullWarningTime;

    public DocumentSubsetBitsetCache(Settings settings) {
        this.logger = LogManager.getLogger(getClass());
        final TimeValue ttl = CACHE_TTL_SETTING.get(settings);
        this.maxWeightBytes = CACHE_SIZE_SETTING.get(settings).getBytes();
        this.bitsetCache = CacheBuilder.<BitsetCacheKey, BitSet>builder()
            .setExpireAfterAccess(ttl)
            .setMaximumWeight(maxWeightBytes)
            .weigher((key, bitSet) -> bitSet == NULL_MARKER ? 0 : bitSet.ramBytesUsed()).build();
        this.keysByIndex = new ConcurrentHashMap<>();
        this.cacheFullWarningTime = new AtomicLong(0);
    }

    @Override
    public void onClose(IndexReader.CacheKey ownerCoreCacheKey) {
        final Set<BitsetCacheKey> keys = keysByIndex.remove(ownerCoreCacheKey);
        if (keys != null) {
            // Because this Set has been removed from the map, and the only update to the set is performed in a
            // Map#compute call, it should not be possible to get a concurrent modification here.
            keys.forEach(bitsetCache::invalidate);
        }
    }

    @Override
    public void close() {
        clear("close");
    }

    public void clear(String reason) {
        logger.debug("clearing all DLS bitsets because [{}]", reason);
        // Due to the order here, it is possible than a new entry could be added _after_ the keysByIndex map is cleared
        // but _before_ the cache is cleared. This would mean it sits orphaned in keysByIndex, but this is not a issue.
        // When the index is closed, the key will be removed from the map, and there will not be a corresponding item
        // in the cache, which will make the cache-invalidate a no-op.
        // Since the entry is not in the cache, if #getBitSet is called, it will be loaded, and the new key will be added
        // to the index without issue.
        keysByIndex.clear();
        bitsetCache.invalidateAll();
    }

    int entryCount() {
        return this.bitsetCache.count();
    }

    @Override
    public long ramBytesUsed() {
        return this.bitsetCache.weight();
    }

    /**
     * Obtain the {@link BitSet} for the given {@code query} in the given {@code context}.
     * If there is a cached entry for that query and context, it will be returned.
     * Otherwise a new BitSet will be created and stored in the cache.
     * The returned BitSet may be null (e.g. if the query has no results).
     */
    @Nullable
    public BitSet getBitSet(final Query query, final LeafReaderContext context) throws ExecutionException {
        final IndexReader.CacheHelper coreCacheHelper = context.reader().getCoreCacheHelper();
        if (coreCacheHelper == null) {
            throw new IllegalArgumentException("Reader " + context.reader() + " does not support caching");
        }
        coreCacheHelper.addClosedListener(this);
        final IndexReader.CacheKey indexKey = coreCacheHelper.getKey();
        final BitsetCacheKey cacheKey = new BitsetCacheKey(indexKey, query);

        final BitSet bitSet = bitsetCache.computeIfAbsent(cacheKey, ignore1 -> {
            // This ensures all insertions into the set are guarded by ConcurrentHashMap's atomicity guarantees.
            keysByIndex.compute(indexKey, (ignore2, set) -> {
                if (set == null) {
                    set = Sets.newConcurrentHashSet();
                }
                set.add(cacheKey);
                return set;
            });
            final IndexReaderContext topLevelContext = ReaderUtil.getTopLevelContext(context);
            final IndexSearcher searcher = new IndexSearcher(topLevelContext);
            searcher.setQueryCache(null);
            final Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
            Scorer s = weight.scorer(context);
            if (s == null) {
                // A cache loader is not allowed to return null, return a marker object instead.
                return NULL_MARKER;
            } else {
                final BitSet bs = BitSet.of(s.iterator(), context.reader().maxDoc());
                final long bitSetBytes = bs.ramBytesUsed();
                if (bitSetBytes > this.maxWeightBytes) {
                    logger.warn("built a DLS BitSet that uses [{}] bytes; the BitSet cache has a maximum size of [{}] bytes;" +
                            " this object cannot be cached and will need to be rebuilt for each use; consider increasing the value of [{}]",
                        bitSetBytes, maxWeightBytes, CACHE_SIZE_SETTING.getKey());
                } else if (bitSetBytes + bitsetCache.weight() > maxWeightBytes) {
                    maybeLogCacheFullWarning();
                }
                return bs;
            }
        });
        if (bitSet == NULL_MARKER) {
            return null;
        } else {
            return bitSet;
        }
    }

    private void maybeLogCacheFullWarning() {
        final long nextLogTime = cacheFullWarningTime.get();
        final long now = System.currentTimeMillis();
        if (nextLogTime > now) {
            return;
        }
        final long nextCheck = now + TimeUnit.MINUTES.toMillis(30);
        if (cacheFullWarningTime.compareAndSet(nextLogTime, nextCheck)) {
            logger.info(
                "the Document Level Security BitSet cache is full which may impact performance; consider increasing the value of [{}]",
                CACHE_SIZE_SETTING.getKey());
        }
    }

    public static List<Setting<?>> getSettings() {
        return List.of(CACHE_TTL_SETTING, CACHE_SIZE_SETTING);
    }

    public Map<String, Object> usageStats() {
        final ByteSizeValue ram = new ByteSizeValue(ramBytesUsed(), ByteSizeUnit.BYTES);
        return Map.of(
            "count", entryCount(),
            "memory", ram.toString(),
            "memory_in_bytes", ram.getBytes()
        );
    }

    private class BitsetCacheKey {
        final IndexReader.CacheKey index;
        final Query query;

        private BitsetCacheKey(IndexReader.CacheKey index, Query query) {
            this.index = index;
            this.query = query;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            final BitsetCacheKey that = (BitsetCacheKey) other;
            return Objects.equals(this.index, that.index) &&
                Objects.equals(this.query, that.query);
        }

        @Override
        public int hashCode() {
            return Objects.hash(index, query);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" + index + "," + query + ")";
        }
    }
}
