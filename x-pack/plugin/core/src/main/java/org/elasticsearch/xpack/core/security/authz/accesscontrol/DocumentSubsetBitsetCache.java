/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.accesscontrol;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.cache.RemovalNotification;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.lucene.util.BitSets;
import org.elasticsearch.lucene.util.MatchAllBitSet;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

/**
 * This is a cache for {@link BitSet} instances that are used with the {@link DocumentSubsetReader}.
 * It is bounded by memory size and access time.
 * <p>
 * DLS uses {@link BitSet} instances to track which documents should be visible to the user ("live") and which should not ("dead").
 * This means that there is a bit for each document in a Lucene index (ES shard).
 * Consequently, an index with 10 million document will use more than 1Mb of bitset memory for every unique DLS query, and an index
 * with 1 billion documents will use more than 100Mb of memory per DLS query.
 * Because DLS supports templating queries based on user metadata, there may be many distinct queries in use for each index, even if
 * there is only a single active role.
 * <p>
 * The primary benefit of the cache is to avoid recalculating the "live docs" (visible documents) when a user performs multiple
 * consecutive queries across one or more large indices. Given the memory examples above, the cache is only useful if it can hold at
 * least 1 large (100Mb or more ) {@code BitSet} during a user's active session, and ideally should be capable of support multiple
 * simultaneous users with distinct DLS queries.
 * <p>
 * For this reason the default memory usage (weight) for the cache set to 10% of JVM heap ({@link #CACHE_SIZE_SETTING}), so that it
 * automatically scales with the size of the Elasticsearch deployment, and can provide benefit to most use cases without needing
 * customisation. On a 32Gb heap, a 10% cache would be 3.2Gb which is large enough to store BitSets representing 25 billion docs.
 * <p>
 * However, because queries can be templated by user metadata and that metadata can change frequently, it is common for the
 * effective lifetime of a single DLS query to be relatively short. We do not want to sacrifice 10% of heap to a cache that is storing
 * BitSets that are no longer needed, so we set the TTL on this cache to be 2 hours ({@link #CACHE_TTL_SETTING}). This time has been
 * chosen so that it will retain BitSets that are in active use during a user's session, but not be an ongoing drain on memory.
 *
 * @see org.elasticsearch.index.cache.bitset.BitsetFilterCache
 */
public final class DocumentSubsetBitsetCache implements IndexReader.ClosedListener, Closeable, Accountable {

    private static final Logger logger = LogManager.getLogger(DocumentSubsetBitsetCache.class);

    /**
     * The TTL defaults to 2 hours. We default to a large cache size ({@link #CACHE_SIZE_SETTING}), and aggressively
     * expire unused entries so that the cache does not hold on to memory unnecessarily.
     */
    static final Setting<TimeValue> CACHE_TTL_SETTING = Setting.timeSetting(
        "xpack.security.dls.bitset.cache.ttl",
        TimeValue.timeValueHours(2),
        Property.NodeScope
    );

    /**
     * The size defaults to 10% of heap so that it automatically scales up with larger node size
     */
    static final Setting<ByteSizeValue> CACHE_SIZE_SETTING = Setting.memorySizeSetting(
        "xpack.security.dls.bitset.cache.size",
        "10%",
        Property.NodeScope
    );

    private static final BitSet NULL_MARKER = new FixedBitSet(0);

    private final long maxWeightBytes;
    private final Cache<BitsetCacheKey, BitSet> bitsetCache;
    private final Map<IndexReader.CacheKey, Set<BitsetCacheKey>> keysByIndex;
    private final AtomicLong cacheFullWarningTime;
    private final LongSupplier relativeNanoTimeProvider;
    private final LongAdder hitsTimeInNanos = new LongAdder();
    private final LongAdder missesTimeInNanos = new LongAdder();

    public DocumentSubsetBitsetCache(Settings settings) {
        this(settings, System::nanoTime);
    }

    /**
     * @param settings The global settings object for this node
     * @param relativeNanoTimeProvider Provider of nanos for code that needs to measure relative time.
     */
    // visible for testing
    DocumentSubsetBitsetCache(Settings settings, LongSupplier relativeNanoTimeProvider) {
        final TimeValue ttl = CACHE_TTL_SETTING.get(settings);
        this.maxWeightBytes = CACHE_SIZE_SETTING.get(settings).getBytes();
        this.bitsetCache = CacheBuilder.<BitsetCacheKey, BitSet>builder()
            .setExpireAfterAccess(ttl)
            .setMaximumWeight(maxWeightBytes)
            .weigher((key, bitSet) -> bitSet == NULL_MARKER ? 0 : bitSet.ramBytesUsed())
            .removalListener(this::onCacheEviction)
            .build();

        this.keysByIndex = new ConcurrentHashMap<>();
        this.cacheFullWarningTime = new AtomicLong(0);
        this.relativeNanoTimeProvider = Objects.requireNonNull(relativeNanoTimeProvider);
    }

    @Override
    public void onClose(IndexReader.CacheKey indexKey) {
        final Set<BitsetCacheKey> keys = keysByIndex.remove(indexKey);
        if (keys != null) {
            // Because this Set has been removed from the map, and the only update to the set is performed in a
            // Map#compute call, it should not be possible to get a concurrent modification here.
            keys.forEach(bitsetCache::invalidate);
        }
    }

    /**
     * Cleanup (synchronize) the internal state when an object is removed from the primary cache
     */
    private void onCacheEviction(RemovalNotification<BitsetCacheKey, BitSet> notification) {
        final BitsetCacheKey cacheKey = notification.getKey();
        final IndexReader.CacheKey indexKey = cacheKey.indexKey;
        // the key is *probably* no longer in the cache, so make sure it is no longer in the lookup map.
        // note: rather than locking (which destroys our throughput), we're erring on the side of tidying the keysByIndex
        // structure even if some other racing thread has already added a new bitset into the cache for this same key.
        // the keysByIndex structure is used in onClose (our notification from lucene that a segment has become inaccessible),
        // so we might end up failing to *eagerly* invalidate a bitset -- the consequence of that would be temporarily higher
        // memory use (the bitset will not be accessed, and it will still be invalidated eventually for size or ttl reasons).
        keysByIndex.computeIfPresent(indexKey, (ignored, keys) -> {
            keys.remove(cacheKey);
            return keys.isEmpty() ? null : keys;
        });
    }

    @Override
    public void close() {
        clear("close");
    }

    public void clear(String reason) {
        logger.debug("clearing all DLS bitsets because [{}]", reason);
        // Due to the order here, it is possible than a new entry could be added _after_ the keysByIndex map is cleared
        // but _before_ the cache is cleared. This should get fixed up in the "onCacheEviction" callback, but if anything slips through
        // and sits orphaned in keysByIndex, it will not be a significant issue.
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
     * Otherwise, a new BitSet will be created and stored in the cache.
     * The returned BitSet may be null (e.g. if the query has no results).
     */
    @Nullable
    public BitSet getBitSet(final Query query, final LeafReaderContext context) throws ExecutionException {
        final long cacheStart = relativeNanoTimeProvider.getAsLong();

        final IndexReader.CacheHelper coreCacheHelper = context.reader().getCoreCacheHelper();
        if (coreCacheHelper == null) {
            try {
                return computeBitSet(query, context);
            } catch (IOException e) {
                throw new ExecutionException(e);
            }
        }
        coreCacheHelper.addClosedListener(this);
        final IndexReader.CacheKey indexKey = coreCacheHelper.getKey();
        final BitsetCacheKey cacheKey = new BitsetCacheKey(indexKey, query);

        final boolean[] cacheKeyWasPresent = new boolean[] { true };
        final BitSet bitSet = bitsetCache.computeIfAbsent(cacheKey, ignore1 -> {
            cacheKeyWasPresent[0] = false;
            // This ensures all insertions into the set are guarded by ConcurrentHashMap's atomicity guarantees.
            keysByIndex.compute(indexKey, (ignore2, keys) -> {
                if (keys == null) {
                    keys = ConcurrentCollections.newConcurrentSet();
                }
                keys.add(cacheKey);
                return keys;
            });
            final BitSet result = computeBitSet(query, context);
            if (result == null) {
                // A cache loader is not allowed to return null, return a marker object instead.
                return NULL_MARKER;
            }
            final long bitSetBytes = result.ramBytesUsed();
            if (bitSetBytes > this.maxWeightBytes) {
                logger.warn(
                    "built a DLS BitSet that uses [{}] bytes; the DLS BitSet cache has a maximum size of [{}] bytes;"
                        + " this object cannot be cached and will need to be rebuilt for each use;"
                        + " consider increasing the value of [{}]",
                    bitSetBytes,
                    maxWeightBytes,
                    CACHE_SIZE_SETTING.getKey()
                );
            } else if (bitSetBytes + bitsetCache.weight() > maxWeightBytes) {
                maybeLogCacheFullWarning();
            }
            return result;
        });
        if (cacheKeyWasPresent[0]) {
            hitsTimeInNanos.add(relativeNanoTimeProvider.getAsLong() - cacheStart);
        } else {
            missesTimeInNanos.add(relativeNanoTimeProvider.getAsLong() - cacheStart);
        }
        if (bitSet == NULL_MARKER) {
            return null;
        } else {
            return bitSet;
        }
    }

    @Nullable
    private static BitSet computeBitSet(Query query, LeafReaderContext context) throws IOException {
        final IndexReaderContext topLevelContext = ReaderUtil.getTopLevelContext(context);
        final IndexSearcher searcher = new IndexSearcher(topLevelContext);
        searcher.setQueryCache(null);
        final Query rewrittenQuery = searcher.rewrite(query);
        if (isEffectiveMatchAllDocsQuery(rewrittenQuery)) {
            return new MatchAllBitSet(context.reader().maxDoc());
        }
        final Weight weight = searcher.createWeight(rewrittenQuery, ScoreMode.COMPLETE_NO_SCORES, 1f);
        final Scorer s = weight.scorer(context);
        if (s == null) {
            return null;
        } else {
            return BitSets.of(s.iterator(), context.reader().maxDoc());
        }
    }

    // Package private for testing
    static boolean isEffectiveMatchAllDocsQuery(Query rewrittenQuery) {
        if (rewrittenQuery instanceof ConstantScoreQuery csq && csq.getQuery() instanceof MatchAllDocsQuery) {
            return true;
        }
        if (rewrittenQuery instanceof MatchAllDocsQuery) {
            return true;
        }
        return false;
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
                CACHE_SIZE_SETTING.getKey()
            );
        }
    }

    public static List<Setting<?>> getSettings() {
        return List.of(CACHE_TTL_SETTING, CACHE_SIZE_SETTING);
    }

    public Map<String, Object> usageStats() {
        final ByteSizeValue ram = ByteSizeValue.ofBytes(ramBytesUsed());
        final Cache.Stats cacheStats = bitsetCache.stats();

        final Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("count", entryCount());
        stats.put("memory", ram.toString());
        stats.put("memory_in_bytes", ram.getBytes());
        stats.put("hits", cacheStats.getHits());
        stats.put("misses", cacheStats.getMisses());
        stats.put("evictions", cacheStats.getEvictions());
        stats.put("hits_time_in_millis", TimeValue.nsecToMSec(hitsTimeInNanos.sum()));
        stats.put("misses_time_in_millis", TimeValue.nsecToMSec(missesTimeInNanos.sum()));
        return stats;
    }

    private static final class BitsetCacheKey {

        final IndexReader.CacheKey indexKey;
        final Query query;
        final int hashCode;

        private BitsetCacheKey(IndexReader.CacheKey indexKey, Query query) {
            this.indexKey = indexKey;
            this.query = query;
            // compute the hashCode eagerly, since it's used multiple times in the cache implementation anyway -- the query here will
            // be a ConstantScoreQuery around a BooleanQuery, and BooleanQuery already *lazily* caches the hashCode, so this isn't
            // altogether that much faster in reality, but it makes it more explicit here that we're doing this
            this.hashCode = computeHashCode();
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
            return Objects.equals(this.indexKey, that.indexKey) && Objects.equals(this.query, that.query);
        }

        private int computeHashCode() {
            int result = indexKey.hashCode();
            result = 31 * result + query.hashCode();
            return result;
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" + indexKey + "," + query + ")";
        }
    }

    /**
     * This test-only method verifies that the two internal data structures ({@link #bitsetCache} and {@link #keysByIndex}) are consistent
     * with one another.
     */
    // visible for testing
    void verifyInternalConsistency() {
        verifyInternalConsistencyCacheToKeys();
        verifyInternalConsistencyKeysToCache();
    }

    /**
     * This test-only method iterates over the {@link #bitsetCache} and checks that {@link #keysByIndex} is consistent with it.
     */
    // visible for testing
    void verifyInternalConsistencyCacheToKeys() {
        bitsetCache.keys().forEach(cacheKey -> {
            final Set<BitsetCacheKey> keys = keysByIndex.get(cacheKey.indexKey);
            if (keys == null || keys.contains(cacheKey) == false) {
                throw new IllegalStateException(
                    "Key [" + cacheKey + "] is in the cache, but the lookup entry for [" + cacheKey.indexKey + "] does not contain that key"
                );
            }
        });
    }

    /**
     * This test-only method iterates over the {@link #keysByIndex} and checks that {@link #bitsetCache} is consistent with it.
     */
    // visible for testing
    void verifyInternalConsistencyKeysToCache() {
        keysByIndex.forEach((indexKey, keys) -> {
            if (keys == null || keys.isEmpty()) {
                throw new IllegalStateException("The lookup entry for [" + indexKey + "] is null or empty");
            } else {
                keys.forEach(cacheKey -> {
                    if (bitsetCache.get(cacheKey) == null) {
                        throw new IllegalStateException("Key [" + cacheKey + "] is in the lookup map, but is not in the cache");
                    }
                });
            }
        });
    }

}
