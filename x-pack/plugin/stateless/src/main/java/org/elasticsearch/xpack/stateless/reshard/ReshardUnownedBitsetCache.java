/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.reshard;

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
 * Bounded cache for “unowned document” {@link BitSet}s built during resharding search filtering.
 * Keys segment cores via {@link org.apache.lucene.index.LeafReader#getCoreCacheHelper()} and the
 * {@link org.elasticsearch.index.shard.ShardSplittingQuery} so repeated reader wraps reuse work.
 */
public final class ReshardUnownedBitsetCache implements IndexReader.ClosedListener, Closeable, Accountable {

    private static final Logger logger = LogManager.getLogger(ReshardUnownedBitsetCache.class);

    //// Default time to live for this cache is: The grace period for deleting unowned documents
    /// ({@link SplitSourceService#RESHARD_SPLIT_DELETE_UNOWNED_GRACE_PERIOD}) plus 30 sec.
    public static final Setting<TimeValue> CACHE_TTL_SETTING = Setting.timeSetting(
        "stateless.reshard.unowned_bitset.cache.ttl",
        TimeValue.timeValueSeconds(330),
        Property.NodeScope
    );

    public static final Setting<ByteSizeValue> CACHE_SIZE_SETTING = Setting.memorySizeSetting(
        "stateless.reshard.unowned_bitset.cache.size",
        "1%",
        Property.NodeScope
    );

    private static final BitSet NULL_MARKER = new FixedBitSet(0);

    private final long maxWeightBytes;
    private final Cache<BitsetCacheKey, BitSet> bitsetCache;
    private final Map<IndexReader.CacheKey, Set<BitsetCacheKey>> keysByIndexReader;
    private final AtomicLong cacheFullWarningTime;
    private final LongSupplier relativeNanoTimeProvider;
    private final LongAdder hitsTimeInNanos = new LongAdder();
    private final LongAdder missesTimeInNanos = new LongAdder();

    public ReshardUnownedBitsetCache(Settings settings) {
        this(settings, System::nanoTime);
    }

    // visible for testing
    ReshardUnownedBitsetCache(Settings settings, LongSupplier relativeNanoTimeProvider) {
        final TimeValue ttl = CACHE_TTL_SETTING.get(settings);
        this.maxWeightBytes = CACHE_SIZE_SETTING.get(settings).getBytes();
        this.bitsetCache = CacheBuilder.<BitsetCacheKey, BitSet>builder()
            .setExpireAfterAccess(ttl)
            .setMaximumWeight(maxWeightBytes)
            .weigher((key, bitSet) -> bitSet == NULL_MARKER ? 0 : bitSet.ramBytesUsed())
            .removalListener(this::onCacheEviction)
            .build();

        this.keysByIndexReader = new ConcurrentHashMap<>();
        this.cacheFullWarningTime = new AtomicLong(0);
        this.relativeNanoTimeProvider = Objects.requireNonNull(relativeNanoTimeProvider);
    }

    @Override
    public void onClose(IndexReader.CacheKey indexKey) {
        final Set<BitsetCacheKey> keys = keysByIndexReader.remove(indexKey);
        if (keys != null) {
            keys.forEach(bitsetCache::invalidate);
        }
    }

    private void onCacheEviction(RemovalNotification<BitsetCacheKey, BitSet> notification) {
        final BitsetCacheKey cacheKey = notification.getKey();
        final IndexReader.CacheKey indexKey = cacheKey.indexKey;
        keysByIndexReader.computeIfPresent(indexKey, (ignored, keys) -> {
            keys.remove(cacheKey);
            return keys.isEmpty() ? null : keys;
        });
    }

    @Override
    public void close() {
        clear("close");
    }

    public void clear(String reason) {
        logger.debug("clearing all reshard unowned bitsets because [{}]", reason);
        keysByIndexReader.clear();
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
     * Returns the bitset of documents matching the query (unowned docs), the bitset is empty if there are no
     * unowned docs.
     * The returned bitset can be null, however it is an unlikely scenario for a ShardSplittingQuery.
     * It can happen for a nested index with no parent docs on leaf.
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
            keysByIndexReader.compute(indexKey, (ignore2, keys) -> {
                if (keys == null) {
                    keys = ConcurrentCollections.newConcurrentSet();
                }
                keys.add(cacheKey);
                return keys;
            });
            try {
                final BitSet result = computeBitSet(query, context);
                if (result == null) {
                    return NULL_MARKER;
                }
                final long bitSetBytes = result.ramBytesUsed();
                if (bitSetBytes > this.maxWeightBytes) {
                    logger.warn(
                        "built a resharding unowned-doc BitSet that uses [{}] bytes; the cache maximum size is [{}] bytes;"
                            + " consider increasing [{}]",
                        bitSetBytes,
                        maxWeightBytes,
                        CACHE_SIZE_SETTING.getKey()
                    );
                } else if (bitSetBytes + bitsetCache.weight() > maxWeightBytes) {
                    maybeLogCacheFullWarning();
                }
                return result;
            } catch (Exception e) {
                keysByIndexReader.computeIfPresent(indexKey, (ignore2, keys) -> {
                    keys.remove(cacheKey);
                    return keys.isEmpty() ? null : keys;
                });
                throw e;
            }
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

    /**
     * Bitset of documents matching {@code query} on the given leaf (resharding: docs not owned by the shard).
     * Shared by {@link ReshardSearchFilters} and {@link #getBitSet}.
     */
    @Nullable
    public static BitSet computeBitSet(Query query, LeafReaderContext context) throws IOException {
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

    static boolean isEffectiveMatchAllDocsQuery(Query rewrittenQuery) {
        if (rewrittenQuery instanceof ConstantScoreQuery csq && csq.getQuery() instanceof MatchAllDocsQuery) {
            return true;
        }
        return rewrittenQuery instanceof MatchAllDocsQuery;
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
                "the resharding unowned BitSet cache is full which may impact performance; consider increasing [{}]",
                CACHE_SIZE_SETTING.getKey()
            );
        }
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

    void verifyInternalConsistency() {
        bitsetCache.keys().forEach(cacheKey -> {
            final Set<BitsetCacheKey> keys = keysByIndexReader.get(cacheKey.indexKey);
            if (keys == null || keys.contains(cacheKey) == false) {
                throw new IllegalStateException(
                    "Key [" + cacheKey + "] is in the cache, but the lookup entry for [" + cacheKey.indexKey + "] does not contain that key"
                );
            }
        });
        keysByIndexReader.forEach((indexKey, keys) -> {
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
