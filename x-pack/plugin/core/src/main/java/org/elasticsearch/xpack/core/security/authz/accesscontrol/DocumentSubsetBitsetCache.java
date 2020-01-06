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
import org.elasticsearch.common.cache.RemovalNotification;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This is a cache for {@link BitSet} instances that are used with the {@link DocumentSubsetReader}.
 * It is bounded by memory size and access time.
 *
 * @see org.elasticsearch.index.cache.bitset.BitsetFilterCache
 */
public final class DocumentSubsetBitsetCache implements IndexReader.ClosedListener, Closeable, Accountable {

    /**
     * The TTL defaults to 1 week. We depend on the {@code max_bytes} setting to keep the cache to a sensible size, by evicting LRU
     * entries, however there is benefit in reclaiming memory by expiring bitsets that have not be used for some period of time.
     * Because {@link org.elasticsearch.xpack.core.security.authz.permission.IndicesPermission.Group#query} can be templated, it is
     * not uncommon for a query to only be used for a relatively short period of time (e.g. because a user's metadata changed, or because
     * that user is an infrequent user of Elasticsearch). This access time expiry helps free up memory in those circumstances even if the
     * cache is never filled.
     */
    static final Setting<TimeValue> CACHE_TTL_SETTING =
        Setting.timeSetting("xpack.security.dls.bitset.cache.ttl", TimeValue.timeValueHours(24 * 7), Property.NodeScope);

    static final Setting<ByteSizeValue> CACHE_SIZE_SETTING = Setting.byteSizeSetting("xpack.security.dls.bitset.cache.size",
            new ByteSizeValue(50, ByteSizeUnit.MB), Property.NodeScope);

    private static final BitSet NULL_MARKER = new FixedBitSet(0);

    private final Logger logger;

    /**
     * When a {@link BitSet} is evicted from {@link #bitsetCache}, we need to also remove it from {@link #keysByIndex}.
     * We use a {@link ReentrantReadWriteLock} to control atomicity here - the "read" side represents potential insertions to the
     * {@link #bitsetCache}, the "write" side represents removals from {@link #keysByIndex}.
     * The risk (that {@link Cache} does not provide protection for) is that an entry is removed from the cache, and then immediately
     * re-populated, before we process the removal event. To protect against that we need to check the state of the {@link #bitsetCache}
     * but we need exclusive ("write") access while performing that check and updating the values in {@link #keysByIndex}.
     */
    private final ReleasableLock cacheEvictionLock;
    private final ReleasableLock cacheModificationLock;
    private final ExecutorService cleanupExecutor;

    private final Cache<BitsetCacheKey, BitSet> bitsetCache;
    private final Map<IndexReader.CacheKey, Set<BitsetCacheKey>> keysByIndex;

    public DocumentSubsetBitsetCache(Settings settings, ThreadPool threadPool) {
        this(settings, threadPool.executor(ThreadPool.Names.GENERIC));
    }

    /**
     * @param settings The global settings object for this node
     * @param cleanupExecutor An executor on which the cache cleanup tasks can be run. Due to the way the cache is structured internally,
     *                        it is sometimes necessary to run an asynchronous task to synchronize the internal state.
     */
    protected DocumentSubsetBitsetCache(Settings settings, ExecutorService cleanupExecutor) {
        this.logger = LogManager.getLogger(getClass());

        final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
        this.cacheEvictionLock = new ReleasableLock(readWriteLock.writeLock());
        this.cacheModificationLock = new ReleasableLock(readWriteLock.readLock());
        this.cleanupExecutor = cleanupExecutor;

        final TimeValue ttl = CACHE_TTL_SETTING.get(settings);
        final ByteSizeValue size = CACHE_SIZE_SETTING.get(settings);
        this.bitsetCache = CacheBuilder.<BitsetCacheKey, BitSet>builder()
            .setExpireAfterAccess(ttl)
            .setMaximumWeight(size.getBytes())
            .weigher((key, bitSet) -> bitSet == NULL_MARKER ? 0 : bitSet.ramBytesUsed())
            .removalListener(this::onCacheEviction)
            .build();

        this.keysByIndex = new ConcurrentHashMap<>();
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

    /**
     * Cleanup (synchronize) the internal state when an object is removed from the primary cache
     */
    private void onCacheEviction(RemovalNotification<BitsetCacheKey, BitSet> notification) {
        final BitsetCacheKey bitsetKey = notification.getKey();
        final IndexReader.CacheKey indexKey = bitsetKey.index;
        if (keysByIndex.getOrDefault(indexKey, Set.of()).contains(bitsetKey) == false) {
            // If the bitsetKey isn't in the lookup map, then there's nothing to synchronize
            return;
        }
        // We push this to a background thread, so that it reduces the risk of blocking searches, but also so that the lock management is
        // simpler - this callback is likely to take place on a thread that is actively adding something to the cache, and is therefore
        // holding the read ("update") side of the lock. It is not possible to upgrade a read lock to a write ("eviction") lock, but we
        // need to acquire that lock here.
        cleanupExecutor.submit(() -> {
            try (ReleasableLock ignored = cacheEvictionLock.acquire()) {
                // it's possible for the key to be back in the cache if it was immediately repopulated after it was evicted, so check
                if (bitsetCache.get(bitsetKey) == null) {
                    // key is no longer in the cache, make sure it is no longer in the lookup map either.
                    keysByIndex.getOrDefault(indexKey, Set.of()).remove(bitsetKey);
                }
            }
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

        try (ReleasableLock ignored = cacheModificationLock.acquire()) {
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
                    return BitSet.of(s.iterator(), context.reader().maxDoc());
                }
            });
            if (bitSet == NULL_MARKER) {
                return null;
            } else {
                return bitSet;
            }
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

    /**
     * This method verifies that the two internal data structures ({@link #bitsetCache} and {@link #keysByIndex}) are consistent with one
     * another. This method is only called by tests.
     */
    void verifyInternalConsistency() {
        this.bitsetCache.keys().forEach(bck -> {
            final Set<BitsetCacheKey> set = this.keysByIndex.get(bck.index);
            if (set == null) {
                throw new IllegalStateException("Key [" + bck + "] is in the cache, but there is no entry for [" + bck.index +
                    "] in the lookup map");
            }
            if (set.contains(bck) == false) {
                throw new IllegalStateException("Key [" + bck + "] is in the cache, but the lookup entry for [" + bck.index +
                    "] does not contain that key");
            }
        });
        this.keysByIndex.values().stream().flatMap(Set::stream).forEach(bck -> {
            if (this.bitsetCache.get(bck) == null) {
                throw new IllegalStateException("Key [" + bck + "] is in the lookup map, but is not in the cache");
            }
        });
    }
}
