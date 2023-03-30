/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.cache.bitset;

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
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.cache.RemovalListener;
import org.elasticsearch.common.cache.RemovalNotification;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexWarmer;
import org.elasticsearch.index.IndexWarmer.TerminationHandle;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.NestedLookup;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardUtils;
import org.elasticsearch.lucene.util.BitSets;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

/**
 * This is a cache for {@link BitDocIdSet} based filters and is unbounded by size or time.
 * <p>
 * Use this cache with care, only components that require that a filter is to be materialized as a {@link BitDocIdSet}
 * and require that it should always be around should use this cache, otherwise the
 * {@link org.elasticsearch.index.cache.query.QueryCache} should be used instead.
 */
public final class BitsetFilterCache
    implements
        IndexReader.ClosedListener,
        RemovalListener<IndexReader.CacheKey, Cache<Query, BitsetFilterCache.Value>>,
        Closeable {

    public static final Setting<Boolean> INDEX_LOAD_RANDOM_ACCESS_FILTERS_EAGERLY_SETTING = Setting.boolSetting(
        "index.load_fixed_bitset_filters_eagerly",
        true,
        Property.IndexScope
    );

    private static final Logger logger = LogManager.getLogger(BitsetFilterCache.class);

    private final boolean loadRandomAccessFiltersEagerly;

    /**
     * Lazy initialized by {@link #buildFiltersCache()} to save heap for indices not using this cache as even empty {@link Cache} are
     * quite heavy-weight.
     */
    private volatile Cache<IndexReader.CacheKey, Cache<Query, Value>> loadedFilters;
    private final Listener listener;

    private final Index index;

    public BitsetFilterCache(IndexSettings indexSettings, Listener listener) {
        if (listener == null) {
            throw new IllegalArgumentException("listener must not be null");
        }
        this.index = indexSettings.getIndex();
        this.loadRandomAccessFiltersEagerly = indexSettings.getValue(INDEX_LOAD_RANDOM_ACCESS_FILTERS_EAGERLY_SETTING);
        this.listener = listener;
    }

    public static BitSet bitsetFromQuery(Query query, LeafReaderContext context) throws IOException {
        final IndexReaderContext topLevelContext = ReaderUtil.getTopLevelContext(context);
        final IndexSearcher searcher = new IndexSearcher(topLevelContext);
        searcher.setQueryCache(null);
        final Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
        Scorer s = weight.scorer(context);
        if (s == null) {
            return null;
        } else {
            return BitSets.of(s.iterator(), context.reader().maxDoc());
        }
    }

    public IndexWarmer.Listener createListener(ThreadPool threadPool) {
        return new BitSetProducerWarmer(threadPool);
    }

    public BitSetProducer getBitSetProducer(Query query) {
        return new QueryWrapperBitSetProducer(query);
    }

    @Override
    public void onClose(IndexReader.CacheKey ownerCoreCacheKey) {
        var filters = loadedFilters;
        if (filters != null) {
            filters.invalidate(ownerCoreCacheKey);
        }
    }

    @Override
    public void close() {
        clear("close");
    }

    public void clear(String reason) {
        logger.debug("clearing all bitsets for [{}] because [{}]", index, reason);
        var filters = loadedFilters;
        if (filters != null) {
            filters.invalidateAll();
        }
    }

    private BitSet getAndLoadIfNotPresent(final Query query, final LeafReaderContext context) throws ExecutionException {
        final IndexReader.CacheHelper cacheHelper = context.reader().getCoreCacheHelper();
        if (cacheHelper == null) {
            throw new IllegalArgumentException("Reader " + context.reader() + " does not support caching");
        }
        final IndexReader.CacheKey coreCacheReader = cacheHelper.getKey();
        final ShardId shardId = ShardUtils.extractShardId(context.reader());
        if (shardId == null) {
            throw new IllegalStateException(
                "Null shardId. If you got here from a test, you need to wrap the directory reader. "
                    + "see for example AggregatorTestCase#wrapInMockESDirectoryReader.  If you got here in production, please file a bug."
            );
        }
        if (index.equals(shardId.getIndex()) == false) {
            // insanity
            throw new IllegalStateException("Trying to load bit set for index " + shardId.getIndex() + " with cache of index " + index);
        }
        var filters = loadedFilters;
        if (filters == null) {
            filters = buildFiltersCache();
        }
        Cache<Query, Value> filterToFbs = filters.computeIfAbsent(coreCacheReader, key -> {
            cacheHelper.addClosedListener(BitsetFilterCache.this);
            return CacheBuilder.<Query, Value>builder().build();
        });

        return filterToFbs.computeIfAbsent(query, key -> {
            final BitSet bitSet = bitsetFromQuery(query, context);
            Value value = new Value(bitSet, shardId);
            listener.onCache(shardId, value.bitset);
            return value;
        }).bitset;
    }

    private synchronized Cache<IndexReader.CacheKey, Cache<Query, Value>> buildFiltersCache() {
        var existing = loadedFilters;
        if (existing != null) {
            return existing;
        }
        existing = CacheBuilder.<IndexReader.CacheKey, Cache<Query, Value>>builder().removalListener(this).build();
        loadedFilters = existing;
        return existing;
    }

    @Override
    public void onRemoval(RemovalNotification<IndexReader.CacheKey, Cache<Query, Value>> notification) {
        if (notification.getKey() == null) {
            return;
        }

        Cache<Query, Value> valueCache = notification.getValue();
        if (valueCache == null) {
            return;
        }

        for (Value value : valueCache.values()) {
            listener.onRemoval(value.shardId, value.bitset);
            // if null then this means the shard has already been removed and the stats are 0 anyway for the shard this key belongs to
        }
    }

    public static final class Value {

        final BitSet bitset;
        final ShardId shardId;

        public Value(BitSet bitset, ShardId shardId) {
            this.bitset = bitset;
            this.shardId = shardId;
        }
    }

    final class QueryWrapperBitSetProducer implements BitSetProducer {

        final Query query;

        QueryWrapperBitSetProducer(Query query) {
            this.query = Objects.requireNonNull(query);
        }

        @Override
        public BitSet getBitSet(LeafReaderContext context) {
            try {
                return getAndLoadIfNotPresent(query, context);
            } catch (ExecutionException e) {
                throw ExceptionsHelper.convertToElastic(e);
            }
        }

        @Override
        public String toString() {
            return "random_access(" + query + ")";
        }

        @Override
        public boolean equals(Object o) {
            if ((o instanceof QueryWrapperBitSetProducer) == false) return false;
            return this.query.equals(((QueryWrapperBitSetProducer) o).query);
        }

        @Override
        public int hashCode() {
            return 31 * getClass().hashCode() + query.hashCode();
        }
    }

    final class BitSetProducerWarmer implements IndexWarmer.Listener {

        private final Executor executor;

        BitSetProducerWarmer(ThreadPool threadPool) {
            this.executor = threadPool.executor(ThreadPool.Names.WARMER);
        }

        @Override
        public IndexWarmer.TerminationHandle warmReader(final IndexShard indexShard, final ElasticsearchDirectoryReader reader) {
            if (index.equals(indexShard.indexSettings().getIndex()) == false) {
                // this is from a different index
                return TerminationHandle.NO_WAIT;
            }

            if (loadRandomAccessFiltersEagerly == false) {
                return TerminationHandle.NO_WAIT;
            }

            final Set<Query> warmUp = new HashSet<>();
            final MapperService mapperService = indexShard.mapperService();
            MappingLookup lookup = mapperService.mappingLookup();
            NestedLookup nestedLookup = lookup.nestedLookup();
            if (nestedLookup != NestedLookup.EMPTY) {
                warmUp.add(Queries.newNonNestedFilter(mapperService.getIndexSettings().getIndexVersionCreated()));
                warmUp.addAll(nestedLookup.getNestedParentFilters().values());
            }

            final CountDownLatch latch = new CountDownLatch(reader.leaves().size() * warmUp.size());
            for (final LeafReaderContext ctx : reader.leaves()) {
                for (final Query filterToWarm : warmUp) {
                    executor.execute(() -> {
                        try {
                            final long start = System.nanoTime();
                            getAndLoadIfNotPresent(filterToWarm, ctx);
                            if (indexShard.warmerService().logger().isTraceEnabled()) {
                                indexShard.warmerService()
                                    .logger()
                                    .trace(
                                        "warmed bitset for [{}], took [{}]",
                                        filterToWarm,
                                        TimeValue.timeValueNanos(System.nanoTime() - start)
                                    );
                            }
                        } catch (Exception e) {
                            indexShard.warmerService().logger().warn(() -> "failed to load bitset for [" + filterToWarm + "]", e);
                        } finally {
                            latch.countDown();
                        }
                    });
                }
            }
            return () -> latch.await();
        }

    }

    // for testing
    Cache<IndexReader.CacheKey, Cache<Query, Value>> getLoadedFilters() {
        return loadedFilters;
    }

    /**
     *  A listener interface that is executed for each onCache / onRemoval event
     */
    public interface Listener {
        /**
         * Called for each cached bitset on the cache event.
         * @param shardId the shard id the bitset was cached for. This can be <code>null</code>
         * @param accountable the bitsets ram representation
         */
        void onCache(ShardId shardId, Accountable accountable);

        /**
         * Called for each cached bitset on the removal event.
         * @param shardId the shard id the bitset was cached for. This can be <code>null</code>
         * @param accountable the bitsets ram representation
         */
        void onRemoval(ShardId shardId, Accountable accountable);
    }
}
