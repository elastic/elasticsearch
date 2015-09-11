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

package org.elasticsearch.index.cache.bitset;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardUtils;
import org.elasticsearch.indices.IndicesWarmer;
import org.elasticsearch.indices.IndicesWarmer.TerminationHandle;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.*;

/**
 * This is a cache for {@link BitDocIdSet} based filters and is unbounded by size or time.
 * <p/>
 * Use this cache with care, only components that require that a filter is to be materialized as a {@link BitDocIdSet}
 * and require that it should always be around should use this cache, otherwise the
 * {@link org.elasticsearch.index.cache.query.QueryCache} should be used instead.
 */
public class BitsetFilterCache extends AbstractIndexComponent implements LeafReader.CoreClosedListener, RemovalListener<Object, Cache<Query, BitsetFilterCache.Value>>, Closeable {

    public static final String LOAD_RANDOM_ACCESS_FILTERS_EAGERLY = "index.load_fixed_bitset_filters_eagerly";
    private static final Listener DEFAULT_NOOP_LISTENER =  new Listener() {
        @Override
        public void onCache(ShardId shardId, Accountable accountable) {
        }

        @Override
        public void onRemoval(ShardId shardId, Accountable accountable) {
        }
    };

    private final boolean loadRandomAccessFiltersEagerly;
    private final Cache<Object, Cache<Query, Value>> loadedFilters;
    private volatile Listener listener = DEFAULT_NOOP_LISTENER;
    private final BitSetProducerWarmer warmer;

    private IndicesWarmer indicesWarmer;

    @Inject
    public BitsetFilterCache(Index index, @IndexSettings Settings indexSettings) {
        super(index, indexSettings);
        this.loadRandomAccessFiltersEagerly = indexSettings.getAsBoolean(LOAD_RANDOM_ACCESS_FILTERS_EAGERLY, true);
        this.loadedFilters = CacheBuilder.newBuilder().removalListener(this).build();
        this.warmer = new BitSetProducerWarmer();
    }

    @Inject(optional = true)
    public void setIndicesWarmer(IndicesWarmer indicesWarmer) {
        this.indicesWarmer = indicesWarmer;
        indicesWarmer.addListener(warmer);
    }

    /**
     * Sets a listener that is invoked for all subsequent cache and removal events.
     * @throws IllegalStateException if the listener is set more than once
     */
    public void setListener(Listener listener) {
        if (listener == null) {
            throw new IllegalArgumentException("listener must not be null");
        }
        if (this.listener != DEFAULT_NOOP_LISTENER) {
            throw new IllegalStateException("can't set listener more than once");
        }
        this.listener = listener;
    }


    public BitSetProducer getBitSetProducer(Query query) {
        return new QueryWrapperBitSetProducer(query);
    }

    @Override
    public void onClose(Object ownerCoreCacheKey) {
        loadedFilters.invalidate(ownerCoreCacheKey);
    }

    @Override
    public void close() {
        if (indicesWarmer != null) {
            indicesWarmer.removeListener(warmer);
        }
        clear("close");
    }

    public void clear(String reason) {
        logger.debug("clearing all bitsets because [{}]", reason);
        loadedFilters.invalidateAll();
    }

    private BitSet getAndLoadIfNotPresent(final Query query, final LeafReaderContext context) throws IOException, ExecutionException {
        final Object coreCacheReader = context.reader().getCoreCacheKey();
        final ShardId shardId = ShardUtils.extractShardId(context.reader());
        Cache<Query, Value> filterToFbs = loadedFilters.get(coreCacheReader, new Callable<Cache<Query, Value>>() {
            @Override
            public Cache<Query, Value> call() throws Exception {
                context.reader().addCoreClosedListener(BitsetFilterCache.this);
                return CacheBuilder.newBuilder().build();
            }
        });
        return filterToFbs.get(query, () -> {
            final IndexReaderContext topLevelContext = ReaderUtil.getTopLevelContext(context);
            final IndexSearcher searcher = new IndexSearcher(topLevelContext);
            searcher.setQueryCache(null);
            final Weight weight = searcher.createNormalizedWeight(query, false);
            final DocIdSetIterator it = weight.scorer(context);
            final BitSet bitSet;
            if (it == null) {
                bitSet = null;
            } else {
                bitSet = BitSet.of(it, context.reader().maxDoc());
            }

            Value value = new Value(bitSet, shardId);
            listener.onCache(shardId, value.bitset);
            return value;
        }).bitset;
    }

    @Override
    public void onRemoval(RemovalNotification<Object, Cache<Query, Value>> notification) {
        Object key = notification.getKey();
        if (key == null) {
            return;
        }

        Cache<Query, Value> valueCache = notification.getValue();
        if (valueCache == null) {
            return;
        }

        for (Value value : valueCache.asMap().values()) {
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
        public BitSet getBitSet(LeafReaderContext context) throws IOException {
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
            if (!(o instanceof QueryWrapperBitSetProducer)) return false;
            return this.query.equals(((QueryWrapperBitSetProducer) o).query);
        }

        @Override
        public int hashCode() {
            return 31 * getClass().hashCode() + query.hashCode();
        }
    }

    final class BitSetProducerWarmer extends IndicesWarmer.Listener {

        @Override
        public IndicesWarmer.TerminationHandle warmNewReaders(final IndexShard indexShard, IndexMetaData indexMetaData, IndicesWarmer.WarmerContext context, ThreadPool threadPool) {
            if (!loadRandomAccessFiltersEagerly) {
                return TerminationHandle.NO_WAIT;
            }

            boolean hasNested = false;
            final Set<Query> warmUp = new HashSet<>();
            final MapperService mapperService = indexShard.mapperService();
            for (DocumentMapper docMapper : mapperService.docMappers(false)) {
                if (docMapper.hasNestedObjects()) {
                    hasNested = true;
                    for (ObjectMapper objectMapper : docMapper.objectMappers().values()) {
                        if (objectMapper.nested().isNested()) {
                            ObjectMapper parentObjectMapper = docMapper.findParentObjectMapper(objectMapper);
                            if (parentObjectMapper != null && parentObjectMapper.nested().isNested()) {
                                warmUp.add(parentObjectMapper.nestedTypeFilter());
                            }
                        }
                    }
                }
            }

            if (hasNested) {
                warmUp.add(Queries.newNonNestedFilter());
            }

            final Executor executor = threadPool.executor(executor());
            final CountDownLatch latch = new CountDownLatch(context.searcher().reader().leaves().size() * warmUp.size());
            for (final LeafReaderContext ctx : context.searcher().reader().leaves()) {
                for (final Query filterToWarm : warmUp) {
                    executor.execute(() -> {
                        try {
                            final long start = System.nanoTime();
                            getAndLoadIfNotPresent(filterToWarm, ctx);
                            if (indexShard.warmerService().logger().isTraceEnabled()) {
                                indexShard.warmerService().logger().trace("warmed bitset for [{}], took [{}]", filterToWarm, TimeValue.timeValueNanos(System.nanoTime() - start));
                            }
                        } catch (Throwable t) {
                            indexShard.warmerService().logger().warn("failed to load bitset for [{}]", t, filterToWarm);
                        } finally {
                            latch.countDown();
                        }
                    });
                }
            }
            return () -> latch.await();
        }

        @Override
        public TerminationHandle warmTopReader(IndexShard indexShard, IndexMetaData indexMetaData, IndicesWarmer.WarmerContext context, ThreadPool threadPool) {
            return TerminationHandle.NO_WAIT;
        }

    }

    Cache<Object, Cache<Query, Value>> getLoadedFilters() {
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
