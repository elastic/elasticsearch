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

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.join.BitDocIdSetFilter;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.SparseFixedBitSet;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.NoCacheFilter;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
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
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

/**
 * This is a cache for {@link BitDocIdSet} based filters and is unbounded by size or time.
 * <p/>
 * Use this cache with care, only components that require that a filter is to be materialized as a {@link BitDocIdSet}
 * and require that it should always be around should use this cache, otherwise the
 * {@link org.elasticsearch.index.cache.filter.FilterCache} should be used instead.
 */
public class BitsetFilterCache extends AbstractIndexComponent implements LeafReader.CoreClosedListener, RemovalListener<Object, Cache<Filter, BitsetFilterCache.Value>>, Closeable {

    public static final String LOAD_RANDOM_ACCESS_FILTERS_EAGERLY = "index.load_fixed_bitset_filters_eagerly";

    private final boolean loadRandomAccessFiltersEagerly;
    private final Cache<Object, Cache<Filter, Value>> loadedFilters;
    private final BitDocIdSetFilterWarmer warmer;

    private IndexService indexService;
    private IndicesWarmer indicesWarmer;

    @Inject
    public BitsetFilterCache(Index index, @IndexSettings Settings indexSettings) {
        super(index, indexSettings);
        this.loadRandomAccessFiltersEagerly = indexSettings.getAsBoolean(LOAD_RANDOM_ACCESS_FILTERS_EAGERLY, true);
        this.loadedFilters = CacheBuilder.newBuilder().removalListener(this).build();
        this.warmer = new BitDocIdSetFilterWarmer();
    }

    @Inject(optional = true)
    public void setIndicesWarmer(IndicesWarmer indicesWarmer) {
        this.indicesWarmer = indicesWarmer;
    }

    public void setIndexService(IndexService indexService) {
        this.indexService = indexService;
        // First the indicesWarmer is set and then the indexService is set, because of this there is a small window of
        // time where indexService is null. This is why the warmer should only registered after indexService has been set.
        // Otherwise there is a small chance of the warmer running into a NPE, since it uses the indexService
        indicesWarmer.addListener(warmer);
    }

    public BitDocIdSetFilter getBitDocIdSetFilter(Filter filter) {
        assert filter != null;
        assert !(filter instanceof NoCacheFilter);
        return new BitDocIdSetFilterWrapper(filter);
    }

    @Override
    public void onClose(Object ownerCoreCacheKey) {
        loadedFilters.invalidate(ownerCoreCacheKey);
    }

    @Override
    public void close() throws ElasticsearchException {
        indicesWarmer.removeListener(warmer);
        clear("close");
    }

    public void clear(String reason) {
        logger.debug("clearing all bitsets because [{}]", reason);
        loadedFilters.invalidateAll();
    }

    private BitDocIdSet getAndLoadIfNotPresent(final Filter filter, final LeafReaderContext context) throws IOException, ExecutionException {
        final Object coreCacheReader = context.reader().getCoreCacheKey();
        final ShardId shardId = ShardUtils.extractShardId(context.reader());
        Cache<Filter, Value> filterToFbs = loadedFilters.get(coreCacheReader, new Callable<Cache<Filter, Value>>() {
            @Override
            public Cache<Filter, Value> call() throws Exception {
                context.reader().addCoreClosedListener(BitsetFilterCache.this);
                return CacheBuilder.newBuilder().build();
            }
        });
        return filterToFbs.get(filter, new Callable<Value>() {
            @Override
            public Value call() throws Exception {
                DocIdSet docIdSet = filter.getDocIdSet(context, null);
                final BitDocIdSet bitSet;
                if (docIdSet instanceof BitDocIdSet) {
                    bitSet = (BitDocIdSet) docIdSet;
                } else {
                    BitDocIdSet.Builder builder = new BitDocIdSet.Builder(context.reader().maxDoc());
                    if (docIdSet != null && docIdSet != DocIdSet.EMPTY) {
                        DocIdSetIterator iterator = docIdSet.iterator();
                        // some filters (QueryWrapperFilter) return not null or DocIdSet.EMPTY if there no matching docs
                        if (iterator != null) {
                            builder.or(iterator);
                        }
                    }
                    BitDocIdSet bits = builder.build();
                    // code expects this to be non-null
                    if (bits == null) {
                        bits = new BitDocIdSet(new SparseFixedBitSet(context.reader().maxDoc()), 0);
                    }
                    bitSet = bits;
                }

                Value value = new Value(bitSet, shardId);
                if (shardId != null) {
                    IndexShard shard = indexService.shard(shardId.id());
                    if (shard != null) {
                        shard.shardBitsetFilterCache().onCached(value.bitset.ramBytesUsed());
                    }
                }
                return value;
            }
        }).bitset;
    }

    @Override
    public void onRemoval(RemovalNotification<Object, Cache<Filter, Value>> notification) {
        Object key = notification.getKey();
        if (key == null) {
            return;
        }

        Cache<Filter, Value> value = notification.getValue();
        if (value == null) {
            return;
        }

        for (Map.Entry<Filter, Value> entry : value.asMap().entrySet()) {
            if (entry.getValue().shardId == null) {
                continue;
            }
            IndexShard shard = indexService.shard(entry.getValue().shardId.id());
            if (shard != null) {
                ShardBitsetFilterCache shardBitsetFilterCache = shard.shardBitsetFilterCache();
                shardBitsetFilterCache.onRemoval(entry.getValue().bitset.ramBytesUsed());
            }
            // if null then this means the shard has already been removed and the stats are 0 anyway for the shard this key belongs to
        }
    }

    public static final class Value {

        final BitDocIdSet bitset;
        final ShardId shardId;

        public Value(BitDocIdSet bitset, ShardId shardId) {
            this.bitset = bitset;
            this.shardId = shardId;
        }
    }

    final class BitDocIdSetFilterWrapper extends BitDocIdSetFilter {

        final Filter filter;

        BitDocIdSetFilterWrapper(Filter filter) {
            this.filter = filter;
        }

        @Override
        public BitDocIdSet getDocIdSet(LeafReaderContext context) throws IOException {
            try {
                return getAndLoadIfNotPresent(filter, context);
            } catch (ExecutionException e) {
                throw ExceptionsHelper.convertToElastic(e);
            }
        }

        @Override
        public String toString(String field) {
            return "random_access(" + filter + ")";
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof BitDocIdSetFilterWrapper)) return false;
            return this.filter.equals(((BitDocIdSetFilterWrapper) o).filter);
        }

        @Override
        public int hashCode() {
            return filter.hashCode() ^ 0x1117BF26;
        }
    }

    final class BitDocIdSetFilterWarmer extends IndicesWarmer.Listener {

        @Override
        public IndicesWarmer.TerminationHandle warmNewReaders(final IndexShard indexShard, IndexMetaData indexMetaData, IndicesWarmer.WarmerContext context, ThreadPool threadPool) {
            if (!loadRandomAccessFiltersEagerly) {
                return TerminationHandle.NO_WAIT;
            }

            boolean hasNested = false;
            final Set<Filter> warmUp = new HashSet<>();
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
                for (final Filter filterToWarm : warmUp) {
                    executor.execute(new Runnable() {

                        @Override
                        public void run() {
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
                        }

                    });
                }
            }
            return new TerminationHandle() {
                @Override
                public void awaitTermination() throws InterruptedException {
                    latch.await();
                }
            };
        }

        @Override
        public TerminationHandle warmTopReader(IndexShard indexShard, IndexMetaData indexMetaData, IndicesWarmer.WarmerContext context, ThreadPool threadPool) {
            return TerminationHandle.NO_WAIT;
        }

    }

    Cache<Object, Cache<Filter, Value>> getLoadedFilters() {
        return loadedFilters;
    }
}
