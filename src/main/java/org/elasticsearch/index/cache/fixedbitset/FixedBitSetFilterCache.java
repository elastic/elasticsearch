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

package org.elasticsearch.index.cache.fixedbitset;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.component.CloseableComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.SegmentReaderUtils;
import org.elasticsearch.common.lucene.search.NoCacheFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.search.nested.NestedDocsFilter;
import org.elasticsearch.index.search.nested.NonNestedDocsFilter;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.service.InternalIndexService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardUtils;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.warmer.IndicesWarmer;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

/**
 * This is a cache for {@link FixedBitSet} based filters and is unbounded by size or time.
 * <p/>
 * Use this cache with care, only components that require that a filter is to be materialized as a {@link FixedBitSet}
 * and require that it should always be around should use this cache, otherwise the
 * {@link org.elasticsearch.index.cache.filter.FilterCache} should be used instead.
 */
public class FixedBitSetFilterCache extends AbstractIndexComponent implements AtomicReader.CoreClosedListener, RemovalListener<Object, Cache<Filter, FixedBitSetFilterCache.Value>>, CloseableComponent {

    public static final String LOAD_RANDOM_ACCESS_FILTERS_EAGERLY = "index.load_fixed_bitset_filters_eagerly";

    private final boolean loadRandomAccessFiltersEagerly;
    private final Cache<Object, Cache<Filter, Value>> loadedFilters;
    private final FixedBitSetFilterWarmer warmer;

    private IndexService indexService;
    private IndicesWarmer indicesWarmer;

    @Inject
    public FixedBitSetFilterCache(Index index, @IndexSettings Settings indexSettings) {
        super(index, indexSettings);
        this.loadRandomAccessFiltersEagerly = indexSettings.getAsBoolean(LOAD_RANDOM_ACCESS_FILTERS_EAGERLY, true);
        this.loadedFilters = CacheBuilder.newBuilder().removalListener(this).build();
        this.warmer = new FixedBitSetFilterWarmer();
    }

    @Inject(optional = true)
    public void setIndicesWarmer(IndicesWarmer indicesWarmer) {
        this.indicesWarmer = indicesWarmer;
    }

    public void setIndexService(InternalIndexService indexService) {
        this.indexService = indexService;
        // First the indicesWarmer is set and then the indexService is set, because of this there is a small window of
        // time where indexService is null. This is why the warmer should only registered after indexService has been set.
        // Otherwise there is a small chance of the warmer running into a NPE, since it uses the indexService
        indicesWarmer.addListener(warmer);
    }

    public FixedBitSetFilter getFixedBitSetFilter(Filter filter) {
        assert filter != null;
        assert !(filter instanceof NoCacheFilter);
        return new FixedBitSetFilterWrapper(filter);
    }

    @Override
    public void onClose(Object ownerCoreCacheKey) {
        loadedFilters.invalidate(ownerCoreCacheKey);
    }

    public void close() throws ElasticsearchException {
        indicesWarmer.removeListener(warmer);
        clear("close");
    }

    public void clear(String reason) {
        logger.debug("Clearing all FixedBitSets because [{}]", reason);
        loadedFilters.invalidateAll();
        loadedFilters.cleanUp();
    }

    private FixedBitSet getAndLoadIfNotPresent(final Filter filter, final AtomicReaderContext context) throws IOException, ExecutionException {
        final Object coreCacheReader = context.reader().getCoreCacheKey();
        final ShardId shardId = ShardUtils.extractShardId(context.reader());
        Cache<Filter, Value> filterToFbs = loadedFilters.get(coreCacheReader, new Callable<Cache<Filter, Value>>() {
            @Override
            public Cache<Filter, Value> call() throws Exception {
                SegmentReaderUtils.registerCoreListener(context.reader(), FixedBitSetFilterCache.this);
                return CacheBuilder.newBuilder().build();
            }
        });
        return filterToFbs.get(filter, new Callable<Value>() {
            @Override
            public Value call() throws Exception {
                DocIdSet docIdSet = filter.getDocIdSet(context, null);
                final FixedBitSet fixedBitSet;
                if (docIdSet instanceof FixedBitSet) {
                    fixedBitSet = (FixedBitSet) docIdSet;
                } else {
                    fixedBitSet = new FixedBitSet(context.reader().maxDoc());
                    if (docIdSet != null && docIdSet != DocIdSet.EMPTY) {
                        DocIdSetIterator iterator = docIdSet.iterator();
                        if (iterator != null) {
                            int doc = iterator.nextDoc();
                            if (doc != DocIdSetIterator.NO_MORE_DOCS) {
                                do {
                                    fixedBitSet.set(doc);
                                    doc = iterator.nextDoc();
                                } while (doc != DocIdSetIterator.NO_MORE_DOCS);
                            }
                        }
                    }
                }

                Value value = new Value(fixedBitSet, shardId);
                if (shardId != null) {
                    IndexShard shard = indexService.shard(shardId.id());
                    if (shard != null) {
                        shard.shardFixedBitSetFilterCache().onCached(value.fixedBitSet.ramBytesUsed());
                    }
                }
                return value;
            }
        }).fixedBitSet;
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
                ShardFixedBitSetFilterCache shardFixedBitSetFilterCache = shard.shardFixedBitSetFilterCache();
                shardFixedBitSetFilterCache.onRemoval(entry.getValue().fixedBitSet.ramBytesUsed());
            }
            // if null then this means the shard has already been removed and the stats are 0 anyway for the shard this key belongs to
        }
    }

    public static final class Value {

        final FixedBitSet fixedBitSet;
        final ShardId shardId;

        public Value(FixedBitSet fixedBitSet, ShardId shardId) {
            this.fixedBitSet = fixedBitSet;
            this.shardId = shardId;
        }
    }

    final class FixedBitSetFilterWrapper extends FixedBitSetFilter {

        final Filter filter;

        FixedBitSetFilterWrapper(Filter filter) {
            this.filter = filter;
        }

        @Override
        public FixedBitSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
            try {
                return getAndLoadIfNotPresent(filter, context);
            } catch (ExecutionException e) {
                throw ExceptionsHelper.convertToElastic(e);
            }
        }

        public String toString() {
            return "random_access(" + filter + ")";
        }

        public boolean equals(Object o) {
            if (!(o instanceof FixedBitSetFilterWrapper)) return false;
            return this.filter.equals(((FixedBitSetFilterWrapper) o).filter);
        }

        public int hashCode() {
            return filter.hashCode() ^ 0x1117BF26;
        }
    }

    final class FixedBitSetFilterWarmer extends IndicesWarmer.Listener {

        @Override
        public TerminationHandle warmNewReaders(final IndexShard indexShard, IndexMetaData indexMetaData, IndicesWarmer.WarmerContext context, ThreadPool threadPool) {
            if (!loadRandomAccessFiltersEagerly) {
                return TerminationHandle.NO_WAIT;
            }

            boolean hasNested = false;
            final Set<Filter> warmUp = new HashSet<>();
            final MapperService mapperService = indexShard.mapperService();
            for (DocumentMapper docMapper : mapperService.docMappers(false)) {
                ParentFieldMapper parentFieldMapper = docMapper.parentFieldMapper();
                if (parentFieldMapper.active()) {
                    warmUp.add(docMapper.typeFilter());
                    DocumentMapper parentDocumentMapper = mapperService.documentMapper(parentFieldMapper.type());
                    if (parentDocumentMapper != null) {
                        warmUp.add(parentDocumentMapper.typeFilter());
                    }
                }

                if (docMapper.hasNestedObjects()) {
                    hasNested = true;
                    for (ObjectMapper objectMapper : docMapper.objectMappers().values()) {
                        if (objectMapper.nested().isNested()) {
                            warmUp.add(objectMapper.nestedTypeFilter());
                        }
                    }
                }
            }

            if (hasNested) {
                warmUp.add(NonNestedDocsFilter.INSTANCE);
                warmUp.add(NestedDocsFilter.INSTANCE);
            }

            final Executor executor = threadPool.executor(executor());
            final CountDownLatch latch = new CountDownLatch(context.searcher().reader().leaves().size() * warmUp.size());
            for (final AtomicReaderContext ctx : context.searcher().reader().leaves()) {
                for (final Filter filterToWarm : warmUp) {
                    executor.execute(new Runnable() {

                        @Override
                        public void run() {
                            try {
                                final long start = System.nanoTime();
                                getAndLoadIfNotPresent(filterToWarm, ctx);
                                if (indexShard.warmerService().logger().isTraceEnabled()) {
                                    indexShard.warmerService().logger().trace("warmed fixed bitset for [{}], took [{}]", filterToWarm, TimeValue.timeValueNanos(System.nanoTime() - start));
                                }
                            } catch (Throwable t) {
                                indexShard.warmerService().logger().warn("failed to load fixed bitset for [{}]", t, filterToWarm);
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
