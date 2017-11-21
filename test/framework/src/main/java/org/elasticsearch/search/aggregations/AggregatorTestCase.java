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
package org.elasticsearch.search.aggregations;

import org.apache.lucene.index.AssertingDirectoryReader;
import org.apache.lucene.index.CompositeReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.AssertingIndexSearcher;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.Weight;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache.Listener;
import org.elasticsearch.index.cache.query.DisabledQueryCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper.BuilderContext;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.ObjectMapper.Nested;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.support.NestedScope;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.mock.orig.Mockito;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.subphase.DocValueFieldsFetchSubPhase;
import org.elasticsearch.search.fetch.subphase.FetchSourceSubPhase;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Base class for testing {@link Aggregator} implementations.
 * Provides helpers for constructing and searching an {@link Aggregator} implementation based on a provided
 * {@link AggregationBuilder} instance.
 */
public abstract class AggregatorTestCase extends ESTestCase {
    private static final String NESTEDFIELD_PREFIX = "nested_";
    private List<Releasable> releasables = new ArrayList<>();
    private static final String TYPE_NAME = "type";

    protected AggregatorFactory<?> createAggregatorFactory(AggregationBuilder aggregationBuilder,
                                                           IndexSearcher indexSearcher,
                                                           MappedFieldType... fieldTypes) throws IOException {
        return createAggregatorFactory(aggregationBuilder, indexSearcher, createIndexSettings(), fieldTypes);
    }

    /** Create a factory for the given aggregation builder. */
    protected AggregatorFactory<?> createAggregatorFactory(AggregationBuilder aggregationBuilder,
                                                           IndexSearcher indexSearcher,
                                                           IndexSettings indexSettings,
                                                           MappedFieldType... fieldTypes) throws IOException {
        SearchContext searchContext = createSearchContext(indexSearcher, indexSettings);
        CircuitBreakerService circuitBreakerService = new NoneCircuitBreakerService();
        when(searchContext.bigArrays()).thenReturn(new MockBigArrays(Settings.EMPTY, circuitBreakerService));
        // TODO: now just needed for top_hits, this will need to be revised for other agg unit tests:
        MapperService mapperService = mapperServiceMock();
        when(mapperService.getIndexSettings()).thenReturn(indexSettings);
        when(mapperService.hasNested()).thenReturn(false);
        when(mapperService.types()).thenReturn(Collections.singleton(TYPE_NAME));
        when(searchContext.mapperService()).thenReturn(mapperService);
        IndexFieldDataService ifds = new IndexFieldDataService(indexSettings,
                new IndicesFieldDataCache(Settings.EMPTY, new IndexFieldDataCache.Listener() {
                }), circuitBreakerService, mapperService);
        when(searchContext.getForField(Mockito.any(MappedFieldType.class))).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                return ifds.getForField((MappedFieldType) invocationOnMock.getArguments()[0]);
            }
        });

        SearchLookup searchLookup = new SearchLookup(mapperService, ifds::getForField, new String[]{TYPE_NAME});
        when(searchContext.lookup()).thenReturn(searchLookup);

        QueryShardContext queryShardContext = queryShardContextMock(mapperService, fieldTypes, circuitBreakerService);
        when(queryShardContext.getIndexSettings()).thenReturn(indexSettings);
        when(searchContext.getQueryShardContext()).thenReturn(queryShardContext);
        for (MappedFieldType fieldType : fieldTypes) {
            when(searchContext.smartNameFieldType(fieldType.name())).thenReturn(fieldType);
        }

        return aggregationBuilder.build(searchContext, null);
    }

    protected <A extends Aggregator> A createAggregator(AggregationBuilder aggregationBuilder,
                                                        IndexSearcher indexSearcher,
                                                        MappedFieldType... fieldTypes) throws IOException {
        return createAggregator(aggregationBuilder, indexSearcher, createIndexSettings(), fieldTypes);
    }

    protected <A extends Aggregator> A createAggregator(AggregationBuilder aggregationBuilder,
                                                        IndexSearcher indexSearcher,
                                                        IndexSettings indexSettings,
                                                        MappedFieldType... fieldTypes) throws IOException {
        @SuppressWarnings("unchecked")
        A aggregator = (A) createAggregatorFactory(aggregationBuilder, indexSearcher, indexSettings, fieldTypes)
            .create(null, true);
        return aggregator;
    }

    protected SearchContext createSearchContext(IndexSearcher indexSearcher, IndexSettings indexSettings) {
        Engine.Searcher searcher = new Engine.Searcher("aggregator_test", indexSearcher);
        QueryCache queryCache = new DisabledQueryCache(indexSettings);
        QueryCachingPolicy queryCachingPolicy = new QueryCachingPolicy() {
            @Override
            public void onUse(Query query) {
            }

            @Override
            public boolean shouldCache(Query query) throws IOException {
                // never cache a query
                return false;
            }
        };
        ContextIndexSearcher contextIndexSearcher = new ContextIndexSearcher(searcher, queryCache, queryCachingPolicy);

        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.numberOfShards()).thenReturn(1);
        when(searchContext.searcher()).thenReturn(contextIndexSearcher);
        when(searchContext.fetchPhase())
                .thenReturn(new FetchPhase(Arrays.asList(new FetchSourceSubPhase(), new DocValueFieldsFetchSubPhase())));
        when(searchContext.getObjectMapper(anyString())).thenAnswer(invocation -> {
            String fieldName = (String) invocation.getArguments()[0];
            if (fieldName.startsWith(NESTEDFIELD_PREFIX)) {
                BuilderContext context = new BuilderContext(indexSettings.getSettings(), new ContentPath());
                return new ObjectMapper.Builder<>(fieldName).nested(Nested.newNested(false, false)).build(context);
            }
            return null;
        });
        when(searchContext.bitsetFilterCache()).thenReturn(new BitsetFilterCache(indexSettings, mock(Listener.class)));
        doAnswer(invocation -> {
            /* Store the releasables so we can release them at the end of the test case. This is important because aggregations don't
             * close their sub-aggregations. This is fairly similar to what the production code does. */
            releasables.add((Releasable) invocation.getArguments()[0]);
            return null;
        }).when(searchContext).addReleasable(anyObject(), anyObject());
        return searchContext;
    }

    protected IndexSettings createIndexSettings() {
        return new IndexSettings(
                IndexMetaData.builder("_index").settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
                        .numberOfShards(1)
                        .numberOfReplicas(0)
                        .creationDate(System.currentTimeMillis())
                        .build(),
                Settings.EMPTY
        );
    }

    /**
     * sub-tests that need a more complex mock can overwrite this
     */
    protected MapperService mapperServiceMock() {
        return mock(MapperService.class);
    }

    /**
     * sub-tests that need a more complex mock can overwrite this
     */
    protected QueryShardContext queryShardContextMock(MapperService mapperService, MappedFieldType[] fieldTypes,
            CircuitBreakerService circuitBreakerService) {
        QueryShardContext queryShardContext = mock(QueryShardContext.class);
        when(queryShardContext.getMapperService()).thenReturn(mapperService);
        for (MappedFieldType fieldType : fieldTypes) {
            when(queryShardContext.fieldMapper(fieldType.name())).thenReturn(fieldType);
            when(queryShardContext.getForField(fieldType)).then(invocation -> fieldType.fielddataBuilder(mapperService.getIndexSettings()
                .getIndex().getName())
                .build(mapperService.getIndexSettings(), fieldType, new IndexFieldDataCache.None(), circuitBreakerService, mapperService));
        }
        NestedScope nestedScope = new NestedScope();
        when(queryShardContext.isFilter()).thenCallRealMethod();
        Mockito.doCallRealMethod().when(queryShardContext).setIsFilter(Matchers.anyBoolean());
        when(queryShardContext.nestedScope()).thenReturn(nestedScope);
        return queryShardContext;
    }

    protected <A extends InternalAggregation, C extends Aggregator> A search(IndexSearcher searcher,
                                                                             Query query,
                                                                             AggregationBuilder builder,
                                                                             MappedFieldType... fieldTypes) throws IOException {
        return search(searcher, query, builder, createIndexSettings(), fieldTypes);
    }

    protected <A extends InternalAggregation, C extends Aggregator> A search(IndexSearcher searcher,
                                                                             Query query,
                                                                             AggregationBuilder builder,
                                                                             IndexSettings indexSettings,
                                                                             MappedFieldType... fieldTypes) throws IOException {
        C a = createAggregator(builder, searcher, fieldTypes);
        a.preCollection();
        searcher.search(query, a);
        a.postCollection();
        @SuppressWarnings("unchecked")
        A internalAgg = (A) a.buildAggregation(0L);
        return internalAgg;

    }

    /**
     * Divides the provided {@link IndexSearcher} in sub-searcher, one for each segment,
     * builds an aggregator for each sub-searcher filtered by the provided {@link Query} and
     * returns the reduced {@link InternalAggregation}.
     */
    protected <A extends InternalAggregation, C extends Aggregator> A searchAndReduce(IndexSearcher searcher,
                                                                                      Query query,
                                                                                      AggregationBuilder builder,
                                                                                      MappedFieldType... fieldTypes) throws IOException {
        final IndexReaderContext ctx = searcher.getTopReaderContext();

        final ShardSearcher[] subSearchers;
        if (ctx instanceof LeafReaderContext) {
            subSearchers = new ShardSearcher[1];
            subSearchers[0] = new ShardSearcher((LeafReaderContext) ctx, ctx);
        } else {
            final CompositeReaderContext compCTX = (CompositeReaderContext) ctx;
            final int size = compCTX.leaves().size();
            subSearchers = new ShardSearcher[size];
            for(int searcherIDX=0;searcherIDX<subSearchers.length;searcherIDX++) {
                final LeafReaderContext leave = compCTX.leaves().get(searcherIDX);
                subSearchers[searcherIDX] = new ShardSearcher(leave, compCTX);
            }
        }

        List<InternalAggregation> aggs = new ArrayList<> ();
        Query rewritten = searcher.rewrite(query);
        Weight weight = searcher.createWeight(rewritten, true, 1f);
        C root = createAggregator(builder, searcher, fieldTypes);

        for (ShardSearcher subSearcher : subSearchers) {
            C a = createAggregator(builder, subSearcher, fieldTypes);
            a.preCollection();
            subSearcher.search(weight, a);
            a.postCollection();
            aggs.add(a.buildAggregation(0L));
        }
        if (aggs.isEmpty()) {
            return null;
        } else {
            if (randomBoolean() && aggs.size() > 1) {
                // sometimes do an incremental reduce
                int toReduceSize = aggs.size();
                Collections.shuffle(aggs, random());
                int r = randomIntBetween(1, toReduceSize);
                List<InternalAggregation> toReduce = aggs.subList(0, r);
                A reduced = (A) aggs.get(0).doReduce(toReduce,
                    new InternalAggregation.ReduceContext(root.context().bigArrays(), null, false));
                aggs = new ArrayList<>(aggs.subList(r, toReduceSize));
                aggs.add(reduced);
            }
            // now do the final reduce
            @SuppressWarnings("unchecked")
            A internalAgg = (A) aggs.get(0).doReduce(aggs, new InternalAggregation.ReduceContext(root.context().bigArrays(), null,
                true));
            return internalAgg;
        }

    }

    private static class ShardSearcher extends IndexSearcher {
        private final List<LeafReaderContext> ctx;

        ShardSearcher(LeafReaderContext ctx, IndexReaderContext parent) {
            super(parent);
            this.ctx = Collections.singletonList(ctx);
        }

        public void search(Weight weight, Collector collector) throws IOException {
            search(ctx, weight, collector);
        }

        @Override
        public String toString() {
            return "ShardSearcher(" + ctx.get(0) + ")";
        }
    }

    protected static DirectoryReader wrap(DirectoryReader directoryReader) throws IOException {
        return ElasticsearchDirectoryReader.wrap(directoryReader, new ShardId(new Index("_index", "_na_"), 0));
    }

    /**
     * Added to randomly run with more assertions on the index searcher level,
     * like {@link org.apache.lucene.util.LuceneTestCase#newSearcher(IndexReader)}, which can't be used because it also
     * wraps in the IndexSearcher's IndexReader with other implementations that we can't handle. (e.g. ParallelCompositeReader)
     */
    protected static IndexSearcher newIndexSearcher(IndexReader indexReader) {
        if (randomBoolean()) {
            return new AssertingIndexSearcher(random(), indexReader);
        } else {
            return new IndexSearcher(indexReader);
        }
    }

    /**
     * Added to randomly run with more assertions on the index reader level,
     * like {@link org.apache.lucene.util.LuceneTestCase#wrapReader(IndexReader)}, which can't be used because it also
     * wraps in the IndexReader with other implementations that we can't handle. (e.g. ParallelCompositeReader)
     */
    protected static IndexReader maybeWrapReaderEs(DirectoryReader reader) throws IOException {
        if (randomBoolean()) {
            return new AssertingDirectoryReader(reader);
        } else {
            return reader;
        }
    }

    @After
    private void cleanupReleasables() {
        Releasables.close(releasables);
        releasables.clear();
    }
}
