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

import org.apache.lucene.index.CompositeReaderContext;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.cache.query.DisabledQueryCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.subphase.DocValueFieldsFetchSubPhase;
import org.elasticsearch.search.fetch.subphase.FetchSourceSubPhase;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Base class for testing {@link Aggregator} implementations.
 * Provides helpers for constructing and searching an {@link Aggregator} implementation based on a provided
 * {@link AggregationBuilder} instance.
 */
public abstract class AggregatorTestCase extends ESTestCase {
    private List<Releasable> releasables = new ArrayList<>();

    protected <A extends Aggregator, B extends AggregationBuilder> A createAggregator(B aggregationBuilder,
                                                                                      IndexSearcher indexSearcher,
                                                                                      MappedFieldType... fieldTypes) throws IOException {
        IndexSettings indexSettings = new IndexSettings(
            IndexMetaData.builder("_index").settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .creationDate(System.currentTimeMillis())
                .build(),
            Settings.EMPTY
        );

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

        CircuitBreakerService circuitBreakerService = new NoneCircuitBreakerService();
        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.numberOfShards()).thenReturn(1);
        when(searchContext.searcher()).thenReturn(contextIndexSearcher);
        when(searchContext.bigArrays()).thenReturn(new MockBigArrays(Settings.EMPTY, circuitBreakerService));
        when(searchContext.fetchPhase())
            .thenReturn(new FetchPhase(Arrays.asList(new FetchSourceSubPhase(), new DocValueFieldsFetchSubPhase())));
        doAnswer(invocation -> {
            /* Store the releasables so we can release them at the end of the test case. This is important because aggregations don't
             * close their sub-aggregations. This is fairly similar to what the production code does. */
            releasables.add((Releasable) invocation.getArguments()[0]);
            return null;
        }).when(searchContext).addReleasable(anyObject(), anyObject());

        // TODO: now just needed for top_hits, this will need to be revised for other agg unit tests:
        MapperService mapperService = mapperServiceMock();
        when(mapperService.hasNested()).thenReturn(false);
        when(searchContext.mapperService()).thenReturn(mapperService);
        IndexFieldDataService ifds = new IndexFieldDataService(IndexSettingsModule.newIndexSettings("test", Settings.EMPTY),
                new IndicesFieldDataCache(Settings.EMPTY, new IndexFieldDataCache.Listener() {
                }), circuitBreakerService, mapperService);
        when(searchContext.fieldData()).thenReturn(ifds);

        SearchLookup searchLookup = new SearchLookup(mapperService, ifds, new String[]{"type"});
        when(searchContext.lookup()).thenReturn(searchLookup);

        QueryShardContext queryShardContext = mock(QueryShardContext.class);
        for (MappedFieldType fieldType : fieldTypes) {
            when(queryShardContext.fieldMapper(fieldType.name())).thenReturn(fieldType);
            when(queryShardContext.getForField(fieldType)).then(invocation -> fieldType.fielddataBuilder().build(
                    indexSettings, fieldType, new IndexFieldDataCache.None(), circuitBreakerService, mock(MapperService.class)));
            when(searchContext.getQueryShardContext()).thenReturn(queryShardContext);
        }

        @SuppressWarnings("unchecked")
        A aggregator = (A) aggregationBuilder.build(searchContext, null).create(null, true);
        return aggregator;
    }

    /**
     * sub-tests that need a more complex mock can overwrite this
     */
    protected MapperService mapperServiceMock() {
        return mock(MapperService.class);
    }

    protected <A extends InternalAggregation, C extends Aggregator> A search(IndexSearcher searcher,
                                                                             Query query,
                                                                             AggregationBuilder builder,
                                                                             MappedFieldType... fieldTypes) throws IOException {
        C a = createAggregator(builder, searcher, fieldTypes);
        try {
            a.preCollection();
            searcher.search(query, a);
            a.postCollection();
            @SuppressWarnings("unchecked")
            A internalAgg = (A) a.buildAggregation(0L);
            return internalAgg;
        } finally {
            Releasables.close(releasables);
            releasables.clear();
        }
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
        Weight weight = searcher.createWeight(rewritten, true);
        C root = createAggregator(builder, searcher, fieldTypes);
        try {
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
                if (randomBoolean()) {
                    // sometimes do an incremental reduce
                    List<InternalAggregation> internalAggregations = randomSubsetOf(randomIntBetween(1, aggs.size()), aggs);
                    A internalAgg = (A) aggs.get(0).doReduce(internalAggregations,
                        new InternalAggregation.ReduceContext(root.context().bigArrays(), null, false));
                    aggs.removeAll(internalAggregations);
                    aggs.add(internalAgg);
                }
                // now do the final reduce
                @SuppressWarnings("unchecked")
                A internalAgg = (A) aggs.get(0).doReduce(aggs, new InternalAggregation.ReduceContext(root.context().bigArrays(), null,
                    true));
                return internalAgg;
            }
        } finally {
            Releasables.close(releasables);
            releasables.clear();
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
}
