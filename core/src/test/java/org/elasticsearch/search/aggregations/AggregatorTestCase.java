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

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.search.QueryCachingPolicy;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.cache.query.DisabledQueryCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.subphase.DocValueFieldsFetchSubPhase;
import org.elasticsearch.search.fetch.subphase.FetchSourceSubPhase;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Base class for testing {@link Aggregator} implementations.
 * Provides a helper constructing the {@link Aggregator} implementation based on a provided {@link AggregationBuilder} instance.
 */
public abstract class AggregatorTestCase extends ESTestCase {

    protected <A extends Aggregator, B extends AggregationBuilder> A createAggregator(B aggregationBuilder,
                                                                                      MappedFieldType fieldType,
                                                                                      IndexSearcher indexSearcher) throws IOException {
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
        when(searchContext.searcher()).thenReturn(contextIndexSearcher);
        when(searchContext.parseFieldMatcher()).thenReturn(ParseFieldMatcher.STRICT);
        when(searchContext.bigArrays()).thenReturn(new MockBigArrays(Settings.EMPTY, circuitBreakerService));
        when(searchContext.fetchPhase())
            .thenReturn(new FetchPhase(Arrays.asList(new FetchSourceSubPhase(), new DocValueFieldsFetchSubPhase())));

        // TODO: now just needed for top_hits, this will need to be revised for other agg unit tests:
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.hasNested()).thenReturn(false);
        when(searchContext.mapperService()).thenReturn(mapperService);

        SearchLookup searchLookup = new SearchLookup(mapperService, mock(IndexFieldDataService.class), new String[]{"type"});
        when(searchContext.lookup()).thenReturn(searchLookup);

        QueryShardContext queryShardContext = mock(QueryShardContext.class);
        IndexFieldData<?> fieldData = fieldType.fielddataBuilder().build(indexSettings, fieldType, null, circuitBreakerService,
                mock(MapperService.class));
        when(queryShardContext.fieldMapper(anyString())).thenReturn(fieldType);
        when(queryShardContext.getForField(any())).thenReturn(fieldData);
        when(searchContext.getQueryShardContext()).thenReturn(queryShardContext);

        @SuppressWarnings("unchecked")
        A aggregator = (A) aggregationBuilder.build(searchContext, null).create(null, true);
        return aggregator;
    }

}
