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

package org.elasticsearch.index.query;


import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.cache.recycler.CacheRecyclerModule;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.lucene.search.AndFilter;
import org.elasticsearch.common.lucene.search.CachedFilter;
import org.elasticsearch.common.lucene.search.NoCacheFilter;
import org.elasticsearch.common.lucene.search.XBooleanFilter;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNameModule;
import org.elasticsearch.index.analysis.AnalysisModule;
import org.elasticsearch.index.cache.IndexCacheModule;
import org.elasticsearch.index.codec.CodecModule;
import org.elasticsearch.index.engine.IndexEngineModule;
import org.elasticsearch.index.fielddata.IndexFieldDataModule;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceModule;
import org.elasticsearch.index.query.functionscore.FunctionScoreModule;
import org.elasticsearch.index.search.child.TestSearchContext;
import org.elasticsearch.index.settings.IndexSettingsModule;
import org.elasticsearch.index.similarity.SimilarityModule;
import org.elasticsearch.indices.fielddata.breaker.CircuitBreakerService;
import org.elasticsearch.indices.fielddata.breaker.DummyCircuitBreakerService;
import org.elasticsearch.indices.query.IndicesQueriesModule;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.index.service.StubIndexService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolModule;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.common.io.Streams.copyToBytesFromClasspath;
import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

/**
 *
 */
public class IndexQueryParserFilterCachingTests extends ElasticsearchTestCase {

    private static Injector injector;

    private static IndexQueryParserService queryParser;

    @BeforeClass
    public static void setupQueryParser() throws IOException {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("index.cache.filter.type", "weighted")
                .build();
        Index index = new Index("test");
        injector = new ModulesBuilder().add(
                new CacheRecyclerModule(settings),
                new CodecModule(settings),
                new SettingsModule(settings),
                new ThreadPoolModule(settings),
                new IndicesQueriesModule(),
                new ScriptModule(settings),
                new MapperServiceModule(),
                new IndexSettingsModule(index, settings),
                new IndexCacheModule(settings),
                new AnalysisModule(settings),
                new IndexEngineModule(settings),
                new SimilarityModule(settings),
                new IndexQueryParserModule(settings),
                new IndexNameModule(index),
                new FunctionScoreModule(),
                new IndexFieldDataModule(settings),
                new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(ClusterService.class).toProvider(Providers.of((ClusterService) null));
                        bind(CircuitBreakerService.class).to(DummyCircuitBreakerService.class);
                    }
                }
        ).createInjector();

        injector.getInstance(IndexFieldDataService.class).setIndexService((new StubIndexService(injector.getInstance(MapperService.class))));
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/query/mapping.json");
        injector.getInstance(MapperService.class).merge("person", new CompressedString(mapping), true);
        String childMapping = copyToStringFromClasspath("/org/elasticsearch/index/query/child-mapping.json");
        injector.getInstance(MapperService.class).merge("child", new CompressedString(childMapping), true);
        injector.getInstance(MapperService.class).documentMapper("person").parse(new BytesArray(copyToBytesFromClasspath("/org/elasticsearch/index/query/data.json")));
        queryParser = injector.getInstance(IndexQueryParserService.class);
    }

    @AfterClass
    public static void close() {
        injector.getInstance(ThreadPool.class).shutdownNow();
        queryParser = null;
        injector = null;
    }

    private IndexQueryParserService queryParser() throws IOException {
        return this.queryParser;
    }

    private BytesRef longToPrefixCoded(long val, int shift) {
        BytesRef bytesRef = new BytesRef();
        NumericUtils.longToPrefixCoded(val, shift, bytesRef);
        return bytesRef;
    }


    @Test
    public void testNoFilterParsing() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/date_range_in_boolean.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(ConstantScoreQuery.class));
        assertThat(((ConstantScoreQuery)parsedQuery).getFilter(), instanceOf(XBooleanFilter.class));
        assertThat(((XBooleanFilter)((ConstantScoreQuery)parsedQuery).getFilter()).clauses().get(1).getFilter(), instanceOf(NoCacheFilter.class));
        assertThat(((XBooleanFilter)((ConstantScoreQuery)parsedQuery).getFilter()).clauses().size(), is(2));

        query = copyToStringFromClasspath("/org/elasticsearch/index/query/date_range_in_boolean_cached_now.json");
        parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(ConstantScoreQuery.class));
        assertThat(((ConstantScoreQuery)parsedQuery).getFilter(), instanceOf(XBooleanFilter.class));
        assertThat(((XBooleanFilter)((ConstantScoreQuery)parsedQuery).getFilter()).clauses().get(1).getFilter(), instanceOf(NoCacheFilter.class));
        assertThat(((XBooleanFilter)((ConstantScoreQuery)parsedQuery).getFilter()).clauses().size(), is(2));

        query = copyToStringFromClasspath("/org/elasticsearch/index/query/date_range_in_boolean_cached_complex_now.json");
        parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(ConstantScoreQuery.class));
        assertThat(((ConstantScoreQuery) parsedQuery).getFilter(), instanceOf(XBooleanFilter.class));
        assertThat(((XBooleanFilter) ((ConstantScoreQuery) parsedQuery).getFilter()).clauses().get(1).getFilter(), instanceOf(NoCacheFilter.class));
        assertThat(((XBooleanFilter) ((ConstantScoreQuery) parsedQuery).getFilter()).clauses().size(), is(2));

        query = copyToStringFromClasspath("/org/elasticsearch/index/query/date_range_in_boolean_cached.json");
        parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(ConstantScoreQuery.class));
        assertThat(((ConstantScoreQuery) parsedQuery).getFilter(), instanceOf(CachedFilter.class));

        query = copyToStringFromClasspath("/org/elasticsearch/index/query/date_range_in_boolean_cached_now_with_rounding.json");
        parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(ConstantScoreQuery.class));
        assertThat(((ConstantScoreQuery) parsedQuery).getFilter(), instanceOf(CachedFilter.class));

        query = copyToStringFromClasspath("/org/elasticsearch/index/query/date_range_in_boolean_cached_complex_now_with_rounding.json");
        parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(ConstantScoreQuery.class));
        assertThat(((ConstantScoreQuery) parsedQuery).getFilter(), instanceOf(CachedFilter.class));

        try {
            SearchContext.setCurrent(new TestSearchContext());
            query = copyToStringFromClasspath("/org/elasticsearch/index/query/has-child.json");
            parsedQuery = queryParser.parse(query).query();
            assertThat(parsedQuery, instanceOf(ConstantScoreQuery.class));
            assertThat(((ConstantScoreQuery) parsedQuery).getFilter(), instanceOf(NoCacheFilter.class));

            query = copyToStringFromClasspath("/org/elasticsearch/index/query/and-filter-cache.json");
            parsedQuery = queryParser.parse(query).query();
            assertThat(parsedQuery, instanceOf(ConstantScoreQuery.class));
            assertThat(((ConstantScoreQuery) parsedQuery).getFilter(), instanceOf(CachedFilter.class));

            query = copyToStringFromClasspath("/org/elasticsearch/index/query/has-child-in-and-filter-cached.json");
            parsedQuery = queryParser.parse(query).query();
            assertThat(parsedQuery, instanceOf(ConstantScoreQuery.class));
            assertThat(((ConstantScoreQuery) parsedQuery).getFilter(), instanceOf(AndFilter.class));
        } finally {
            SearchContext.removeCurrent();
        }
    }

}
