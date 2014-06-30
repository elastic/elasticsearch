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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.lucene.search.AndFilter;
import org.elasticsearch.common.lucene.search.CachedFilter;
import org.elasticsearch.common.lucene.search.NoCacheFilter;
import org.elasticsearch.common.lucene.search.XBooleanFilter;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.search.child.TestSearchContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.elasticsearch.test.index.service.StubIndexService;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.common.io.Streams.copyToBytesFromClasspath;
import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

/**
 *
 */
public class IndexQueryParserFilterCachingTests extends ElasticsearchSingleNodeTest {

    private Injector injector;
    private IndexQueryParserService queryParser;

    @Before
    public void setup() throws IOException {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("index.cache.filter.type", "weighted")
                .put("name", "IndexQueryParserFilterCachingTests")
                .build();
        injector = createIndex("test", settings).injector();

        injector.getInstance(IndexFieldDataService.class).setIndexService((new StubIndexService(injector.getInstance(MapperService.class))));
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/query/mapping.json");
        injector.getInstance(MapperService.class).merge("person", new CompressedString(mapping), true);
        String childMapping = copyToStringFromClasspath("/org/elasticsearch/index/query/child-mapping.json");
        injector.getInstance(MapperService.class).merge("child", new CompressedString(childMapping), true);
        injector.getInstance(MapperService.class).documentMapper("person").parse(new BytesArray(copyToBytesFromClasspath("/org/elasticsearch/index/query/data.json")));
        queryParser = injector.getInstance(IndexQueryParserService.class);
    }

    private IndexQueryParserService queryParser() throws IOException {
        return this.queryParser;
    }

    @Test
    public void testNoFilterParsing() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/date_range_in_boolean.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(ConstantScoreQuery.class));
        assertThat(((ConstantScoreQuery) parsedQuery).getFilter(), instanceOf(XBooleanFilter.class));
        assertThat(((XBooleanFilter) ((ConstantScoreQuery) parsedQuery).getFilter()).clauses().get(1).getFilter(), instanceOf(NoCacheFilter.class));
        assertThat(((XBooleanFilter) ((ConstantScoreQuery) parsedQuery).getFilter()).clauses().size(), is(2));

        query = copyToStringFromClasspath("/org/elasticsearch/index/query/date_range_in_boolean_cached_now.json");
        parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(ConstantScoreQuery.class));
        assertThat(((ConstantScoreQuery) parsedQuery).getFilter(), instanceOf(XBooleanFilter.class));
        assertThat(((XBooleanFilter) ((ConstantScoreQuery) parsedQuery).getFilter()).clauses().get(1).getFilter(), instanceOf(NoCacheFilter.class));
        assertThat(((XBooleanFilter) ((ConstantScoreQuery) parsedQuery).getFilter()).clauses().size(), is(2));

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
