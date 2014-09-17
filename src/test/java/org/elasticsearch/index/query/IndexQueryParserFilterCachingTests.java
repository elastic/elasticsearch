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
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.elasticsearch.test.TestSearchContext;
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
        IndexService indexService = createIndex("test", settings);
        injector = indexService.injector();

        MapperService mapperService = indexService.mapperService();
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/query/mapping.json");
        mapperService.merge("person", new CompressedString(mapping), true);
        String childMapping = copyToStringFromClasspath("/org/elasticsearch/index/query/child-mapping.json");
        mapperService.merge("child", new CompressedString(childMapping), true);
        mapperService.documentMapper("person").parse(new BytesArray(copyToBytesFromClasspath("/org/elasticsearch/index/query/data.json")));
        queryParser = injector.getInstance(IndexQueryParserService.class);
    }

    private IndexQueryParserService queryParser() throws IOException {
        return this.queryParser;
    }

    /**
     * Runner to test our cache cases when using date range filter
     * @param lte could be null
     * @param gte could be null
     * @param forcedCache true if we want to force the cache, false if we want to force no cache, null either
     * @param expectedCache true if we expect a cached filter
     */
    private void testDateRangeFilterCache(IndexQueryParserService queryParser, Object gte, Object lte, Boolean forcedCache, boolean expectedCache) {
        RangeFilterBuilder filterBuilder = FilterBuilders.rangeFilter("born")
                .gte(gte)
                .lte(lte);
        if (forcedCache != null) {
            filterBuilder.cache(forcedCache);
        }

        Query parsedQuery = queryParser.parse(QueryBuilders.constantScoreQuery(filterBuilder)).query();
        assertThat(parsedQuery, instanceOf(ConstantScoreQuery.class));


        if (expectedCache) {
            if (((ConstantScoreQuery)parsedQuery).getFilter() instanceof CachedFilter) {
                logger.info("gte [{}], lte [{}], _cache [{}] is cached", gte, lte, forcedCache);
            } else {
                logger.warn("gte [{}], lte [{}], _cache [{}] should be cached", gte, lte, forcedCache);
            }
        } else {
            if (((ConstantScoreQuery)parsedQuery).getFilter() instanceof NoCacheFilter) {
                logger.info("gte [{}], lte [{}], _cache [{}] is not cached", gte, lte, forcedCache);
            } else {
                logger.warn("gte [{}], lte [{}], _cache [{}] should not be cached", gte, lte, forcedCache);
            }
        }

       if (expectedCache) {
            assertThat(((ConstantScoreQuery)parsedQuery).getFilter(), instanceOf(CachedFilter.class));
        } else {
            assertThat(((ConstantScoreQuery)parsedQuery).getFilter(), instanceOf(NoCacheFilter.class));
        }
    }

    /**
     * We test all possible combinations for range date filter cache
     */
    @Test
    public void testDateRangeFilterCache() throws IOException {
        IndexQueryParserService queryParser = queryParser();

        testDateRangeFilterCache(queryParser, null, null, null, true);
        testDateRangeFilterCache(queryParser, null, null, true, true);
        testDateRangeFilterCache(queryParser, null, null, false, false);
        testDateRangeFilterCache(queryParser, "now", null, null, false);
        testDateRangeFilterCache(queryParser, null, "now", null, false);
        testDateRangeFilterCache(queryParser, "now", "now", null, false);
        testDateRangeFilterCache(queryParser, "now/d", null, null, true);
        testDateRangeFilterCache(queryParser, null, "now/d", null, true);
        testDateRangeFilterCache(queryParser, "now/d", "now/d", null, true);
        testDateRangeFilterCache(queryParser, "2012-01-01", null, null, true);
        testDateRangeFilterCache(queryParser, null, "2012-01-01", null, true);
        testDateRangeFilterCache(queryParser, "2012-01-01", "2012-01-01", null, true);
        testDateRangeFilterCache(queryParser, "now", "2012-01-01", null, false);
        testDateRangeFilterCache(queryParser, "2012-01-01", "now", null, false);
        testDateRangeFilterCache(queryParser, "2012-01-01", "now/d", null, true);
        testDateRangeFilterCache(queryParser, "now/d", "2012-01-01", null, true);
        testDateRangeFilterCache(queryParser, null, 1577836800, null, true);
        testDateRangeFilterCache(queryParser, 1325376000, null, null, true);
        testDateRangeFilterCache(queryParser, 1325376000, 1577836800, null, true);
        testDateRangeFilterCache(queryParser, "now", 1577836800, null, false);
        testDateRangeFilterCache(queryParser, 1325376000, "now", null, false);
        testDateRangeFilterCache(queryParser, 1325376000, "now/d", null, true);
        testDateRangeFilterCache(queryParser, "now/d", 1577836800, null, true);
        testDateRangeFilterCache(queryParser, "now", null, true, false);
        testDateRangeFilterCache(queryParser, null, "now", true, false);
        testDateRangeFilterCache(queryParser, "now", "now", true, false);
        testDateRangeFilterCache(queryParser, "now/d", null, true, true);
        testDateRangeFilterCache(queryParser, null, "now/d", true, true);
        testDateRangeFilterCache(queryParser, "now/d", "now/d", true, true);
        testDateRangeFilterCache(queryParser, "2012-01-01", null, true, true);
        testDateRangeFilterCache(queryParser, null, "2012-01-01", true, true);
        testDateRangeFilterCache(queryParser, "2012-01-01", "2012-01-01", true, true);
        testDateRangeFilterCache(queryParser, "now", "2012-01-01", true, false);
        testDateRangeFilterCache(queryParser, "2012-01-01", "now", true, false);
        testDateRangeFilterCache(queryParser, "2012-01-01", "now/d", true, true);
        testDateRangeFilterCache(queryParser, "now/d", "2012-01-01", true, true);
        testDateRangeFilterCache(queryParser, null, 1577836800, true, true);
        testDateRangeFilterCache(queryParser, 1325376000, null, true, true);
        testDateRangeFilterCache(queryParser, 1325376000, 1577836800, true, true);
        testDateRangeFilterCache(queryParser, "now", 1577836800, true, false);
        testDateRangeFilterCache(queryParser, 1325376000, "now", true, false);
        testDateRangeFilterCache(queryParser, 1325376000, "now/d", true, true);
        testDateRangeFilterCache(queryParser, "now/d", 1577836800, true, true);
        testDateRangeFilterCache(queryParser, "now", null, false, false);
        testDateRangeFilterCache(queryParser, null, "now", false, false);
        testDateRangeFilterCache(queryParser, "now", "now", false, false);
        testDateRangeFilterCache(queryParser, "now/d", null, false, false);
        testDateRangeFilterCache(queryParser, null, "now/d", false, false);
        testDateRangeFilterCache(queryParser, "now/d", "now/d", false, false);
        testDateRangeFilterCache(queryParser, "2012-01-01", null, false, false);
        testDateRangeFilterCache(queryParser, null, "2012-01-01", false, false);
        testDateRangeFilterCache(queryParser, "2012-01-01", "2012-01-01", false, false);
        testDateRangeFilterCache(queryParser, "now", "2012-01-01", false, false);
        testDateRangeFilterCache(queryParser, "2012-01-01", "now", false, false);
        testDateRangeFilterCache(queryParser, "2012-01-01", "now/d", false, false);
        testDateRangeFilterCache(queryParser, "now/d", "2012-01-01", false, false);
        testDateRangeFilterCache(queryParser, null, 1577836800, false, false);
        testDateRangeFilterCache(queryParser, 1325376000, null, false, false);
        testDateRangeFilterCache(queryParser, 1325376000, 1577836800, false, false);
        testDateRangeFilterCache(queryParser, "now", 1577836800, false, false);
        testDateRangeFilterCache(queryParser, 1325376000, "now", false, false);
        testDateRangeFilterCache(queryParser, 1325376000, "now/d", false, false);
        testDateRangeFilterCache(queryParser, "now/d", 1577836800, false, false);
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

        query = copyToStringFromClasspath("/org/elasticsearch/index/query/date_range_in_boolean_with_long_value.json");
        parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(ConstantScoreQuery.class));
        assertThat(((ConstantScoreQuery) parsedQuery).getFilter(), instanceOf(XBooleanFilter.class));
        assertThat(((XBooleanFilter) ((ConstantScoreQuery) parsedQuery).getFilter()).clauses().get(1).getFilter(), instanceOf(CachedFilter.class));
        assertThat(((XBooleanFilter) ((ConstantScoreQuery) parsedQuery).getFilter()).clauses().size(), is(2));

        query = copyToStringFromClasspath("/org/elasticsearch/index/query/date_range_in_boolean_with_long_value_not_cached.json");
        parsedQuery = queryParser.parse(query).query();
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
