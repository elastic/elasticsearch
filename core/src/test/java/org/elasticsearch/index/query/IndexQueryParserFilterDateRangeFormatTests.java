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


import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.TestSearchContext;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.test.StreamsUtils.copyToBytesFromClasspath;
import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

/**
 *
 */
public class IndexQueryParserFilterDateRangeFormatTests extends ESSingleNodeTestCase {

    private Injector injector;
    private IndexQueryParserService queryParser;

    @Before
    public void setup() throws IOException {
        IndexService indexService = createIndex("test");
        injector = indexService.injector();

        MapperService mapperService = indexService.mapperService();
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/query/mapping.json");
        mapperService.merge("person", new CompressedXContent(mapping), true, false);
        ParsedDocument doc = mapperService.documentMapper("person").parse("test", "person", "1", new BytesArray(copyToBytesFromClasspath("/org/elasticsearch/index/query/data.json")));
        assertNotNull(doc.dynamicMappingsUpdate());
        client().admin().indices().preparePutMapping("test").setType("person").setSource(doc.dynamicMappingsUpdate().toString()).get();
        queryParser = injector.getInstance(IndexQueryParserService.class);
    }

    private IndexQueryParserService queryParser() throws IOException {
        return this.queryParser;
    }

    @Test
    public void testDateRangeFilterFormat() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/date_range_filter_format.json");
        queryParser.parse(query).query();
        // Sadly from NoCacheFilter, we can not access to the delegate filter so we can not check
        // it's the one we are expecting

        // Test Invalid format
        query = copyToStringFromClasspath("/org/elasticsearch/index/query/date_range_filter_format_invalid.json");
        try {
            SearchContext.setCurrent(new TestSearchContext());
            // We need to rewrite, because range on date field initially returns LateParsingQuery
            queryParser.parse(query).query().rewrite(null);
            fail("A Range Filter with a specific format but with an unexpected date should raise a QueryParsingException");
        } catch (ElasticsearchParseException e) {
            // We expect it
        } finally {
            SearchContext.removeCurrent();
        }
    }

    @Test
    public void testDateRangeQueryFormat() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        // We test 01/01/2012 from gte and 2030 for lt
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/date_range_query_format.json");
        Query parsedQuery;
        try {
            SearchContext.setCurrent(new TestSearchContext());
            // We need to rewrite, because range on date field initially returns LateParsingQuery
            parsedQuery = queryParser.parse(query).query().rewrite(null);
        } finally {
            SearchContext.removeCurrent();;
        }
        assertThat(parsedQuery, instanceOf(NumericRangeQuery.class));

        // Min value was 01/01/2012 (dd/MM/yyyy)
        DateTime min = DateTime.parse("2012-01-01T00:00:00.000+00");
        assertThat(((NumericRangeQuery) parsedQuery).getMin().longValue(), is(min.getMillis()));

        // Max value was 2030 (yyyy)
        DateTime max = DateTime.parse("2030-01-01T00:00:00.000+00");
        assertThat(((NumericRangeQuery) parsedQuery).getMax().longValue(), is(max.getMillis()));

        // Test Invalid format
        query = copyToStringFromClasspath("/org/elasticsearch/index/query/date_range_query_format_invalid.json");
        try {
            SearchContext.setCurrent(new TestSearchContext());
            queryParser.parse(query).query().rewrite(null);
            fail("A Range Query with a specific format but with an unexpected date should raise a QueryParsingException");
        } catch (ElasticsearchParseException e) {
            // We expect it
        } finally {
            SearchContext.removeCurrent();
        }
    }

    @Test
    public void testDateRangeBoundaries() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/date_range_query_boundaries_inclusive.json");
        Query parsedQuery;
        try {
            SearchContext.setCurrent(new TestSearchContext());
            // We need to rewrite, because range on date field initially returns LateParsingQuery
            parsedQuery = queryParser.parse(query).query().rewrite(null);
        } finally {
            SearchContext.removeCurrent();
        }
        assertThat(parsedQuery, instanceOf(NumericRangeQuery.class));
        NumericRangeQuery rangeQuery = (NumericRangeQuery) parsedQuery;

        DateTime min = DateTime.parse("2014-11-01T00:00:00.000+00");
        assertThat(rangeQuery.getMin().longValue(), is(min.getMillis()));
        assertTrue(rangeQuery.includesMin());

        DateTime max = DateTime.parse("2014-12-08T23:59:59.999+00");
        assertThat(rangeQuery.getMax().longValue(), is(max.getMillis()));
        assertTrue(rangeQuery.includesMax());

        query = copyToStringFromClasspath("/org/elasticsearch/index/query/date_range_query_boundaries_exclusive.json");
        try {
            SearchContext.setCurrent(new TestSearchContext());
            // We need to rewrite, because range on date field initially returns LateParsingQuery
            parsedQuery = queryParser.parse(query).query().rewrite(null);
        } finally {
            SearchContext.removeCurrent();
        }
        assertThat(parsedQuery, instanceOf(NumericRangeQuery.class));
        rangeQuery = (NumericRangeQuery) parsedQuery;

        min = DateTime.parse("2014-11-30T23:59:59.999+00");
        assertThat(rangeQuery.getMin().longValue(), is(min.getMillis()));
        assertFalse(rangeQuery.includesMin());

        max = DateTime.parse("2014-12-08T00:00:00.000+00");
        assertThat(rangeQuery.getMax().longValue(), is(max.getMillis()));
        assertFalse(rangeQuery.includesMax());
    }
}
