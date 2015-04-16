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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.elasticsearch.test.TestSearchContext;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.common.io.Streams.copyToBytesFromClasspath;
import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 *
 */
public class IndexQueryParserFilterDateRangeTimezoneTests extends ElasticsearchSingleNodeTest {

    private Injector injector;
    private IndexQueryParserService queryParser;

    @Before
    public void setup() throws IOException {
        IndexService indexService = createIndex("test");
        injector = indexService.injector();

        MapperService mapperService = indexService.mapperService();
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/query/mapping.json");
        mapperService.merge("person", new CompressedString(mapping), true);
        ParsedDocument doc = mapperService.documentMapper("person").parse(new BytesArray(copyToBytesFromClasspath("/org/elasticsearch/index/query/data.json")));
        assertNotNull(doc.dynamicMappingsUpdate());
        client().admin().indices().preparePutMapping("test").setType("person").setSource(doc.dynamicMappingsUpdate().toString()).get();
        queryParser = injector.getInstance(IndexQueryParserService.class);
    }

    private IndexQueryParserService queryParser() throws IOException {
        return this.queryParser;
    }

    @Test
    public void testDateRangeFilterTimezone() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/date_range_filter_timezone.json");
        queryParser.parse(query).query();
        // Sadly from NoCacheFilter, we can not access to the delegate filter so we can not check
        // it's the one we are expecting

        query = copyToStringFromClasspath("/org/elasticsearch/index/query/date_range_filter_timezone_numeric_field.json");
        try {
            SearchContext.setCurrent(new TestSearchContext());
            queryParser.parse(query).query();
            fail("A Range Filter on a numeric field with a TimeZone should raise a QueryParsingException");
        } catch (QueryParsingException e) {
            // We expect it
        } finally {
            SearchContext.removeCurrent();
        }
    }

    @Test
    public void testDateRangeQueryTimezone() throws IOException {
        long startDate = System.currentTimeMillis();

        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/date_range_query_timezone.json");
        Query parsedQuery;
        try {
            SearchContext.setCurrent(new TestSearchContext());
            parsedQuery = queryParser.parse(query).query();
        } finally {
            SearchContext.removeCurrent();
        }
        assertThat(parsedQuery, instanceOf(NumericRangeQuery.class));

        // Min value was 2012-01-01 (UTC) so we need to remove one hour
        DateTime min = DateTime.parse("2012-01-01T00:00:00.000+01:00");
        // Max value is when we started the test. So it should be some ms from now
        DateTime max = new DateTime(startDate);

        assertThat(((NumericRangeQuery) parsedQuery).getMin().longValue(), is(min.getMillis()));

        // We should not have a big difference here (should be some ms)
        assertThat(((NumericRangeQuery) parsedQuery).getMax().longValue() - max.getMillis(), lessThanOrEqualTo(60000L));

        query = copyToStringFromClasspath("/org/elasticsearch/index/query/date_range_query_timezone_numeric_field.json");
        try {
            SearchContext.setCurrent(new TestSearchContext());
            queryParser.parse(query).query();
            fail("A Range Query on a numeric field with a TimeZone should raise a QueryParsingException");
        } catch (QueryParsingException e) {
            // We expect it
        } finally {
            SearchContext.removeCurrent();
        }
    }
}
