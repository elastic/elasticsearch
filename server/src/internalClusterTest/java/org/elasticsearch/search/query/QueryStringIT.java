/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.query;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponses;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class QueryStringIT extends ESIntegTestCase {

    @Before
    public void setup() throws Exception {
        String indexBody = copyToStringFromClasspath("/org/elasticsearch/search/query/all-query-index.json");
        prepareCreate("test").setSource(indexBody, XContentType.JSON).get();
        ensureGreen("test");
    }

    public void testBasicAllQuery() throws Exception {
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        reqs.add(prepareIndex("test").setId("1").setSource("f1", "foo bar baz"));
        reqs.add(prepareIndex("test").setId("2").setSource("f2", "Bar"));
        reqs.add(prepareIndex("test").setId("3").setSource("f3", "foo bar baz"));
        indexRandom(true, false, reqs);

        assertResponses(response -> {
            assertHitCount(response, 2L);
            assertHits(response.getHits(), "1", "3");
        }, prepareSearch("test").setQuery(queryStringQuery("foo")), prepareSearch("test").setQuery(queryStringQuery("bar")));
        assertResponse(prepareSearch("test").setQuery(queryStringQuery("Bar")), response -> {
            assertHitCount(response, 3L);
            assertHits(response.getHits(), "1", "2", "3");
        });
    }

    public void testWithDate() throws Exception {
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        reqs.add(prepareIndex("test").setId("1").setSource("f1", "foo", "f_date", "2015/09/02"));
        reqs.add(prepareIndex("test").setId("2").setSource("f1", "bar", "f_date", "2015/09/01"));
        indexRandom(true, false, reqs);

        assertResponses(response -> {
            assertHits(response.getHits(), "1", "2");
            assertHitCount(response, 2L);
        },
            prepareSearch("test").setQuery(queryStringQuery("foo bar")),
            prepareSearch("test").setQuery(queryStringQuery("bar \"2015/09/02\"")),
            prepareSearch("test").setQuery(queryStringQuery("\"2015/09/02\" \"2015/09/01\""))
        );
        assertResponse(prepareSearch("test").setQuery(queryStringQuery("\"2015/09/02\"")), response -> {
            assertHits(response.getHits(), "1");
            assertHitCount(response, 1L);
        });
    }

    public void testWithLotsOfTypes() throws Exception {
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        reqs.add(prepareIndex("test").setId("1").setSource("f1", "foo", "f_date", "2015/09/02", "f_float", "1.7", "f_ip", "127.0.0.1"));
        reqs.add(prepareIndex("test").setId("2").setSource("f1", "bar", "f_date", "2015/09/01", "f_float", "1.8", "f_ip", "127.0.0.2"));
        indexRandom(true, false, reqs);

        assertResponses(response -> {
            assertHits(response.getHits(), "1", "2");
            assertHitCount(response, 2L);
        },
            prepareSearch("test").setQuery(queryStringQuery("foo bar")),
            prepareSearch("test").setQuery(queryStringQuery("127.0.0.2 \"2015/09/02\"")),
            prepareSearch("test").setQuery(queryStringQuery("127.0.0.1 OR 1.8"))
        );
        assertResponse(prepareSearch("test").setQuery(queryStringQuery("\"2015/09/02\"")), response -> {
            assertHits(response.getHits(), "1");
            assertHitCount(response, 1L);
        });
    }

    public void testDocWithAllTypes() throws Exception {
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        String docBody = copyToStringFromClasspath("/org/elasticsearch/search/query/all-example-document.json");
        reqs.add(prepareIndex("test").setId("1").setSource(docBody, XContentType.JSON));
        indexRandom(true, false, reqs);

        assertResponses(
            response -> assertHits(response.getHits(), "1"),
            prepareSearch("test").setQuery(queryStringQuery("foo")),
            prepareSearch("test").setQuery(queryStringQuery("Bar")),
            prepareSearch("test").setQuery(queryStringQuery("Baz")),
            prepareSearch("test").setQuery(queryStringQuery("19")),
            // nested doesn't match because it's hidden
            prepareSearch("test").setQuery(queryStringQuery("1476383971")),
            // bool doesn't match
            prepareSearch("test").setQuery(queryStringQuery("7")),
            prepareSearch("test").setQuery(queryStringQuery("23")),
            prepareSearch("test").setQuery(queryStringQuery("1293")),
            prepareSearch("test").setQuery(queryStringQuery("42")),
            prepareSearch("test").setQuery(queryStringQuery("1.7")),
            prepareSearch("test").setQuery(queryStringQuery("1.5")),
            prepareSearch("test").setQuery(queryStringQuery("127.0.0.1"))
        );
    }

    public void testKeywordWithWhitespace() throws Exception {
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        reqs.add(prepareIndex("test").setId("1").setSource("f2", "Foo Bar"));
        reqs.add(prepareIndex("test").setId("2").setSource("f1", "bar"));
        reqs.add(prepareIndex("test").setId("3").setSource("f1", "foo bar"));
        indexRandom(true, false, reqs);

        assertResponse(prepareSearch("test").setQuery(queryStringQuery("foo")), response -> {
            assertHits(response.getHits(), "3");
            assertHitCount(response, 1L);
        });
        assertResponse(prepareSearch("test").setQuery(queryStringQuery("bar")), response -> {
            assertHits(response.getHits(), "2", "3");
            assertHitCount(response, 2L);
        });
        assertResponse(prepareSearch("test").setQuery(queryStringQuery("Foo Bar")), response -> {
            assertHits(response.getHits(), "1", "2", "3");
            assertHitCount(response, 3L);
        });
    }

    public void testAllFields() throws Exception {
        String indexBody = copyToStringFromClasspath("/org/elasticsearch/search/query/all-query-index.json");

        Settings.Builder settings = Settings.builder().put("index.query.default_field", "*");
        prepareCreate("test_1").setSource(indexBody, XContentType.JSON).setSettings(settings).get();
        ensureGreen("test_1");

        List<IndexRequestBuilder> reqs = new ArrayList<>();
        reqs.add(prepareIndex("test_1").setId("1").setSource("f1", "foo", "f2", "eggplant"));
        indexRandom(true, false, reqs);

        assertHitCount(prepareSearch("test_1").setQuery(queryStringQuery("foo eggplant").defaultOperator(Operator.AND)), 0L);

        assertResponse(prepareSearch("test_1").setQuery(queryStringQuery("foo eggplant").defaultOperator(Operator.OR)), response -> {
            assertHits(response.getHits(), "1");
            assertHitCount(response, 1L);
        });
    }

    public void testPhraseQueryOnFieldWithNoPositions() throws Exception {
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        reqs.add(prepareIndex("test").setId("1").setSource("f1", "foo bar", "f4", "eggplant parmesan"));
        reqs.add(prepareIndex("test").setId("2").setSource("f1", "foo bar", "f4", "chicken parmesan"));
        indexRandom(true, false, reqs);

        assertHitCount(prepareSearch("test").setQuery(queryStringQuery("\"eggplant parmesan\"").lenient(true)), 0L);

        Exception exc = expectThrows(
            Exception.class,
            prepareSearch("test").setQuery(queryStringQuery("f4:\"eggplant parmesan\"").lenient(false))
        );
        IllegalArgumentException iae = (IllegalArgumentException) ExceptionsHelper.unwrap(exc, IllegalArgumentException.class);
        assertNotNull(iae);
        assertThat(iae.getMessage(), containsString("field:[f4] was indexed without position data; cannot run PhraseQuery"));
    }

    public void testBooleanStrictQuery() throws Exception {
        Exception e = expectThrows(Exception.class, prepareSearch("test").setQuery(queryStringQuery("foo").field("f_bool")));
        assertThat(
            ExceptionsHelper.unwrap(e, IllegalArgumentException.class).getMessage(),
            containsString("Can't parse boolean value [foo], expected [true] or [false]")
        );
    }

    public void testAllFieldsWithSpecifiedLeniency() throws IOException {
        Exception e = expectThrows(
            Exception.class,
            prepareSearch("test").setQuery(queryStringQuery("f_date:[now-2D TO now]").lenient(false))
        );
        assertThat(e.getCause().getMessage(), containsString("unit [D] not supported for date math [-2D]"));
    }

    public void testFieldAlias() throws Exception {
        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        indexRequests.add(prepareIndex("test").setId("1").setSource("f3", "text", "f2", "one"));
        indexRequests.add(prepareIndex("test").setId("2").setSource("f3", "value", "f2", "two"));
        indexRequests.add(prepareIndex("test").setId("3").setSource("f3", "another value", "f2", "three"));
        indexRandom(true, false, indexRequests);

        assertNoFailuresAndResponse(prepareSearch("test").setQuery(queryStringQuery("value").field("f3_alias")), response -> {
            assertHitCount(response, 2);
            assertHits(response.getHits(), "2", "3");
        });
    }

    public void testFieldAliasWithEmbeddedFieldNames() throws Exception {
        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        indexRequests.add(prepareIndex("test").setId("1").setSource("f3", "text", "f2", "one"));
        indexRequests.add(prepareIndex("test").setId("2").setSource("f3", "value", "f2", "two"));
        indexRequests.add(prepareIndex("test").setId("3").setSource("f3", "another value", "f2", "three"));
        indexRandom(true, false, indexRequests);

        assertNoFailuresAndResponse(prepareSearch("test").setQuery(queryStringQuery("f3_alias:value AND f2:three")), response -> {
            assertHitCount(response, 1);
            assertHits(response.getHits(), "3");
        });
    }

    public void testFieldAliasWithWildcardField() throws Exception {
        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        indexRequests.add(prepareIndex("test").setId("1").setSource("f3", "text", "f2", "one"));
        indexRequests.add(prepareIndex("test").setId("2").setSource("f3", "value", "f2", "two"));
        indexRequests.add(prepareIndex("test").setId("3").setSource("f3", "another value", "f2", "three"));
        indexRandom(true, false, indexRequests);

        assertNoFailuresAndResponse(prepareSearch("test").setQuery(queryStringQuery("value").field("f3_*")), response -> {
            assertHitCount(response, 2);
            assertHits(response.getHits(), "2", "3");
        });
    }

    public void testFieldAliasOnDisallowedFieldType() throws Exception {
        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        indexRequests.add(prepareIndex("test").setId("1").setSource("f3", "text", "f2", "one"));
        indexRandom(true, false, indexRequests);

        // The wildcard field matches aliases for both a text and geo_point field.
        // By default, the geo_point field should be ignored when building the query.
        assertNoFailuresAndResponse(prepareSearch("test").setQuery(queryStringQuery("text").field("f*_alias")), response -> {
            assertHitCount(response, 1);
            assertHits(response.getHits(), "1");
        });
    }

    private void assertHits(SearchHits hits, String... ids) {
        assertThat(hits.getTotalHits().value(), equalTo((long) ids.length));
        Set<String> hitIds = new HashSet<>();
        for (SearchHit hit : hits.getHits()) {
            hitIds.add(hit.getId());
        }
        assertThat(hitIds, containsInAnyOrder(ids));
    }
}
