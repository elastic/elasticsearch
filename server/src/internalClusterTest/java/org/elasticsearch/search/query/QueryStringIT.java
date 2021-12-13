/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.query;

import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
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
        reqs.add(client().prepareIndex("test").setId("1").setSource("f1", "foo bar baz"));
        reqs.add(client().prepareIndex("test").setId("2").setSource("f2", "Bar"));
        reqs.add(client().prepareIndex("test").setId("3").setSource("f3", "foo bar baz"));
        indexRandom(true, false, reqs);

        SearchResponse resp = client().prepareSearch("test").setQuery(queryStringQuery("foo")).get();
        assertHitCount(resp, 2L);
        assertHits(resp.getHits(), "1", "3");

        resp = client().prepareSearch("test").setQuery(queryStringQuery("bar")).get();
        assertHitCount(resp, 2L);
        assertHits(resp.getHits(), "1", "3");

        resp = client().prepareSearch("test").setQuery(queryStringQuery("Bar")).get();
        assertHitCount(resp, 3L);
        assertHits(resp.getHits(), "1", "2", "3");
    }

    public void testWithDate() throws Exception {
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        reqs.add(client().prepareIndex("test").setId("1").setSource("f1", "foo", "f_date", "2015/09/02"));
        reqs.add(client().prepareIndex("test").setId("2").setSource("f1", "bar", "f_date", "2015/09/01"));
        indexRandom(true, false, reqs);

        SearchResponse resp = client().prepareSearch("test").setQuery(queryStringQuery("foo bar")).get();
        assertHits(resp.getHits(), "1", "2");
        assertHitCount(resp, 2L);

        resp = client().prepareSearch("test").setQuery(queryStringQuery("\"2015/09/02\"")).get();
        assertHits(resp.getHits(), "1");
        assertHitCount(resp, 1L);

        resp = client().prepareSearch("test").setQuery(queryStringQuery("bar \"2015/09/02\"")).get();
        assertHits(resp.getHits(), "1", "2");
        assertHitCount(resp, 2L);

        resp = client().prepareSearch("test").setQuery(queryStringQuery("\"2015/09/02\" \"2015/09/01\"")).get();
        assertHits(resp.getHits(), "1", "2");
        assertHitCount(resp, 2L);
    }

    public void testWithLotsOfTypes() throws Exception {
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        reqs.add(
            client().prepareIndex("test").setId("1").setSource("f1", "foo", "f_date", "2015/09/02", "f_float", "1.7", "f_ip", "127.0.0.1")
        );
        reqs.add(
            client().prepareIndex("test").setId("2").setSource("f1", "bar", "f_date", "2015/09/01", "f_float", "1.8", "f_ip", "127.0.0.2")
        );
        indexRandom(true, false, reqs);

        SearchResponse resp = client().prepareSearch("test").setQuery(queryStringQuery("foo bar")).get();
        assertHits(resp.getHits(), "1", "2");
        assertHitCount(resp, 2L);

        resp = client().prepareSearch("test").setQuery(queryStringQuery("\"2015/09/02\"")).get();
        assertHits(resp.getHits(), "1");
        assertHitCount(resp, 1L);

        resp = client().prepareSearch("test").setQuery(queryStringQuery("127.0.0.2 \"2015/09/02\"")).get();
        assertHits(resp.getHits(), "1", "2");
        assertHitCount(resp, 2L);

        resp = client().prepareSearch("test").setQuery(queryStringQuery("127.0.0.1 OR 1.8")).get();
        assertHits(resp.getHits(), "1", "2");
        assertHitCount(resp, 2L);
    }

    public void testDocWithAllTypes() throws Exception {
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        String docBody = copyToStringFromClasspath("/org/elasticsearch/search/query/all-example-document.json");
        reqs.add(client().prepareIndex("test").setId("1").setSource(docBody, XContentType.JSON));
        indexRandom(true, false, reqs);

        SearchResponse resp = client().prepareSearch("test").setQuery(queryStringQuery("foo")).get();
        assertHits(resp.getHits(), "1");
        resp = client().prepareSearch("test").setQuery(queryStringQuery("Bar")).get();
        assertHits(resp.getHits(), "1");
        resp = client().prepareSearch("test").setQuery(queryStringQuery("Baz")).get();
        assertHits(resp.getHits(), "1");
        resp = client().prepareSearch("test").setQuery(queryStringQuery("19")).get();
        assertHits(resp.getHits(), "1");
        // nested doesn't match because it's hidden
        resp = client().prepareSearch("test").setQuery(queryStringQuery("1476383971")).get();
        assertHits(resp.getHits(), "1");
        // bool doesn't match
        resp = client().prepareSearch("test").setQuery(queryStringQuery("7")).get();
        assertHits(resp.getHits(), "1");
        resp = client().prepareSearch("test").setQuery(queryStringQuery("23")).get();
        assertHits(resp.getHits(), "1");
        resp = client().prepareSearch("test").setQuery(queryStringQuery("1293")).get();
        assertHits(resp.getHits(), "1");
        resp = client().prepareSearch("test").setQuery(queryStringQuery("42")).get();
        assertHits(resp.getHits(), "1");
        resp = client().prepareSearch("test").setQuery(queryStringQuery("1.7")).get();
        assertHits(resp.getHits(), "1");
        resp = client().prepareSearch("test").setQuery(queryStringQuery("1.5")).get();
        assertHits(resp.getHits(), "1");
        resp = client().prepareSearch("test").setQuery(queryStringQuery("127.0.0.1")).get();
        assertHits(resp.getHits(), "1");
        // binary doesn't match
        // suggest doesn't match
        // geo_point doesn't match
        // geo_shape doesn't match
    }

    public void testKeywordWithWhitespace() throws Exception {
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        reqs.add(client().prepareIndex("test").setId("1").setSource("f2", "Foo Bar"));
        reqs.add(client().prepareIndex("test").setId("2").setSource("f1", "bar"));
        reqs.add(client().prepareIndex("test").setId("3").setSource("f1", "foo bar"));
        indexRandom(true, false, reqs);

        SearchResponse resp = client().prepareSearch("test").setQuery(queryStringQuery("foo")).get();
        assertHits(resp.getHits(), "3");
        assertHitCount(resp, 1L);

        resp = client().prepareSearch("test").setQuery(queryStringQuery("bar")).get();
        assertHits(resp.getHits(), "2", "3");
        assertHitCount(resp, 2L);

        resp = client().prepareSearch("test").setQuery(queryStringQuery("Foo Bar")).get();
        assertHits(resp.getHits(), "1", "2", "3");
        assertHitCount(resp, 3L);
    }

    public void testAllFields() throws Exception {
        String indexBody = copyToStringFromClasspath("/org/elasticsearch/search/query/all-query-index.json");

        Settings.Builder settings = Settings.builder().put("index.query.default_field", "*");
        prepareCreate("test_1").setSource(indexBody, XContentType.JSON).setSettings(settings).get();
        ensureGreen("test_1");

        List<IndexRequestBuilder> reqs = new ArrayList<>();
        reqs.add(client().prepareIndex("test_1").setId("1").setSource("f1", "foo", "f2", "eggplant"));
        indexRandom(true, false, reqs);

        SearchResponse resp = client().prepareSearch("test_1")
            .setQuery(queryStringQuery("foo eggplant").defaultOperator(Operator.AND))
            .get();
        assertHitCount(resp, 0L);

        resp = client().prepareSearch("test_1").setQuery(queryStringQuery("foo eggplant").defaultOperator(Operator.OR)).get();
        assertHits(resp.getHits(), "1");
        assertHitCount(resp, 1L);
    }

    public void testPhraseQueryOnFieldWithNoPositions() throws Exception {
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        reqs.add(client().prepareIndex("test").setId("1").setSource("f1", "foo bar", "f4", "eggplant parmesan"));
        reqs.add(client().prepareIndex("test").setId("2").setSource("f1", "foo bar", "f4", "chicken parmesan"));
        indexRandom(true, false, reqs);

        SearchResponse resp = client().prepareSearch("test").setQuery(queryStringQuery("\"eggplant parmesan\"").lenient(true)).get();
        assertHitCount(resp, 0L);

        Exception exc = expectThrows(
            Exception.class,
            () -> client().prepareSearch("test").setQuery(queryStringQuery("f4:\"eggplant parmesan\"").lenient(false)).get()
        );
        IllegalStateException ise = (IllegalStateException) ExceptionsHelper.unwrap(exc, IllegalStateException.class);
        assertNotNull(ise);
        assertThat(ise.getMessage(), containsString("field:[f4] was indexed without position data; cannot run PhraseQuery"));
    }

    public void testBooleanStrictQuery() throws Exception {
        Exception e = expectThrows(
            Exception.class,
            () -> client().prepareSearch("test").setQuery(queryStringQuery("foo").field("f_bool")).get()
        );
        assertThat(
            ExceptionsHelper.unwrap(e, IllegalArgumentException.class).getMessage(),
            containsString("Can't parse boolean value [foo], expected [true] or [false]")
        );
    }

    public void testAllFieldsWithSpecifiedLeniency() throws IOException {
        Exception e = expectThrows(
            Exception.class,
            () -> client().prepareSearch("test").setQuery(queryStringQuery("f_date:[now-2D TO now]").lenient(false)).get()
        );
        assertThat(e.getCause().getMessage(), containsString("unit [D] not supported for date math [-2D]"));
    }

    public void testLimitOnExpandedFields() throws Exception {

        final int maxClauseCount = randomIntBetween(50, 100);

        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        {
            builder.startObject("_doc");
            {
                builder.startObject("properties");
                {
                    for (int i = 0; i < maxClauseCount; i++) {
                        builder.startObject("field_A" + i).field("type", "text").endObject();
                        builder.startObject("field_B" + i).field("type", "text").endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }

        assertAcked(
            prepareCreate("testindex").setSettings(
                Settings.builder().put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), maxClauseCount + 100)
            ).setMapping(builder)
        );

        client().prepareIndex("testindex").setId("1").setSource("field_A0", "foo bar baz").get();
        refresh();

        int originalMaxClauses = IndexSearcher.getMaxClauseCount();
        try {

            IndexSearcher.setMaxClauseCount(maxClauseCount);

            // single field shouldn't trigger the limit
            doAssertOneHitForQueryString("field_A0:foo");
            // expanding to the limit should work
            doAssertOneHitForQueryString("field_A\\*:foo");

            // adding a non-existing field on top shouldn't overshoot the limit
            doAssertOneHitForQueryString("field_A\\*:foo unmapped:something");

            // the following should exceed the limit
            doAssertLimitExceededException("foo", IndexSearcher.getMaxClauseCount() * 2, "*");
            doAssertLimitExceededException("*:foo", IndexSearcher.getMaxClauseCount() * 2, "*");
            doAssertLimitExceededException("field_\\*:foo", IndexSearcher.getMaxClauseCount() * 2, "field_*");

        } finally {
            IndexSearcher.setMaxClauseCount(originalMaxClauses);
        }
    }

    private void doAssertOneHitForQueryString(String queryString) {
        QueryStringQueryBuilder qb = queryStringQuery(queryString);
        if (randomBoolean()) {
            qb.defaultField("*");
        }
        SearchResponse response = client().prepareSearch("testindex").setQuery(qb).get();
        assertHitCount(response, 1);
    }

    private void doAssertLimitExceededException(String queryString, int exceedingFieldCount, String inputFieldPattern) {
        Exception e = expectThrows(Exception.class, () -> {
            QueryStringQueryBuilder qb = queryStringQuery(queryString);
            if (randomBoolean()) {
                qb.defaultField("*");
            }
            client().prepareSearch("testindex").setQuery(qb).get();
        });
        assertThat(
            ExceptionsHelper.unwrap(e, IllegalArgumentException.class).getMessage(),
            containsString(
                "field expansion for ["
                    + inputFieldPattern
                    + "] matches too many fields, limit: "
                    + IndexSearcher.getMaxClauseCount()
                    + ", got: "
                    + exceedingFieldCount
            )
        );
    }

    public void testFieldAlias() throws Exception {
        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        indexRequests.add(client().prepareIndex("test").setId("1").setSource("f3", "text", "f2", "one"));
        indexRequests.add(client().prepareIndex("test").setId("2").setSource("f3", "value", "f2", "two"));
        indexRequests.add(client().prepareIndex("test").setId("3").setSource("f3", "another value", "f2", "three"));
        indexRandom(true, false, indexRequests);

        SearchResponse response = client().prepareSearch("test").setQuery(queryStringQuery("value").field("f3_alias")).get();

        assertNoFailures(response);
        assertHitCount(response, 2);
        assertHits(response.getHits(), "2", "3");
    }

    public void testFieldAliasWithEmbeddedFieldNames() throws Exception {
        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        indexRequests.add(client().prepareIndex("test").setId("1").setSource("f3", "text", "f2", "one"));
        indexRequests.add(client().prepareIndex("test").setId("2").setSource("f3", "value", "f2", "two"));
        indexRequests.add(client().prepareIndex("test").setId("3").setSource("f3", "another value", "f2", "three"));
        indexRandom(true, false, indexRequests);

        SearchResponse response = client().prepareSearch("test").setQuery(queryStringQuery("f3_alias:value AND f2:three")).get();

        assertNoFailures(response);
        assertHitCount(response, 1);
        assertHits(response.getHits(), "3");
    }

    public void testFieldAliasWithWildcardField() throws Exception {
        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        indexRequests.add(client().prepareIndex("test").setId("1").setSource("f3", "text", "f2", "one"));
        indexRequests.add(client().prepareIndex("test").setId("2").setSource("f3", "value", "f2", "two"));
        indexRequests.add(client().prepareIndex("test").setId("3").setSource("f3", "another value", "f2", "three"));
        indexRandom(true, false, indexRequests);

        SearchResponse response = client().prepareSearch("test").setQuery(queryStringQuery("value").field("f3_*")).get();

        assertNoFailures(response);
        assertHitCount(response, 2);
        assertHits(response.getHits(), "2", "3");
    }

    public void testFieldAliasOnDisallowedFieldType() throws Exception {
        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        indexRequests.add(client().prepareIndex("test").setId("1").setSource("f3", "text", "f2", "one"));
        indexRandom(true, false, indexRequests);

        // The wildcard field matches aliases for both a text and geo_point field.
        // By default, the geo_point field should be ignored when building the query.
        SearchResponse response = client().prepareSearch("test").setQuery(queryStringQuery("text").field("f*_alias")).get();

        assertNoFailures(response);
        assertHitCount(response, 1);
        assertHits(response.getHits(), "1");
    }

    private void assertHits(SearchHits hits, String... ids) {
        assertThat(hits.getTotalHits().value, equalTo((long) ids.length));
        Set<String> hitIds = new HashSet<>();
        for (SearchHit hit : hits.getHits()) {
            hitIds.add(hit.getId());
        }
        assertThat(hitIds, containsInAnyOrder(ids));
    }
}
