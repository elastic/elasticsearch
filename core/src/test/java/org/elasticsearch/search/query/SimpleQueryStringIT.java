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

package org.elasticsearch.search.query;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.SimpleQueryStringBuilder;
import org.elasticsearch.index.query.SimpleQueryStringFlag;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.index.query.QueryBuilders.simpleQueryStringQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for the {@code simple_query_string} query
 */
public class SimpleQueryStringIT extends ESIntegTestCase {

    @Test
    public void testSimpleQueryString() throws ExecutionException, InterruptedException {
        createIndex("test");
        indexRandom(true, false,
                client().prepareIndex("test", "type1", "1").setSource("body", "foo"),
                client().prepareIndex("test", "type1", "2").setSource("body", "bar"),
                client().prepareIndex("test", "type1", "3").setSource("body", "foo bar"),
                client().prepareIndex("test", "type1", "4").setSource("body", "quux baz eggplant"),
                client().prepareIndex("test", "type1", "5").setSource("body", "quux baz spaghetti"),
                client().prepareIndex("test", "type1", "6").setSource("otherbody", "spaghetti"));

        SearchResponse searchResponse = client().prepareSearch().setQuery(simpleQueryStringQuery("foo bar")).get();
        assertHitCount(searchResponse, 3l);
        assertSearchHits(searchResponse, "1", "2", "3");

        // Tests boost value setting. In this case doc 1 should always be ranked above the other
        // two matches.
        searchResponse = client().prepareSearch().setQuery(
                boolQuery()
                    .should(simpleQueryStringQuery("\"foo bar\"").boost(10.0f))
                    .should(termQuery("body", "eggplant"))).get();
        assertHitCount(searchResponse, 2l);
        assertFirstHit(searchResponse, hasId("3"));

        searchResponse = client().prepareSearch().setQuery(
                simpleQueryStringQuery("foo bar").defaultOperator(SimpleQueryStringBuilder.Operator.AND)).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("3"));

        searchResponse = client().prepareSearch().setQuery(simpleQueryStringQuery("\"quux baz\" +(eggplant | spaghetti)")).get();
        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "4", "5");

        searchResponse = client().prepareSearch().setQuery(
                simpleQueryStringQuery("eggplants").analyzer("snowball")).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("4"));

        searchResponse = client().prepareSearch().setQuery(
                simpleQueryStringQuery("spaghetti").field("body", 1000.0f).field("otherbody", 2.0f).queryName("myquery")).get();
        assertHitCount(searchResponse, 2l);
        assertFirstHit(searchResponse, hasId("5"));
        assertSearchHits(searchResponse, "5", "6");
        assertThat(searchResponse.getHits().getAt(0).getMatchedQueries()[0], equalTo("myquery"));

        searchResponse = client().prepareSearch().setQuery(simpleQueryStringQuery("spaghetti").field("*body")).get();
        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "5", "6");
    }

    @Test
    public void testSimpleQueryStringMinimumShouldMatch() throws Exception {
        createIndex("test");
        ensureGreen("test");
        indexRandom(true, false,
                client().prepareIndex("test", "type1", "1").setSource("body", "foo"),
                client().prepareIndex("test", "type1", "2").setSource("body", "bar"),
                client().prepareIndex("test", "type1", "3").setSource("body", "foo bar"),
                client().prepareIndex("test", "type1", "4").setSource("body", "foo baz bar"));


        logger.info("--> query 1");
        SearchResponse searchResponse = client().prepareSearch().setQuery(simpleQueryStringQuery("foo bar").minimumShouldMatch("2")).get();
        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "3", "4");

        logger.info("--> query 2");
        searchResponse = client().prepareSearch().setQuery(simpleQueryStringQuery("foo bar").field("body").field("body2").minimumShouldMatch("2")).get();
        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "3", "4");

        logger.info("--> query 3");
        searchResponse = client().prepareSearch().setQuery(simpleQueryStringQuery("foo bar baz").field("body").field("body2").minimumShouldMatch("70%")).get();
        assertHitCount(searchResponse, 2l);
        assertSearchHits(searchResponse, "3", "4");

        indexRandom(true, false,
                client().prepareIndex("test", "type1", "5").setSource("body2", "foo", "other", "foo"),
                client().prepareIndex("test", "type1", "6").setSource("body2", "bar", "other", "foo"),
                client().prepareIndex("test", "type1", "7").setSource("body2", "foo bar", "other", "foo"),
                client().prepareIndex("test", "type1", "8").setSource("body2", "foo baz bar", "other", "foo"));

        logger.info("--> query 4");
        searchResponse = client().prepareSearch().setQuery(simpleQueryStringQuery("foo bar").field("body").field("body2").minimumShouldMatch("2")).get();
        assertHitCount(searchResponse, 4l);
        assertSearchHits(searchResponse, "3", "4", "7", "8");

        logger.info("--> query 5");
        searchResponse = client().prepareSearch().setQuery(simpleQueryStringQuery("foo bar").minimumShouldMatch("2")).get();
        assertHitCount(searchResponse, 5l);
        assertSearchHits(searchResponse, "3", "4", "6", "7", "8");

        logger.info("--> query 6");
        searchResponse = client().prepareSearch().setQuery(simpleQueryStringQuery("foo bar baz").field("body2").field("other").minimumShouldMatch("70%")).get();
        assertHitCount(searchResponse, 3l);
        assertSearchHits(searchResponse, "6", "7", "8");
    }

    @Test
    public void testSimpleQueryStringLowercasing() {
        createIndex("test");
        client().prepareIndex("test", "type1", "1").setSource("body", "Professional").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch().setQuery(simpleQueryStringQuery("Professio*")).get();
        assertHitCount(searchResponse, 1l);
        assertSearchHits(searchResponse, "1");

        searchResponse = client().prepareSearch().setQuery(
                simpleQueryStringQuery("Professio*").lowercaseExpandedTerms(false)).get();
        assertHitCount(searchResponse, 0l);

        searchResponse = client().prepareSearch().setQuery(
                simpleQueryStringQuery("Professionan~1")).get();
        assertHitCount(searchResponse, 1l);
        assertSearchHits(searchResponse, "1");

        searchResponse = client().prepareSearch().setQuery(
                simpleQueryStringQuery("Professionan~1").lowercaseExpandedTerms(false)).get();
        assertHitCount(searchResponse, 0l);
    }

    @Test
    public void testQueryStringLocale() {
        createIndex("test");
        client().prepareIndex("test", "type1", "1").setSource("body", "bılly").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch().setQuery(simpleQueryStringQuery("BILL*")).get();
        assertHitCount(searchResponse, 0l);
        searchResponse = client().prepareSearch().setQuery(queryStringQuery("body:BILL*")).get();
        assertHitCount(searchResponse, 0l);

        searchResponse = client().prepareSearch().setQuery(
                simpleQueryStringQuery("BILL*").locale(new Locale("tr", "TR"))).get();
        assertHitCount(searchResponse, 1l);
        assertSearchHits(searchResponse, "1");
        searchResponse = client().prepareSearch().setQuery(
                queryStringQuery("body:BILL*").locale(new Locale("tr", "TR"))).get();
        assertHitCount(searchResponse, 1l);
        assertSearchHits(searchResponse, "1");
    }

    @Test
    public void testNestedFieldSimpleQueryString() throws IOException {
        assertAcked(prepareCreate("test")
                .addMapping("type1", jsonBuilder()
                        .startObject()
                        .startObject("type1")
                        .startObject("properties")
                        .startObject("body").field("type", "string")
                        .startObject("fields")
                        .startObject("sub").field("type", "string")
                        .endObject() // sub
                        .endObject() // fields
                        .endObject() // body
                        .endObject() // properties
                        .endObject() // type1
                        .endObject()));
        client().prepareIndex("test", "type1", "1").setSource("body", "foo bar baz").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch().setQuery(
                simpleQueryStringQuery("foo bar baz").field("body")).get();
        assertHitCount(searchResponse, 1l);
        assertSearchHits(searchResponse, "1");

        searchResponse = client().prepareSearch().setTypes("type1").setQuery(
                simpleQueryStringQuery("foo bar baz").field("body")).get();
        assertHitCount(searchResponse, 1l);
        assertSearchHits(searchResponse, "1");

        searchResponse = client().prepareSearch().setQuery(
                simpleQueryStringQuery("foo bar baz").field("body.sub")).get();
        assertHitCount(searchResponse, 1l);
        assertSearchHits(searchResponse, "1");

        searchResponse = client().prepareSearch().setTypes("type1").setQuery(
                simpleQueryStringQuery("foo bar baz").field("body.sub")).get();
        assertHitCount(searchResponse, 1l);
        assertSearchHits(searchResponse, "1");
    }

    @Test
    public void testSimpleQueryStringFlags() throws ExecutionException, InterruptedException {
        createIndex("test");
        indexRandom(true,
                client().prepareIndex("test", "type1", "1").setSource("body", "foo"),
                client().prepareIndex("test", "type1", "2").setSource("body", "bar"),
                client().prepareIndex("test", "type1", "3").setSource("body", "foo bar"),
                client().prepareIndex("test", "type1", "4").setSource("body", "quux baz eggplant"),
                client().prepareIndex("test", "type1", "5").setSource("body", "quux baz spaghetti"),
                client().prepareIndex("test", "type1", "6").setSource("otherbody", "spaghetti"));

        SearchResponse searchResponse = client().prepareSearch().setQuery(
                simpleQueryStringQuery("foo bar").flags(SimpleQueryStringFlag.ALL)).get();
        assertHitCount(searchResponse, 3l);
        assertSearchHits(searchResponse, "1", "2", "3");

        // Sending a negative 'flags' value is the same as SimpleQueryStringFlag.ALL
        searchResponse = client().prepareSearch().setQuery("{\"simple_query_string\": {\"query\": \"foo bar\", \"flags\": -1}}").get();
        assertHitCount(searchResponse, 3l);
        assertSearchHits(searchResponse, "1", "2", "3");

        searchResponse = client().prepareSearch().setQuery(
                simpleQueryStringQuery("foo | bar")
                        .defaultOperator(SimpleQueryStringBuilder.Operator.AND)
                        .flags(SimpleQueryStringFlag.OR)).get();
        assertHitCount(searchResponse, 3l);
        assertSearchHits(searchResponse, "1", "2", "3");

        searchResponse = client().prepareSearch().setQuery(
                simpleQueryStringQuery("foo | bar")
                        .defaultOperator(SimpleQueryStringBuilder.Operator.AND)
                        .flags(SimpleQueryStringFlag.NONE)).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("3"));

        searchResponse = client().prepareSearch().setQuery(
                simpleQueryStringQuery("baz | egg*")
                        .defaultOperator(SimpleQueryStringBuilder.Operator.AND)
                        .flags(SimpleQueryStringFlag.NONE)).get();
        assertHitCount(searchResponse, 0l);

        searchResponse = client().prepareSearch().setSource(new BytesArray("{\n" +
                "  \"query\": {\n" +
                "    \"simple_query_string\": {\n" +
                "      \"query\": \"foo|bar\",\n" +
                "      \"default_operator\": \"AND\"," +
                "      \"flags\": \"NONE\"\n" +
                "    }\n" +
                "  }\n" +
                "}")).get();
        assertHitCount(searchResponse, 1l);

        searchResponse = client().prepareSearch().setQuery(
                simpleQueryStringQuery("baz | egg*")
                        .defaultOperator(SimpleQueryStringBuilder.Operator.AND)
                        .flags(SimpleQueryStringFlag.WHITESPACE, SimpleQueryStringFlag.PREFIX)).get();
        assertHitCount(searchResponse, 1l);
        assertFirstHit(searchResponse, hasId("4"));
    }

    @Test
    public void testSimpleQueryStringLenient() throws ExecutionException, InterruptedException {
        createIndex("test1", "test2");
        indexRandom(true, client().prepareIndex("test1", "type1", "1").setSource("field", "foo"),
                client().prepareIndex("test2", "type1", "10").setSource("field", 5));
        refresh();

        SearchResponse searchResponse = client().prepareSearch().setQuery(simpleQueryStringQuery("foo").field("field")).get();
        assertFailures(searchResponse);
        assertHitCount(searchResponse, 1l);
        assertSearchHits(searchResponse, "1");

        searchResponse = client().prepareSearch().setQuery(simpleQueryStringQuery("foo").field("field").lenient(true)).get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 1l);
        assertSearchHits(searchResponse, "1");
    }

    @Test // see: https://github.com/elasticsearch/elasticsearch/issues/7967
    public void testLenientFlagBeingTooLenient() throws Exception {
        indexRandom(true,
                client().prepareIndex("test", "doc", "1").setSource("num", 1, "body", "foo bar baz"),
                client().prepareIndex("test", "doc", "2").setSource("num", 2, "body", "eggplant spaghetti lasagna"));

        BoolQueryBuilder q = boolQuery().should(simpleQueryStringQuery("bar").field("num").field("body").lenient(true));
        SearchResponse resp = client().prepareSearch("test").setQuery(q).get();
        assertNoFailures(resp);
        // the bug is that this would be parsed into basically a match_all
        // query and this would match both documents
        assertHitCount(resp, 1);
        assertSearchHits(resp, "1");
    }

    @Test
    public void testSimpleQueryStringAnalyzeWildcard() throws ExecutionException, InterruptedException, IOException {
        String mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("type1")
                .startObject("properties")
                .startObject("location")
                .field("type", "string")
                .field("analyzer", "german")
                .endObject()
                .endObject()
                .endObject()
                .endObject().string();

        CreateIndexRequestBuilder mappingRequest = client().admin().indices().prepareCreate("test1").addMapping("type1", mapping);
        mappingRequest.execute().actionGet();
        indexRandom(true, client().prepareIndex("test1", "type1", "1").setSource("location", "Köln"));
        refresh();

        SearchResponse searchResponse = client().prepareSearch().setQuery(simpleQueryStringQuery("Köln*").analyzeWildcard(true).field("location")).get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 1l);
        assertSearchHits(searchResponse, "1");
    }

}
