/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.query;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.analysis.PreConfiguredTokenFilter;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.SimpleQueryStringFlag;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static java.util.Collections.singletonList;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.index.query.QueryBuilders.simpleQueryStringQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFirstHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasId;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for the {@code simple_query_string} query
 */
public class SimpleQueryStringIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(MockAnalysisPlugin.class);
    }

    public void testSimpleQueryString() throws ExecutionException, InterruptedException {
        Settings.Builder settings = Settings.builder();
        settings.put(indexSettings());
        settings.put("index.analysis.analyzer.mock_snowball.tokenizer", "standard");
        settings.put("index.analysis.analyzer.mock_snowball.filter", "mock_snowball");
        createIndex("test", settings.build());
        indexRandom(
            true,
            false,
            client().prepareIndex("test").setId("1").setSource("body", "foo"),
            client().prepareIndex("test").setId("2").setSource("body", "bar"),
            client().prepareIndex("test").setId("3").setSource("body", "foo bar"),
            client().prepareIndex("test").setId("4").setSource("body", "quux baz eggplant"),
            client().prepareIndex("test").setId("5").setSource("body", "quux baz spaghetti"),
            client().prepareIndex("test").setId("6").setSource("otherbody", "spaghetti")
        );

        SearchResponse searchResponse = client().prepareSearch().setQuery(simpleQueryStringQuery("foo bar")).get();
        assertHitCount(searchResponse, 3L);
        assertSearchHits(searchResponse, "1", "2", "3");

        // Tests boost value setting. In this case doc 1 should always be ranked above the other
        // two matches.
        searchResponse = client().prepareSearch()
            .setQuery(boolQuery().should(simpleQueryStringQuery("\"foo bar\"").boost(10.0f)).should(termQuery("body", "eggplant")))
            .get();
        assertHitCount(searchResponse, 2L);
        assertFirstHit(searchResponse, hasId("3"));

        searchResponse = client().prepareSearch().setQuery(simpleQueryStringQuery("foo bar").defaultOperator(Operator.AND)).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("3"));

        searchResponse = client().prepareSearch().setQuery(simpleQueryStringQuery("\"quux baz\" +(eggplant | spaghetti)")).get();
        assertHitCount(searchResponse, 2L);
        assertSearchHits(searchResponse, "4", "5");

        searchResponse = client().prepareSearch().setQuery(simpleQueryStringQuery("eggplants").analyzer("mock_snowball")).get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("4"));

        searchResponse = client().prepareSearch()
            .setQuery(simpleQueryStringQuery("spaghetti").field("body", 1000.0f).field("otherbody", 2.0f).queryName("myquery"))
            .get();
        assertHitCount(searchResponse, 2L);
        assertFirstHit(searchResponse, hasId("5"));
        assertSearchHits(searchResponse, "5", "6");
        assertThat(searchResponse.getHits().getAt(0).getMatchedQueries()[0], equalTo("myquery"));

        searchResponse = client().prepareSearch().setQuery(simpleQueryStringQuery("spaghetti").field("*body")).get();
        assertHitCount(searchResponse, 2L);
        assertSearchHits(searchResponse, "5", "6");
    }

    public void testSimpleQueryStringMinimumShouldMatch() throws Exception {
        createIndex("test");
        ensureGreen("test");
        indexRandom(
            true,
            false,
            client().prepareIndex("test").setId("1").setSource("body", "foo"),
            client().prepareIndex("test").setId("2").setSource("body", "bar"),
            client().prepareIndex("test").setId("3").setSource("body", "foo bar"),
            client().prepareIndex("test").setId("4").setSource("body", "foo baz bar")
        );

        logger.info("--> query 1");
        SearchResponse searchResponse = client().prepareSearch().setQuery(simpleQueryStringQuery("foo bar").minimumShouldMatch("2")).get();
        assertHitCount(searchResponse, 2L);
        assertSearchHits(searchResponse, "3", "4");

        logger.info("--> query 2");
        searchResponse = client().prepareSearch()
            .setQuery(simpleQueryStringQuery("foo bar").field("body").field("body2").minimumShouldMatch("2"))
            .get();
        assertHitCount(searchResponse, 2L);
        assertSearchHits(searchResponse, "3", "4");

        // test case from #13884
        logger.info("--> query 3");
        searchResponse = client().prepareSearch()
            .setQuery(simpleQueryStringQuery("foo").field("body").field("body2").field("body3").minimumShouldMatch("-50%"))
            .get();
        assertHitCount(searchResponse, 3L);
        assertSearchHits(searchResponse, "1", "3", "4");

        logger.info("--> query 4");
        searchResponse = client().prepareSearch()
            .setQuery(simpleQueryStringQuery("foo bar baz").field("body").field("body2").minimumShouldMatch("70%"))
            .get();
        assertHitCount(searchResponse, 2L);
        assertSearchHits(searchResponse, "3", "4");

        indexRandom(
            true,
            false,
            client().prepareIndex("test").setId("5").setSource("body2", "foo", "other", "foo"),
            client().prepareIndex("test").setId("6").setSource("body2", "bar", "other", "foo"),
            client().prepareIndex("test").setId("7").setSource("body2", "foo bar", "other", "foo"),
            client().prepareIndex("test").setId("8").setSource("body2", "foo baz bar", "other", "foo")
        );

        logger.info("--> query 5");
        searchResponse = client().prepareSearch()
            .setQuery(simpleQueryStringQuery("foo bar").field("body").field("body2").minimumShouldMatch("2"))
            .get();
        assertHitCount(searchResponse, 4L);
        assertSearchHits(searchResponse, "3", "4", "7", "8");

        logger.info("--> query 6");
        searchResponse = client().prepareSearch().setQuery(simpleQueryStringQuery("foo bar").minimumShouldMatch("2")).get();
        assertHitCount(searchResponse, 5L);
        assertSearchHits(searchResponse, "3", "4", "6", "7", "8");

        logger.info("--> query 7");
        searchResponse = client().prepareSearch()
            .setQuery(simpleQueryStringQuery("foo bar baz").field("body2").field("other").minimumShouldMatch("70%"))
            .get();
        assertHitCount(searchResponse, 3L);
        assertSearchHits(searchResponse, "6", "7", "8");
    }

    public void testNestedFieldSimpleQueryString() throws IOException {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("body")
                    .field("type", "text")
                    .startObject("fields")
                    .startObject("sub")
                    .field("type", "text")
                    .endObject() // sub
                    .endObject() // fields
                    .endObject() // body
                    .endObject() // properties
                    .endObject() // type1
                    .endObject()
            )
        );
        client().prepareIndex("test").setId("1").setSource("body", "foo bar baz").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch().setQuery(simpleQueryStringQuery("foo bar baz").field("body")).get();
        assertHitCount(searchResponse, 1L);
        assertSearchHits(searchResponse, "1");

        searchResponse = client().prepareSearch().setQuery(simpleQueryStringQuery("foo bar baz").field("body")).get();
        assertHitCount(searchResponse, 1L);
        assertSearchHits(searchResponse, "1");

        searchResponse = client().prepareSearch().setQuery(simpleQueryStringQuery("foo bar baz").field("body.sub")).get();
        assertHitCount(searchResponse, 1L);
        assertSearchHits(searchResponse, "1");

        searchResponse = client().prepareSearch().setQuery(simpleQueryStringQuery("foo bar baz").field("body.sub")).get();
        assertHitCount(searchResponse, 1L);
        assertSearchHits(searchResponse, "1");
    }

    public void testSimpleQueryStringFlags() throws ExecutionException, InterruptedException {
        createIndex("test");
        indexRandom(
            true,
            client().prepareIndex("test").setId("1").setSource("body", "foo"),
            client().prepareIndex("test").setId("2").setSource("body", "bar"),
            client().prepareIndex("test").setId("3").setSource("body", "foo bar"),
            client().prepareIndex("test").setId("4").setSource("body", "quux baz eggplant"),
            client().prepareIndex("test").setId("5").setSource("body", "quux baz spaghetti"),
            client().prepareIndex("test").setId("6").setSource("otherbody", "spaghetti")
        );

        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(simpleQueryStringQuery("foo bar").flags(SimpleQueryStringFlag.ALL))
            .get();
        assertHitCount(searchResponse, 3L);
        assertSearchHits(searchResponse, "1", "2", "3");

        searchResponse = client().prepareSearch()
            .setQuery(simpleQueryStringQuery("foo | bar").defaultOperator(Operator.AND).flags(SimpleQueryStringFlag.OR))
            .get();
        assertHitCount(searchResponse, 3L);
        assertSearchHits(searchResponse, "1", "2", "3");

        searchResponse = client().prepareSearch()
            .setQuery(simpleQueryStringQuery("foo | bar").defaultOperator(Operator.AND).flags(SimpleQueryStringFlag.NONE))
            .get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("3"));

        searchResponse = client().prepareSearch()
            .setQuery(simpleQueryStringQuery("baz | egg*").defaultOperator(Operator.AND).flags(SimpleQueryStringFlag.NONE))
            .get();
        assertHitCount(searchResponse, 0L);

        searchResponse = client().prepareSearch()
            .setSource(
                new SearchSourceBuilder().query(
                    QueryBuilders.simpleQueryStringQuery("foo|bar").defaultOperator(Operator.AND).flags(SimpleQueryStringFlag.NONE)
                )
            )
            .get();
        assertHitCount(searchResponse, 1L);

        searchResponse = client().prepareSearch()
            .setQuery(
                simpleQueryStringQuery("quuz~1 + egg*").flags(
                    SimpleQueryStringFlag.WHITESPACE,
                    SimpleQueryStringFlag.AND,
                    SimpleQueryStringFlag.FUZZY,
                    SimpleQueryStringFlag.PREFIX
                )
            )
            .get();
        assertHitCount(searchResponse, 1L);
        assertFirstHit(searchResponse, hasId("4"));
    }

    public void testSimpleQueryStringLenient() throws ExecutionException, InterruptedException {
        createIndex("test1", "test2");
        indexRandom(
            true,
            client().prepareIndex("test1").setId("1").setSource("field", "foo"),
            client().prepareIndex("test2").setId("10").setSource("field", 5)
        );
        refresh();

        SearchResponse searchResponse = client().prepareSearch()
            .setAllowPartialSearchResults(true)
            .setQuery(simpleQueryStringQuery("foo").field("field"))
            .get();
        assertFailures(searchResponse);
        assertHitCount(searchResponse, 1L);
        assertSearchHits(searchResponse, "1");

        searchResponse = client().prepareSearch().setQuery(simpleQueryStringQuery("foo").field("field").lenient(true)).get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 1L);
        assertSearchHits(searchResponse, "1");
    }

    // Issue #7967
    public void testLenientFlagBeingTooLenient() throws Exception {
        indexRandom(
            true,
            client().prepareIndex("test").setId("1").setSource("num", 1, "body", "foo bar baz"),
            client().prepareIndex("test").setId("2").setSource("num", 2, "body", "eggplant spaghetti lasagna")
        );

        BoolQueryBuilder q = boolQuery().should(simpleQueryStringQuery("bar").field("num").field("body").lenient(true));
        SearchResponse resp = client().prepareSearch("test").setQuery(q).get();
        assertNoFailures(resp);
        // the bug is that this would be parsed into basically a match_all
        // query and this would match both documents
        assertHitCount(resp, 1);
        assertSearchHits(resp, "1");
    }

    public void testSimpleQueryStringAnalyzeWildcard() throws ExecutionException, InterruptedException, IOException {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject("location")
                .field("type", "text")
                .field("analyzer", "standard")
                .endObject()
                .endObject()
                .endObject()
        );

        CreateIndexRequestBuilder mappingRequest = client().admin().indices().prepareCreate("test1").setMapping(mapping);
        mappingRequest.get();
        indexRandom(true, client().prepareIndex("test1").setId("1").setSource("location", "Köln"));
        refresh();

        SearchResponse searchResponse = client().prepareSearch().setQuery(simpleQueryStringQuery("Köln*").field("location")).get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 1L);
        assertSearchHits(searchResponse, "1");
    }

    public void testSimpleQueryStringUsesFieldAnalyzer() throws Exception {
        client().prepareIndex("test").setId("1").setSource("foo", 123, "bar", "abc").get();
        client().prepareIndex("test").setId("2").setSource("foo", 234, "bar", "bcd").get();

        refresh();

        SearchResponse searchResponse = client().prepareSearch().setQuery(simpleQueryStringQuery("123").field("foo").field("bar")).get();
        assertHitCount(searchResponse, 1L);
        assertSearchHits(searchResponse, "1");
    }

    public void testSimpleQueryStringOnIndexMetaField() throws Exception {
        client().prepareIndex("test").setId("1").setSource("foo", 123, "bar", "abc").get();
        client().prepareIndex("test").setId("2").setSource("foo", 234, "bar", "bcd").get();

        refresh();

        SearchResponse searchResponse = client().prepareSearch().setQuery(simpleQueryStringQuery("test").field("_index")).get();
        assertHitCount(searchResponse, 2L);
        assertSearchHits(searchResponse, "1", "2");
    }

    public void testEmptySimpleQueryStringWithAnalysis() throws Exception {
        // https://github.com/elastic/elasticsearch/issues/18202
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject("body")
                .field("type", "text")
                .field("analyzer", "stop")
                .endObject()
                .endObject()
                .endObject()
        );

        CreateIndexRequestBuilder mappingRequest = client().admin().indices().prepareCreate("test1").setMapping(mapping);
        mappingRequest.get();
        indexRandom(true, client().prepareIndex("test1").setId("1").setSource("body", "Some Text"));
        refresh();

        SearchResponse searchResponse = client().prepareSearch().setQuery(simpleQueryStringQuery("the*").field("body")).get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 0L);
    }

    public void testBasicAllQuery() throws Exception {
        String indexBody = copyToStringFromClasspath("/org/elasticsearch/search/query/all-query-index.json");
        prepareCreate("test").setSource(indexBody, XContentType.JSON).get();
        ensureGreen("test");

        List<IndexRequestBuilder> reqs = new ArrayList<>();
        reqs.add(client().prepareIndex("test").setId("1").setSource("f1", "foo bar baz"));
        reqs.add(client().prepareIndex("test").setId("2").setSource("f2", "Bar"));
        reqs.add(client().prepareIndex("test").setId("3").setSource("f3", "foo bar baz"));
        indexRandom(true, false, reqs);

        SearchResponse resp = client().prepareSearch("test").setQuery(simpleQueryStringQuery("foo")).get();
        assertHitCount(resp, 2L);
        assertHits(resp.getHits(), "1", "3");

        resp = client().prepareSearch("test").setQuery(simpleQueryStringQuery("bar")).get();
        assertHitCount(resp, 2L);
        assertHits(resp.getHits(), "1", "3");

        resp = client().prepareSearch("test").setQuery(simpleQueryStringQuery("Bar")).get();
        assertHitCount(resp, 3L);
        assertHits(resp.getHits(), "1", "2", "3");
    }

    public void testWithDate() throws Exception {
        String indexBody = copyToStringFromClasspath("/org/elasticsearch/search/query/all-query-index.json");
        prepareCreate("test").setSource(indexBody, XContentType.JSON).get();
        ensureGreen("test");

        List<IndexRequestBuilder> reqs = new ArrayList<>();
        reqs.add(client().prepareIndex("test").setId("1").setSource("f1", "foo", "f_date", "2015/09/02"));
        reqs.add(client().prepareIndex("test").setId("2").setSource("f1", "bar", "f_date", "2015/09/01"));
        indexRandom(true, false, reqs);

        SearchResponse resp = client().prepareSearch("test").setQuery(simpleQueryStringQuery("foo bar")).get();
        assertHits(resp.getHits(), "1", "2");
        assertHitCount(resp, 2L);

        resp = client().prepareSearch("test").setQuery(simpleQueryStringQuery("\"2015/09/02\"")).get();
        assertHits(resp.getHits(), "1");
        assertHitCount(resp, 1L);

        resp = client().prepareSearch("test").setQuery(simpleQueryStringQuery("bar \"2015/09/02\"")).get();
        assertHits(resp.getHits(), "1", "2");
        assertHitCount(resp, 2L);

        resp = client().prepareSearch("test").setQuery(simpleQueryStringQuery("\"2015/09/02\" \"2015/09/01\"")).get();
        assertHits(resp.getHits(), "1", "2");
        assertHitCount(resp, 2L);
    }

    public void testWithLotsOfTypes() throws Exception {
        String indexBody = copyToStringFromClasspath("/org/elasticsearch/search/query/all-query-index.json");
        prepareCreate("test").setSource(indexBody, XContentType.JSON).get();
        ensureGreen("test");

        List<IndexRequestBuilder> reqs = new ArrayList<>();
        reqs.add(
            client().prepareIndex("test").setId("1").setSource("f1", "foo", "f_date", "2015/09/02", "f_float", "1.7", "f_ip", "127.0.0.1")
        );
        reqs.add(
            client().prepareIndex("test").setId("2").setSource("f1", "bar", "f_date", "2015/09/01", "f_float", "1.8", "f_ip", "127.0.0.2")
        );
        indexRandom(true, false, reqs);

        SearchResponse resp = client().prepareSearch("test").setQuery(simpleQueryStringQuery("foo bar")).get();
        assertHits(resp.getHits(), "1", "2");
        assertHitCount(resp, 2L);

        resp = client().prepareSearch("test").setQuery(simpleQueryStringQuery("\"2015/09/02\"")).get();
        assertHits(resp.getHits(), "1");
        assertHitCount(resp, 1L);

        resp = client().prepareSearch("test").setQuery(simpleQueryStringQuery("127.0.0.2 \"2015/09/02\"")).get();
        assertHits(resp.getHits(), "1", "2");
        assertHitCount(resp, 2L);

        resp = client().prepareSearch("test").setQuery(simpleQueryStringQuery("127.0.0.1 1.8")).get();
        assertHits(resp.getHits(), "1", "2");
        assertHitCount(resp, 2L);
    }

    public void testDocWithAllTypes() throws Exception {
        String indexBody = copyToStringFromClasspath("/org/elasticsearch/search/query/all-query-index.json");
        prepareCreate("test").setSource(indexBody, XContentType.JSON).get();
        ensureGreen("test");

        List<IndexRequestBuilder> reqs = new ArrayList<>();
        String docBody = copyToStringFromClasspath("/org/elasticsearch/search/query/all-example-document.json");
        reqs.add(client().prepareIndex("test").setId("1").setSource(docBody, XContentType.JSON));
        indexRandom(true, false, reqs);

        SearchResponse resp = client().prepareSearch("test").setQuery(simpleQueryStringQuery("foo")).get();
        assertHits(resp.getHits(), "1");
        resp = client().prepareSearch("test").setQuery(simpleQueryStringQuery("Bar")).get();
        assertHits(resp.getHits(), "1");
        resp = client().prepareSearch("test").setQuery(simpleQueryStringQuery("Baz")).get();
        assertHits(resp.getHits(), "1");
        resp = client().prepareSearch("test").setQuery(simpleQueryStringQuery("19")).get();
        assertHits(resp.getHits(), "1");
        // nested doesn't match because it's hidden
        resp = client().prepareSearch("test").setQuery(simpleQueryStringQuery("1476383971")).get();
        assertHits(resp.getHits(), "1");
        // bool doesn't match
        resp = client().prepareSearch("test").setQuery(simpleQueryStringQuery("7")).get();
        assertHits(resp.getHits(), "1");
        resp = client().prepareSearch("test").setQuery(simpleQueryStringQuery("23")).get();
        assertHits(resp.getHits(), "1");
        resp = client().prepareSearch("test").setQuery(simpleQueryStringQuery("1293")).get();
        assertHits(resp.getHits(), "1");
        resp = client().prepareSearch("test").setQuery(simpleQueryStringQuery("42")).get();
        assertHits(resp.getHits(), "1");
        resp = client().prepareSearch("test").setQuery(simpleQueryStringQuery("1.7")).get();
        assertHits(resp.getHits(), "1");
        resp = client().prepareSearch("test").setQuery(simpleQueryStringQuery("1.5")).get();
        assertHits(resp.getHits(), "1");
        resp = client().prepareSearch("test").setQuery(simpleQueryStringQuery("127.0.0.1")).get();
        assertHits(resp.getHits(), "1");
        // binary doesn't match
        // suggest doesn't match
        // geo_point doesn't match
        // geo_shape doesn't match

        resp = client().prepareSearch("test").setQuery(simpleQueryStringQuery("foo Bar 19 127.0.0.1").defaultOperator(Operator.AND)).get();
        assertHits(resp.getHits(), "1");
    }

    public void testKeywordWithWhitespace() throws Exception {
        String indexBody = copyToStringFromClasspath("/org/elasticsearch/search/query/all-query-index.json");
        prepareCreate("test").setSource(indexBody, XContentType.JSON).get();
        ensureGreen("test");

        List<IndexRequestBuilder> reqs = new ArrayList<>();
        reqs.add(client().prepareIndex("test").setId("1").setSource("f2", "Foo Bar"));
        reqs.add(client().prepareIndex("test").setId("2").setSource("f1", "bar"));
        reqs.add(client().prepareIndex("test").setId("3").setSource("f1", "foo bar"));
        indexRandom(true, false, reqs);

        SearchResponse resp = client().prepareSearch("test").setQuery(simpleQueryStringQuery("foo")).get();
        assertHits(resp.getHits(), "3");
        assertHitCount(resp, 1L);

        resp = client().prepareSearch("test").setQuery(simpleQueryStringQuery("bar")).get();
        assertHits(resp.getHits(), "2", "3");
        assertHitCount(resp, 2L);
    }

    public void testAllFieldsWithSpecifiedLeniency() throws Exception {
        String indexBody = copyToStringFromClasspath("/org/elasticsearch/search/query/all-query-index.json");
        prepareCreate("test").setSource(indexBody, XContentType.JSON).get();
        ensureGreen("test");

        List<IndexRequestBuilder> reqs = new ArrayList<>();
        reqs.add(client().prepareIndex("test").setId("1").setSource("f_long", 1));
        indexRandom(true, false, reqs);

        SearchPhaseExecutionException e = expectThrows(
            SearchPhaseExecutionException.class,
            () -> client().prepareSearch("test").setQuery(simpleQueryStringQuery("foo123").lenient(false)).get()
        );
        assertThat(e.getDetailedMessage(), containsString("NumberFormatException: For input string: \"foo123\""));
    }

    public void testFieldAlias() throws Exception {
        String indexBody = copyToStringFromClasspath("/org/elasticsearch/search/query/all-query-index.json");
        assertAcked(prepareCreate("test").setSource(indexBody, XContentType.JSON));
        ensureGreen("test");

        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        indexRequests.add(client().prepareIndex("test").setId("1").setSource("f3", "text", "f2", "one"));
        indexRequests.add(client().prepareIndex("test").setId("2").setSource("f3", "value", "f2", "two"));
        indexRequests.add(client().prepareIndex("test").setId("3").setSource("f3", "another value", "f2", "three"));
        indexRandom(true, false, indexRequests);

        SearchResponse response = client().prepareSearch("test").setQuery(simpleQueryStringQuery("value").field("f3_alias")).get();

        assertNoFailures(response);
        assertHitCount(response, 2);
        assertHits(response.getHits(), "2", "3");
    }

    public void testFieldAliasWithWildcardField() throws Exception {
        String indexBody = copyToStringFromClasspath("/org/elasticsearch/search/query/all-query-index.json");
        assertAcked(prepareCreate("test").setSource(indexBody, XContentType.JSON));
        ensureGreen("test");

        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        indexRequests.add(client().prepareIndex("test").setId("1").setSource("f3", "text", "f2", "one"));
        indexRequests.add(client().prepareIndex("test").setId("2").setSource("f3", "value", "f2", "two"));
        indexRequests.add(client().prepareIndex("test").setId("3").setSource("f3", "another value", "f2", "three"));
        indexRandom(true, false, indexRequests);

        SearchResponse response = client().prepareSearch("test").setQuery(simpleQueryStringQuery("value").field("f3_*")).get();

        assertNoFailures(response);
        assertHitCount(response, 2);
        assertHits(response.getHits(), "2", "3");
    }

    public void testFieldAliasOnDisallowedFieldType() throws Exception {
        String indexBody = copyToStringFromClasspath("/org/elasticsearch/search/query/all-query-index.json");
        assertAcked(prepareCreate("test").setSource(indexBody, XContentType.JSON));
        ensureGreen("test");

        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        indexRequests.add(client().prepareIndex("test").setId("1").setSource("f3", "text", "f2", "one"));
        indexRandom(true, false, indexRequests);

        // The wildcard field matches aliases for both a text and boolean field.
        // By default, the boolean field should be ignored when building the query.
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

    public static class MockAnalysisPlugin extends Plugin implements AnalysisPlugin {

        public final class MockSnowBall extends TokenFilter {
            private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

            /** Sole constructor. */
            MockSnowBall(TokenStream in) {
                super(in);
            }

            @Override
            public boolean incrementToken() throws IOException {
                if (input.incrementToken()) {
                    char[] buffer = termAtt.buffer();
                    if (buffer[termAtt.length() - 1] == 's') {
                        termAtt.setLength(termAtt.length() - 1);
                    }
                    return true;
                } else return false;
            }
        }

        @Override
        public List<PreConfiguredTokenFilter> getPreConfiguredTokenFilters() {
            return singletonList(PreConfiguredTokenFilter.singleton("mock_snowball", false, MockSnowBall::new));
        }
    }
}
