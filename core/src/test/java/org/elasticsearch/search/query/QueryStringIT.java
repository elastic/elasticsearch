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

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class QueryStringIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(InternalSettingsPlugin.class); // uses index.version.created
    }

    @Before
    public void setup() throws Exception {
        String indexBody = copyToStringFromClasspath("/org/elasticsearch/search/query/all-query-index.json");
        prepareCreate("test").setSource(indexBody, XContentType.JSON).get();
        ensureGreen("test");
    }

    public void testBasicAllQuery() throws Exception {
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        reqs.add(client().prepareIndex("test", "doc", "1").setSource("f1", "foo bar baz"));
        reqs.add(client().prepareIndex("test", "doc", "2").setSource("f2", "Bar"));
        reqs.add(client().prepareIndex("test", "doc", "3").setSource("f3", "foo bar baz"));
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
        reqs.add(client().prepareIndex("test", "doc", "1").setSource("f1", "foo", "f_date", "2015/09/02"));
        reqs.add(client().prepareIndex("test", "doc", "2").setSource("f1", "bar", "f_date", "2015/09/01"));
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
        reqs.add(client().prepareIndex("test", "doc", "1").setSource("f1", "foo",
                        "f_date", "2015/09/02",
                        "f_float", "1.7",
                        "f_ip", "127.0.0.1"));
        reqs.add(client().prepareIndex("test", "doc", "2").setSource("f1", "bar",
                        "f_date", "2015/09/01",
                        "f_float", "1.8",
                        "f_ip", "127.0.0.2"));
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
        reqs.add(client().prepareIndex("test", "doc", "1").setSource(docBody, XContentType.JSON));
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
        reqs.add(client().prepareIndex("test", "doc", "1").setSource("f2", "Foo Bar"));
        reqs.add(client().prepareIndex("test", "doc", "2").setSource("f1", "bar"));
        reqs.add(client().prepareIndex("test", "doc", "3").setSource("f1", "foo bar"));
        indexRandom(true, false, reqs);

        SearchResponse resp = client().prepareSearch("test").setQuery(queryStringQuery("foo")).get();
        assertHits(resp.getHits(), "3");
        assertHitCount(resp, 1L);

        resp = client().prepareSearch("test").setQuery(queryStringQuery("bar")).get();
        assertHits(resp.getHits(), "2", "3");
        assertHitCount(resp, 2L);

        resp = client().prepareSearch("test")
                .setQuery(queryStringQuery("Foo Bar"))
                .get();
        assertHits(resp.getHits(), "1", "2", "3");
        assertHitCount(resp, 3L);
    }

    public void testAllFields() throws Exception {
        String indexBodyWithAll = copyToStringFromClasspath("/org/elasticsearch/search/query/all-query-index-with-all.json");
        String indexBody = copyToStringFromClasspath("/org/elasticsearch/search/query/all-query-index.json");

        // Defaults to index.query.default_field=_all
        prepareCreate("test_1").setSource(indexBodyWithAll, XContentType.JSON).get();
        Settings.Builder settings = Settings.builder().put("index.query.default_field", "*");
        prepareCreate("test_2").setSource(indexBody, XContentType.JSON).setSettings(settings).get();
        ensureGreen("test_1","test_2");

        List<IndexRequestBuilder> reqs = new ArrayList<>();
        reqs.add(client().prepareIndex("test_1", "doc", "1").setSource("f1", "foo", "f2", "eggplant"));
        reqs.add(client().prepareIndex("test_2", "doc", "1").setSource("f1", "foo", "f2", "eggplant"));
        indexRandom(true, false, reqs);

        SearchResponse resp = client().prepareSearch("test_1").setQuery(
            queryStringQuery("foo eggplant").defaultOperator(Operator.AND)).get();
        assertHitCount(resp, 0L);

        resp = client().prepareSearch("test_2").setQuery(
            queryStringQuery("foo eggplant").defaultOperator(Operator.AND)).get();
        assertHitCount(resp, 0L);

        resp = client().prepareSearch("test_1").setQuery(
            queryStringQuery("foo eggplant").defaultOperator(Operator.OR)).get();
        assertHits(resp.getHits(), "1");
        assertHitCount(resp, 1L);

        resp = client().prepareSearch("test_2").setQuery(
            queryStringQuery("foo eggplant").defaultOperator(Operator.OR)).get();
        assertHits(resp.getHits(), "1");
        assertHitCount(resp, 1L);
    }


    public void testPhraseQueryOnFieldWithNoPositions() throws Exception {
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        reqs.add(client().prepareIndex("test", "doc", "1").setSource("f1", "foo bar", "f4", "eggplant parmesan"));
        reqs.add(client().prepareIndex("test", "doc", "2").setSource("f1", "foo bar", "f4", "chicken parmesan"));
        indexRandom(true, false, reqs);

        SearchResponse resp = client().prepareSearch("test")
            .setQuery(queryStringQuery("\"eggplant parmesan\"").lenient(true)).get();
        assertHitCount(resp, 0L);

        Exception exc = expectThrows(Exception.class,
            () -> client().prepareSearch("test").setQuery(
                queryStringQuery("f4:\"eggplant parmesan\"").lenient(false)
            ).get()
        );
        IllegalStateException ise = (IllegalStateException) ExceptionsHelper.unwrap(exc, IllegalStateException.class);
        assertNotNull(ise);
        assertThat(ise.getMessage(), containsString("field:[f4] was indexed without position data; cannot run PhraseQuery"));
    }

    public void testBooleanStrictQuery() throws Exception {
        Exception e = expectThrows(Exception.class, () ->
                client().prepareSearch("test").setQuery(
                        queryStringQuery("foo").field("f_bool")).get());
        assertThat(ExceptionsHelper.detailedMessage(e),
                containsString("Can't parse boolean value [foo], expected [true] or [false]"));
    }

    public void testAllFieldsWithSpecifiedLeniency() throws IOException {
        Exception e = expectThrows(Exception.class, () ->
                client().prepareSearch("test").setQuery(
                        queryStringQuery("f_date:[now-2D TO now]").lenient(false)).get());
        assertThat(ExceptionsHelper.detailedMessage(e),
                containsString("unit [D] not supported for date math [-2D]"));
    }

    private void setupIndexWithGraph(String index) throws Exception {
        CreateIndexRequestBuilder builder = prepareCreate(index).setSettings(
            Settings.builder()
                .put(indexSettings())
                .put("index.analysis.filter.graphsyns.type", "synonym_graph")
                .putList("index.analysis.filter.graphsyns.synonyms", "wtf, what the fudge", "foo, bar baz")
                .put("index.analysis.analyzer.lower_graphsyns.type", "custom")
                .put("index.analysis.analyzer.lower_graphsyns.tokenizer", "standard")
                .putList("index.analysis.analyzer.lower_graphsyns.filter", "lowercase", "graphsyns")
        );

        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject(index).startObject("properties")
            .startObject("field").field("type", "text").endObject().endObject().endObject().endObject();

        assertAcked(builder.addMapping(index, mapping));
        ensureGreen();

        List<IndexRequestBuilder> builders = new ArrayList<>();
        builders.add(client().prepareIndex(index, index, "1").setSource("field", "say wtf happened foo"));
        builders.add(client().prepareIndex(index, index, "2").setSource("field", "bar baz what the fudge man"));
        builders.add(client().prepareIndex(index, index, "3").setSource("field", "wtf"));
        builders.add(client().prepareIndex(index, index, "4").setSource("field", "what is the name for fudge"));
        builders.add(client().prepareIndex(index, index, "5").setSource("field", "bar two three"));
        builders.add(client().prepareIndex(index, index, "6").setSource("field", "bar baz two three"));

        indexRandom(true, false, builders);
    }

    public void testGraphQueries() throws Exception {
        String index = "graph_test_index";
        setupIndexWithGraph(index);

        // phrase
        SearchResponse searchResponse = client().prepareSearch(index).setQuery(
            QueryBuilders.queryStringQuery("\"foo two three\"")
                .defaultField("field")
                .analyzer("lower_graphsyns")).get();

        assertHitCount(searchResponse, 1L);
        assertSearchHits(searchResponse, "6");

        // and
        searchResponse = client().prepareSearch(index).setQuery(
            QueryBuilders.queryStringQuery("say what the fudge")
                .defaultField("field")
                .defaultOperator(Operator.AND)
                .autoGenerateSynonymsPhraseQuery(false)
                .analyzer("lower_graphsyns")).get();

        assertHitCount(searchResponse, 1L);
        assertSearchHits(searchResponse, "1");

        // or
        searchResponse = client().prepareSearch(index).setQuery(
            QueryBuilders.queryStringQuery("three what the fudge foo")
                .defaultField("field")
                .defaultOperator(Operator.OR)
                .autoGenerateSynonymsPhraseQuery(false)
                .analyzer("lower_graphsyns")).get();

        assertHitCount(searchResponse, 6L);
        assertSearchHits(searchResponse, "1", "2", "3", "4", "5", "6");

        // min should match
        searchResponse = client().prepareSearch(index).setQuery(
            QueryBuilders.queryStringQuery("three what the fudge foo")
                .defaultField("field")
                .defaultOperator(Operator.OR)
                .autoGenerateSynonymsPhraseQuery(false)
                .analyzer("lower_graphsyns")
                .minimumShouldMatch("80%")).get();

        assertHitCount(searchResponse, 3L);
        assertSearchHits(searchResponse, "1", "2", "6");

        // multi terms synonyms phrase
        searchResponse = client().prepareSearch(index).setQuery(
            QueryBuilders.queryStringQuery("what the fudge")
                .defaultField("field")
                .defaultOperator(Operator.AND)
                .analyzer("lower_graphsyns"))
            .get();
        assertHitCount(searchResponse, 3L);
        assertSearchHits(searchResponse,  "1", "2", "3");
    }

    private void assertHits(SearchHits hits, String... ids) {
        assertThat(hits.getTotalHits(), equalTo((long) ids.length));
        Set<String> hitIds = new HashSet<>();
        for (SearchHit hit : hits.getHits()) {
            hitIds.add(hit.getId());
        }
        assertThat(hitIds, containsInAnyOrder(ids));
    }
}
