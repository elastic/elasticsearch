/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.search.customscore;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.Priority;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.termFilter;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.factorFunction;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.scriptFunction;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class CustomScoreSearchTests extends ElasticsearchIntegrationTest {

    @Test
    public void testScoreExplainBug_2283() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(settingsBuilder().put("index.number_of_shards", 1)).execute()
                .actionGet();
        ClusterHealthResponse healthResponse = client().admin().cluster().prepareHealth("test").setWaitForYellowStatus().execute()
                .actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        client().prepareIndex("test", "type", "1").setSource("field", "value1", "color", "red").execute().actionGet();
        client().prepareIndex("test", "type", "2").setSource("field", "value2", "color", "blue").execute().actionGet();
        client().prepareIndex("test", "type", "3").setSource("field", "value3", "color", "red").execute().actionGet();
        client().prepareIndex("test", "type", "4").setSource("field", "value4", "color", "blue").execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client()
                .prepareSearch("test")
                .setQuery(
                        customFiltersScoreQuery(matchAllQuery()).add(termFilter("field", "value4"), "2")
                                .add(termFilter("field", "value2"), "3").scoreMode("first")).setExplain(true).execute().actionGet();

        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().totalHits(), equalTo(4l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(0).score(), equalTo(3.0f));
        logger.info("--> Hit[0] {} Explanation:\n {}", searchResponse.getHits().getAt(0).id(), searchResponse.getHits().getAt(0)
                .explanation());
        Explanation explanation = searchResponse.getHits().getAt(0).explanation();
        assertNotNull(explanation);
        assertThat(explanation.isMatch(), equalTo(true));
        assertThat(explanation.getValue(), equalTo(3f));
        assertThat(explanation.getDescription(), equalTo("function score, product of:"));

        assertThat(explanation.getDetails().length, equalTo(3));
        assertThat(explanation.getDetails()[0].isMatch(), equalTo(true));
        assertThat(explanation.getDetails()[0].getValue(), equalTo(1f));
        assertThat(explanation.getDetails()[0].getDetails().length, equalTo(2));
        assertThat(explanation.getDetails()[1].isMatch(), equalTo(true));
        assertThat(explanation.getDetails()[1].getValue(), equalTo(3f));
        assertThat(explanation.getDetails()[1].getDetails().length, equalTo(2));

        // Same query but with boost
        searchResponse = client()
                .prepareSearch("test")
                .setQuery(
                        customFiltersScoreQuery(matchAllQuery()).add(termFilter("field", "value4"), "2")
                                .add(termFilter("field", "value2"), "3").boost(2).scoreMode("first")).setExplain(true).execute()
                .actionGet();

        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().totalHits(), equalTo(4l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(0).score(), equalTo(6f));
        logger.info("--> Hit[0] {} Explanation:\n {}", searchResponse.getHits().getAt(0).id(), searchResponse.getHits().getAt(0)
                .explanation());
        explanation = searchResponse.getHits().getAt(0).explanation();
        assertNotNull(explanation);
        assertThat(explanation.isMatch(), equalTo(true));
        assertThat(explanation.getValue(), equalTo(6f));
        assertThat(explanation.getDescription(), equalTo("function score, product of:"));

        assertThat(explanation.getDetails().length, equalTo(3));
        assertThat(explanation.getDetails()[0].isMatch(), equalTo(true));
        assertThat(explanation.getDetails()[0].getValue(), equalTo(1f));
        assertThat(explanation.getDetails()[0].getDetails().length, equalTo(2));
        assertThat(explanation.getDetails()[1].isMatch(), equalTo(true));
        assertThat(explanation.getDetails()[1].getValue(), equalTo(3f));
        assertThat(explanation.getDetails()[1].getDetails().length, equalTo(2));
        assertThat(explanation.getDetails()[2].getDescription(), equalTo("queryBoost"));
        assertThat(explanation.getDetails()[2].getValue(), equalTo(2f));
    }

    @Test
    public void testScoreExplainBug_2283_withFunctionScore() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(settingsBuilder().put("index.number_of_shards", 1)).execute()
                .actionGet();
        ClusterHealthResponse healthResponse = client().admin().cluster().prepareHealth("test").setWaitForYellowStatus().execute()
                .actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        client().prepareIndex("test", "type", "1").setSource("field", "value1", "color", "red").execute().actionGet();
        client().prepareIndex("test", "type", "2").setSource("field", "value2", "color", "blue").execute().actionGet();
        client().prepareIndex("test", "type", "3").setSource("field", "value3", "color", "red").execute().actionGet();
        client().prepareIndex("test", "type", "4").setSource("field", "value4", "color", "blue").execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client()
                .prepareSearch("test")
                .setQuery(
                        functionScoreQuery(matchAllQuery()).scoreMode("first").add(termFilter("field", "value4"), scriptFunction("2"))
                                .add(termFilter("field", "value2"), scriptFunction("3"))).setExplain(true).execute().actionGet();

        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));

        assertThat(searchResponse.getHits().totalHits(), equalTo(4l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(0).score(), equalTo(3.0f));
        logger.info("--> Hit[0] {} Explanation:\n {}", searchResponse.getHits().getAt(0).id(), searchResponse.getHits().getAt(0)
                .explanation());
        Explanation explanation = searchResponse.getHits().getAt(0).explanation();
        assertNotNull(explanation);
        assertThat(explanation.isMatch(), equalTo(true));
        assertThat(explanation.getValue(), equalTo(3f));
        assertThat(explanation.getDescription(), equalTo("function score, product of:"));
        assertThat(explanation.getDetails().length, equalTo(3));
        assertThat(explanation.getDetails()[0].isMatch(), equalTo(true));
        assertThat(explanation.getDetails()[0].getValue(), equalTo(1f));
        assertThat(explanation.getDetails()[0].getDetails().length, equalTo(2));
        assertThat(explanation.getDetails()[1].isMatch(), equalTo(true));
        assertThat(explanation.getDetails()[1].getValue(), equalTo(3f));
        assertThat(explanation.getDetails()[1].getDetails().length, equalTo(2));

        // Same query but with boost
        searchResponse = client()
                .prepareSearch("test")
                .setQuery(
                        functionScoreQuery(matchAllQuery()).scoreMode("first").add(termFilter("field", "value4"), scriptFunction("2"))
                                .add(termFilter("field", "value2"), scriptFunction("3")).boost(2)).setExplain(true).execute().actionGet();

        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));

        assertThat(searchResponse.getHits().totalHits(), equalTo(4l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(0).score(), equalTo(6f));
        logger.info("--> Hit[0] {} Explanation:\n {}", searchResponse.getHits().getAt(0).id(), searchResponse.getHits().getAt(0)
                .explanation());
        explanation = searchResponse.getHits().getAt(0).explanation();
        assertNotNull(explanation);
        assertThat(explanation.isMatch(), equalTo(true));
        assertThat(explanation.getValue(), equalTo(6f));
        assertThat(explanation.getDescription(), equalTo("function score, product of:"));

        assertThat(explanation.getDetails().length, equalTo(3));
        assertThat(explanation.getDetails()[0].isMatch(), equalTo(true));
        assertThat(explanation.getDetails()[0].getValue(), equalTo(1f));
        assertThat(explanation.getDetails()[0].getDetails().length, equalTo(2));
        assertThat(explanation.getDetails()[1].isMatch(), equalTo(true));
        assertThat(explanation.getDetails()[1].getValue(), equalTo(3f));
        assertThat(explanation.getDetails()[1].getDetails().length, equalTo(2));
        assertThat(explanation.getDetails()[2].getDescription(), equalTo("queryBoost"));
        assertThat(explanation.getDetails()[2].getValue(), equalTo(2f));
    }

    @Test
    public void testMultiValueCustomScriptBoost() throws ElasticSearchException, IOException {

        client().admin()
                .indices()
                .prepareCreate("test")
                .setSettings(settingsBuilder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .addMapping(
                        "type",
                        jsonBuilder().startObject().startObject("type").startObject("properties").startObject("snum")
                                .field("type", "string").endObject().startObject("dnum").field("type", "double").endObject()
                                .startObject("slnum").field("type", "long").endObject().startObject("gp").field("type", "geo_point")
                                .endObject().endObject().endObject().endObject()).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        String[] values = new String[100];
        String[] gp = new String[100];

        long[] lValues = new long[100];
        double[] dValues = new double[100];
        int offset = 1;
        for (int i = 0; i < values.length; i++) {
            values[i] = "" + (i + offset);
            gp[i] = "" + (i + offset) + "," + (i + offset);
            lValues[i] = (i + offset);
            dValues[i] = (i + offset);
        }
        client().index(
                indexRequest("test")
                        .type("type1")
                        .id("1")
                        .source(jsonBuilder().startObject().field("test", "value check").field("snum", values).field("dnum", dValues)
                                .field("lnum", lValues).field("gp", gp).endObject())).actionGet();
        offset++;
        for (int i = 0; i < values.length; i++) {
            values[i] = "" + (i + offset);
            gp[i] = "" + (i + offset) + "," + (i + offset);
            lValues[i] = (i + offset);
            dValues[i] = (i + offset);
        }
        client().index(
                indexRequest("test")
                        .type("type1")
                        .id("2")
                        .source(jsonBuilder().startObject().field("test", "value check").field("snum", values).field("dnum", dValues)
                                .field("lnum", lValues).field("gp", gp).endObject())).actionGet();
        client().admin().indices().refresh(refreshRequest()).actionGet();

        logger.info("running min(doc['num1'].value)");
        SearchResponse response = client()
                .search(searchRequest()
                        .searchType(SearchType.QUERY_THEN_FETCH)
                        .source(searchSource()
                                .explain(true)
                                .query(customScoreQuery(termQuery("test", "value"))
                                        .script("c_min = 1000; foreach (x : doc['snum'].values) { c_min = min(Integer.parseInt(x), c_min) } return c_min"))))
                .actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).id(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).id(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).id(), equalTo("2"));
        assertThat(response.getHits().getAt(1).id(), equalTo("1"));

        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(
                                customScoreQuery(termQuery("test", "value")).script(
                                        "c_min = 1000; foreach (x : doc['lnum'].values) { c_min = min(x, c_min) } return c_min"))))
                .actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).id(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).id(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).id(), equalTo("2"));
        assertThat(response.getHits().getAt(1).id(), equalTo("1"));

        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(
                                customScoreQuery(termQuery("test", "value")).script(
                                        "c_min = 1000; foreach (x : doc['dnum'].values) { c_min = min(x, c_min) } return c_min"))))
                .actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).id(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).id(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).id(), equalTo("2"));
        assertThat(response.getHits().getAt(1).id(), equalTo("1"));

        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(
                                customScoreQuery(termQuery("test", "value")).script(
                                        "c_min = 1000; foreach (x : doc['gp'].values) { c_min = min(x.lat, c_min) } return c_min"))))
                .actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).id(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).id(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).id(), equalTo("2"));
        assertThat(response.getHits().getAt(1).id(), equalTo("1"));
    }

    @Test
    public void testMultiValueCustomScriptBoost_withFunctionScore() throws ElasticSearchException, IOException {

        client().admin()
                .indices()
                .prepareCreate("test")
                .setSettings(settingsBuilder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .addMapping(
                        "type",
                        jsonBuilder().startObject().startObject("type").startObject("properties").startObject("snum")
                                .field("type", "string").endObject().startObject("dnum").field("type", "double").endObject()
                                .startObject("slnum").field("type", "long").endObject().startObject("gp").field("type", "geo_point")
                                .endObject().endObject().endObject().endObject()).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        String[] values = new String[100];
        String[] gp = new String[100];

        long[] lValues = new long[100];
        double[] dValues = new double[100];
        int offset = 1;
        for (int i = 0; i < values.length; i++) {
            values[i] = "" + (i + offset);
            gp[i] = "" + (i + offset) + "," + (i + offset);
            lValues[i] = (i + offset);
            dValues[i] = (i + offset);
        }
        client().index(
                indexRequest("test")
                        .type("type1")
                        .id("1")
                        .source(jsonBuilder().startObject().field("test", "value check").field("snum", values).field("dnum", dValues)
                                .field("lnum", lValues).field("gp", gp).endObject())).actionGet();
        offset++;
        for (int i = 0; i < values.length; i++) {
            values[i] = "" + (i + offset);
            gp[i] = "" + (i + offset) + "," + (i + offset);
            lValues[i] = (i + offset);
            dValues[i] = (i + offset);
        }
        client().index(
                indexRequest("test")
                        .type("type1")
                        .id("2")
                        .source(jsonBuilder().startObject().field("test", "value check").field("snum", values).field("dnum", dValues)
                                .field("lnum", lValues).field("gp", gp).endObject())).actionGet();
        client().admin().indices().refresh(refreshRequest()).actionGet();

        logger.info("running min(doc['num1'].value)");
        SearchResponse response = client()
                .search(searchRequest()
                        .searchType(SearchType.QUERY_THEN_FETCH)
                        .source(searchSource()
                                .explain(true)
                                .query(functionScoreQuery(
                                        termQuery("test", "value"),
                                        scriptFunction("c_min = 1000; foreach (x : doc['snum'].values) { c_min = min(Integer.parseInt(x), c_min) } return c_min")))))
                .actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).id(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).id(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).id(), equalTo("2"));
        assertThat(response.getHits().getAt(1).id(), equalTo("1"));

        response = client()
                .search(searchRequest()
                        .searchType(SearchType.QUERY_THEN_FETCH)
                        .source(searchSource()
                                .explain(true)
                                .query(functionScoreQuery(
                                        termQuery("test", "value"),
                                        scriptFunction("c_min = 1000; foreach (x : doc['lnum'].values) { c_min = min(x, c_min) } return c_min")))))
                .actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).id(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).id(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).id(), equalTo("2"));
        assertThat(response.getHits().getAt(1).id(), equalTo("1"));

        response = client()
                .search(searchRequest()
                        .searchType(SearchType.QUERY_THEN_FETCH)
                        .source(searchSource()
                                .explain(true)
                                .query(functionScoreQuery(
                                        termQuery("test", "value"),
                                        scriptFunction("c_min = 1000; foreach (x : doc['dnum'].values) { c_min = min(x, c_min) } return c_min")))))
                .actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).id(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).id(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).id(), equalTo("2"));
        assertThat(response.getHits().getAt(1).id(), equalTo("1"));

        response = client()
                .search(searchRequest()
                        .searchType(SearchType.QUERY_THEN_FETCH)
                        .source(searchSource()
                                .explain(true)
                                .query(functionScoreQuery(
                                        termQuery("test", "value"),
                                        scriptFunction("c_min = 1000; foreach (x : doc['gp'].values) { c_min = min(x.lat, c_min) } return c_min")))))
                .actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).id(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).id(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).id(), equalTo("2"));
        assertThat(response.getHits().getAt(1).id(), equalTo("1"));
    }

    @Test
    public void testCustomScriptBoost() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(settingsBuilder().put("index.number_of_shards", 1)).execute()
                .actionGet();

        client().index(
                indexRequest("test").type("type1").id("1")
                        .source(jsonBuilder().startObject().field("test", "value beck").field("num1", 1.0f).endObject())).actionGet();
        client().index(
                indexRequest("test").type("type1").id("2")
                        .source(jsonBuilder().startObject().field("test", "value check").field("num1", 2.0f).endObject())).actionGet();
        client().admin().indices().refresh(refreshRequest()).actionGet();

        logger.info("--- QUERY_THEN_FETCH");

        logger.info("running doc['num1'].value");
        SearchResponse response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(customScoreQuery(termQuery("test", "value")).script("doc['num1'].value"))))
                .actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).id(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).id(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).id(), equalTo("2"));
        assertThat(response.getHits().getAt(1).id(), equalTo("1"));

        logger.info("running -doc['num1'].value");
        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(customScoreQuery(termQuery("test", "value")).script("-doc['num1'].value"))))
                .actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).id(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).id(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).id(), equalTo("1"));
        assertThat(response.getHits().getAt(1).id(), equalTo("2"));

        logger.info("running pow(doc['num1'].value, 2)");
        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true)
                                .query(customScoreQuery(termQuery("test", "value")).script("pow(doc['num1'].value, 2)")))).actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).id(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).id(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).id(), equalTo("2"));
        assertThat(response.getHits().getAt(1).id(), equalTo("1"));

        logger.info("running max(doc['num1'].value, 1)");
        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(
                                customScoreQuery(termQuery("test", "value")).script("max(doc['num1'].value, 1d)")))).actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).id(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).id(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).id(), equalTo("2"));
        assertThat(response.getHits().getAt(1).id(), equalTo("1"));

        logger.info("running doc['num1'].value * _score");
        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(
                                customScoreQuery(termQuery("test", "value")).script("doc['num1'].value * _score")))).actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).id(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).id(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).id(), equalTo("2"));
        assertThat(response.getHits().getAt(1).id(), equalTo("1"));

        logger.info("running param1 * param2 * _score");
        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(
                                customScoreQuery(termQuery("test", "value")).script("param1 * param2 * _score").param("param1", 2)
                                        .param("param2", 2)))).actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).id(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).id(), response.getHits().getAt(1).explanation());
        assertSearchHits(response, "1", "2");

        logger.info("running param1 * param2 * _score with filter instead of query");
        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(
                                customScoreQuery(termFilter("test", "value")).script("param1 * param2 * _score").param("param1", 2)
                                        .param("param2", 2)))).actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).id(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).id(), response.getHits().getAt(1).explanation());
        assertSearchHits(response, "1", "2");
        assertThat(response.getHits().getAt(0).score(), equalTo(4f)); // _score
                                                                      // is
                                                                      // always
                                                                      // 1
        assertThat(response.getHits().getAt(1).score(), equalTo(4f)); // _score
                                                                      // is
                                                                      // always
                                                                      // 1
    }

    @Test
    public void testCustomScriptBoost_withFunctionScore() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(settingsBuilder().put("index.number_of_shards", 1)).execute()
                .actionGet();

        client().index(
                indexRequest("test").type("type1").id("1")
                        .source(jsonBuilder().startObject().field("test", "value beck").field("num1", 1.0f).endObject())).actionGet();
        client().index(
                indexRequest("test").type("type1").id("2")
                        .source(jsonBuilder().startObject().field("test", "value check").field("num1", 2.0f).endObject())).actionGet();
        client().admin().indices().refresh(refreshRequest()).actionGet();

        logger.info("--- QUERY_THEN_FETCH");

        logger.info("running doc['num1'].value");
        SearchResponse response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(
                                functionScoreQuery(termQuery("test", "value"), scriptFunction("doc['num1'].value"))))).actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).id(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).id(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).id(), equalTo("2"));
        assertThat(response.getHits().getAt(1).id(), equalTo("1"));

        logger.info("running -doc['num1'].value");
        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(
                                functionScoreQuery(termQuery("test", "value"), scriptFunction("-doc['num1'].value"))))).actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).id(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).id(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).id(), equalTo("1"));
        assertThat(response.getHits().getAt(1).id(), equalTo("2"));

        logger.info("running pow(doc['num1'].value, 2)");
        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(
                                functionScoreQuery(termQuery("test", "value"), scriptFunction("pow(doc['num1'].value, 2)"))))).actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).id(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).id(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).id(), equalTo("2"));
        assertThat(response.getHits().getAt(1).id(), equalTo("1"));

        logger.info("running max(doc['num1'].value, 1)");
        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(
                                functionScoreQuery(termQuery("test", "value"), scriptFunction("max(doc['num1'].value, 1d)"))))).actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).id(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).id(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).id(), equalTo("2"));
        assertThat(response.getHits().getAt(1).id(), equalTo("1"));

        logger.info("running doc['num1'].value * _score");
        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(
                                functionScoreQuery(termQuery("test", "value"), scriptFunction("doc['num1'].value * _score"))))).actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).id(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).id(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).id(), equalTo("2"));
        assertThat(response.getHits().getAt(1).id(), equalTo("1"));

        logger.info("running param1 * param2 * _score");
        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(
                                functionScoreQuery(termQuery("test", "value"), scriptFunction("param1 * param2 * _score")
                                        .param("param1", 2).param("param2", 2))))).actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).id(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).id(), response.getHits().getAt(1).explanation());
        assertSearchHits(response, "1", "2");

        logger.info("running param1 * param2 * _score with filter instead of query");
        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(
                                functionScoreQuery(termFilter("test", "value"),
                                        scriptFunction("param1 * param2 * _score").param("param1", 2).param("param2", 2))))).actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).id(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).id(), response.getHits().getAt(1).explanation());
        assertSearchHits(response, "1", "2");
        assertThat(response.getHits().getAt(0).score(), equalTo(4f)); // _score
                                                                      // is
                                                                      // always
                                                                      // 1
        assertThat(response.getHits().getAt(1).score(), equalTo(4f)); // _score
                                                                      // is
                                                                      // always
                                                                      // 1
    }

    @Test
    public void testTriggerBooleanScorer() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(settingsBuilder().put("index.number_of_shards", 1)).execute()
                .actionGet();

        client().prepareIndex("test", "type", "1").setSource("field", "value1", "color", "red").execute().actionGet();
        client().prepareIndex("test", "type", "2").setSource("field", "value2", "color", "blue").execute().actionGet();
        client().prepareIndex("test", "type", "3").setSource("field", "value3", "color", "red").execute().actionGet();
        client().prepareIndex("test", "type", "4").setSource("field", "value4", "color", "blue").execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();
        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(customFiltersScoreQuery(fuzzyQuery("field", "value")).add(FilterBuilders.idsFilter("type").addIds("1"), 3))
                .execute().actionGet();
        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().totalHits(), equalTo(4l));
    }

    @Test
    public void testTriggerBooleanScorer_withFunctionScore() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(settingsBuilder().put("index.number_of_shards", 1)).execute()
                .actionGet();

        client().prepareIndex("test", "type", "1").setSource("field", "value1", "color", "red").execute().actionGet();
        client().prepareIndex("test", "type", "2").setSource("field", "value2", "color", "blue").execute().actionGet();
        client().prepareIndex("test", "type", "3").setSource("field", "value3", "color", "red").execute().actionGet();
        client().prepareIndex("test", "type", "4").setSource("field", "value4", "color", "blue").execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();
        SearchResponse searchResponse = client()
                .prepareSearch("test")
                .setQuery(
                        functionScoreQuery(fuzzyQuery("field", "value")).add(FilterBuilders.idsFilter("type").addIds("1"),
                                factorFunction(3))).execute().actionGet();
        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));

        assertThat(searchResponse.getHits().totalHits(), equalTo(4l));
    }

    @Test
    public void testCustomFiltersScore() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(settingsBuilder().put("index.number_of_shards", 1)).execute()
                .actionGet();

        client().prepareIndex("test", "type", "1").setSource("field", "value1", "color", "red").execute().actionGet();
        client().prepareIndex("test", "type", "2").setSource("field", "value2", "color", "blue").execute().actionGet();
        client().prepareIndex("test", "type", "3").setSource("field", "value3", "color", "red").execute().actionGet();
        client().prepareIndex("test", "type", "4").setSource("field", "value4", "color", "blue").execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client()
                .prepareSearch("test")
                .setQuery(
                        customFiltersScoreQuery(matchAllQuery()).add(termFilter("field", "value4"), "2").add(termFilter("field", "value2"),
                                "3")).setExplain(true).execute().actionGet();

        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().totalHits(), equalTo(4l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(0).score(), equalTo(3.0f));
        logger.info("--> Hit[0] {} Explanation {}", searchResponse.getHits().getAt(0).id(), searchResponse.getHits().getAt(0).explanation());
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("4"));
        assertThat(searchResponse.getHits().getAt(1).score(), equalTo(2.0f));
        assertThat(searchResponse.getHits().getAt(2).id(), anyOf(equalTo("1"), equalTo("3")));
        assertThat(searchResponse.getHits().getAt(2).score(), equalTo(1.0f));
        assertThat(searchResponse.getHits().getAt(3).id(), anyOf(equalTo("1"), equalTo("3")));
        assertThat(searchResponse.getHits().getAt(3).score(), equalTo(1.0f));

        searchResponse = client()
                .prepareSearch("test")
                .setQuery(
                        customFiltersScoreQuery(matchAllQuery()).add(termFilter("field", "value4"), 2)
                                .add(termFilter("field", "value2"), 3)).setExplain(true).execute().actionGet();

        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().totalHits(), equalTo(4l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(0).score(), equalTo(3.0f));
        logger.info("--> Hit[0] {} Explanation {}", searchResponse.getHits().getAt(0).id(), searchResponse.getHits().getAt(0).explanation());
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("4"));
        assertThat(searchResponse.getHits().getAt(1).score(), equalTo(2.0f));
        assertThat(searchResponse.getHits().getAt(2).id(), anyOf(equalTo("1"), equalTo("3")));
        assertThat(searchResponse.getHits().getAt(2).score(), equalTo(1.0f));
        assertThat(searchResponse.getHits().getAt(3).id(), anyOf(equalTo("1"), equalTo("3")));
        assertThat(searchResponse.getHits().getAt(3).score(), equalTo(1.0f));

        searchResponse = client()
                .prepareSearch("test")
                .setQuery(
                        customFiltersScoreQuery(matchAllQuery()).scoreMode("total").add(termFilter("field", "value4"), 2)
                                .add(termFilter("field", "value1"), 3).add(termFilter("color", "red"), 5)).setExplain(true).execute()
                .actionGet();

        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(4l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(0).score(), equalTo(8.0f));
        logger.info("--> Hit[0] {} Explanation {}", searchResponse.getHits().getAt(0).id(), searchResponse.getHits().getAt(0).explanation());

        searchResponse = client()
                .prepareSearch("test")
                .setQuery(
                        customFiltersScoreQuery(matchAllQuery()).scoreMode("max").add(termFilter("field", "value4"), 2)
                                .add(termFilter("field", "value1"), 3).add(termFilter("color", "red"), 5)).setExplain(true).execute()
                .actionGet();

        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(4l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("1"), equalTo("3"))); // could
                                                                                               // be
                                                                                               // both
                                                                                               // depending
                                                                                               // on
                                                                                               // the
                                                                                               // order
                                                                                               // of
                                                                                               // the
                                                                                               // docs
                                                                                               // internally
                                                                                               // (lucene
                                                                                               // order)
        assertThat(searchResponse.getHits().getAt(0).score(), equalTo(5.0f));
        logger.info("--> Hit[0] {} Explanation {}", searchResponse.getHits().getAt(0).id(), searchResponse.getHits().getAt(0).explanation());

        searchResponse = client()
                .prepareSearch("test")
                .setQuery(
                        customFiltersScoreQuery(matchAllQuery()).scoreMode("avg").add(termFilter("field", "value4"), 2)
                                .add(termFilter("field", "value1"), 3).add(termFilter("color", "red"), 5)).setExplain(true).execute()
                .actionGet();

        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(4l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(0).score(), equalTo(5.0f));
        logger.info("--> Hit[0] {} Explanation {}", searchResponse.getHits().getAt(0).id(), searchResponse.getHits().getAt(0).explanation());
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).score(), equalTo(4.0f));
        logger.info("--> Hit[1] {} Explanation {}", searchResponse.getHits().getAt(1).id(), searchResponse.getHits().getAt(1).explanation());

        searchResponse = client()
                .prepareSearch("test")
                .setQuery(
                        customFiltersScoreQuery(matchAllQuery()).scoreMode("min").add(termFilter("field", "value4"), 2)
                                .add(termFilter("field", "value1"), 3).add(termFilter("color", "red"), 5)).setExplain(true).execute()
                .actionGet();

        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(4l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(0).score(), equalTo(5.0f));
        logger.info("--> Hit[0] {} Explanation {}", searchResponse.getHits().getAt(0).id(), searchResponse.getHits().getAt(0).explanation());
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).score(), equalTo(3.0f));
        assertThat(searchResponse.getHits().getAt(2).id(), equalTo("4"));
        assertThat(searchResponse.getHits().getAt(2).score(), equalTo(2.0f));
        assertThat(searchResponse.getHits().getAt(3).id(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(3).score(), equalTo(1.0f));

        searchResponse = client()
                .prepareSearch("test")
                .setQuery(
                        customFiltersScoreQuery(matchAllQuery()).scoreMode("multiply").add(termFilter("field", "value4"), 2)
                                .add(termFilter("field", "value1"), 3).add(termFilter("color", "red"), 5)).setExplain(true).execute()
                .actionGet();

        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(4l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(0).score(), equalTo(15.0f));
        logger.info("--> Hit[0] {} Explanation {}", searchResponse.getHits().getAt(0).id(), searchResponse.getHits().getAt(0).explanation());
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(1).score(), equalTo(5.0f));
        assertThat(searchResponse.getHits().getAt(2).id(), equalTo("4"));
        assertThat(searchResponse.getHits().getAt(2).score(), equalTo(2.0f));
        assertThat(searchResponse.getHits().getAt(3).id(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(3).score(), equalTo(1.0f));

        searchResponse = client()
                .prepareSearch("test")
                .setQuery(
                        customFiltersScoreQuery(termsQuery("field", "value1", "value2", "value3", "value4")).scoreMode("first")
                                .add(termFilter("field", "value4"), 2).add(termFilter("field", "value3"), 3)
                                .add(termFilter("field", "value2"), 4)).setExplain(true).execute().actionGet();

        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(4l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(0).score(), equalTo(searchResponse.getHits().getAt(0).explanation().getValue()));
        logger.info("--> Hit[0] {} Explanation {}", searchResponse.getHits().getAt(0).id(), searchResponse.getHits().getAt(0).explanation());
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(1).score(), equalTo(searchResponse.getHits().getAt(1).explanation().getValue()));
        assertThat(searchResponse.getHits().getAt(2).id(), equalTo("4"));
        assertThat(searchResponse.getHits().getAt(2).score(), equalTo(searchResponse.getHits().getAt(2).explanation().getValue()));
        assertThat(searchResponse.getHits().getAt(3).id(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(3).score(), equalTo(searchResponse.getHits().getAt(3).explanation().getValue()));

        searchResponse = client()
                .prepareSearch("test")
                .setQuery(
                        customFiltersScoreQuery(termsQuery("field", "value1", "value2", "value3", "value4")).scoreMode("multiply")
                                .add(termFilter("field", "value4"), 2).add(termFilter("field", "value1"), 3)
                                .add(termFilter("color", "red"), 5)).setExplain(true).execute().actionGet();

        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(4l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(0).score(), equalTo(searchResponse.getHits().getAt(0).explanation().getValue()));
        logger.info("--> Hit[0] {} Explanation {}", searchResponse.getHits().getAt(0).id(), searchResponse.getHits().getAt(0).explanation());
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(1).score(), equalTo(searchResponse.getHits().getAt(1).explanation().getValue()));
        assertThat(searchResponse.getHits().getAt(2).id(), equalTo("4"));
        assertThat(searchResponse.getHits().getAt(2).score(), equalTo(searchResponse.getHits().getAt(2).explanation().getValue()));
        assertThat(searchResponse.getHits().getAt(3).id(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(3).score(), equalTo(searchResponse.getHits().getAt(3).explanation().getValue()));
    }

    @Test
    public void testCustomFiltersScore_withFunctionScore() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(settingsBuilder().put("index.number_of_shards", 1)).execute()
                .actionGet();

        client().prepareIndex("test", "type", "1").setSource("field", "value1", "color", "red").execute().actionGet();
        client().prepareIndex("test", "type", "2").setSource("field", "value2", "color", "blue").execute().actionGet();
        client().prepareIndex("test", "type", "3").setSource("field", "value3", "color", "red").execute().actionGet();
        client().prepareIndex("test", "type", "4").setSource("field", "value4", "color", "blue").execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client()
                .prepareSearch("test")
                .setQuery(
                        functionScoreQuery(matchAllQuery())
                                .add(termFilter("field", "value4"), scriptFunction("2")).add(
                                        termFilter("field", "value2"), scriptFunction("3"))).setExplain(true)
                .execute().actionGet();

        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));

        assertThat(searchResponse.getHits().totalHits(), equalTo(4l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(0).score(), equalTo(3.0f));
        logger.info("--> Hit[0] {} Explanation {}", searchResponse.getHits().getAt(0).id(), searchResponse.getHits().getAt(0).explanation());
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("4"));
        assertThat(searchResponse.getHits().getAt(1).score(), equalTo(2.0f));
        assertThat(searchResponse.getHits().getAt(2).id(), anyOf(equalTo("1"), equalTo("3")));
        assertThat(searchResponse.getHits().getAt(2).score(), equalTo(1.0f));
        assertThat(searchResponse.getHits().getAt(3).id(), anyOf(equalTo("1"), equalTo("3")));
        assertThat(searchResponse.getHits().getAt(3).score(), equalTo(1.0f));

        searchResponse = client()
                .prepareSearch("test")
                .setQuery(
                        functionScoreQuery(matchAllQuery()).add(termFilter("field", "value4"), factorFunction(2)).add(
                                termFilter("field", "value2"), factorFunction(3))).setExplain(true).execute().actionGet();

        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));

        assertThat(searchResponse.getHits().totalHits(), equalTo(4l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(0).score(), equalTo(3.0f));
        logger.info("--> Hit[0] {} Explanation {}", searchResponse.getHits().getAt(0).id(), searchResponse.getHits().getAt(0).explanation());
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("4"));
        assertThat(searchResponse.getHits().getAt(1).score(), equalTo(2.0f));
        assertThat(searchResponse.getHits().getAt(2).id(), anyOf(equalTo("1"), equalTo("3")));
        assertThat(searchResponse.getHits().getAt(2).score(), equalTo(1.0f));
        assertThat(searchResponse.getHits().getAt(3).id(), anyOf(equalTo("1"), equalTo("3")));
        assertThat(searchResponse.getHits().getAt(3).score(), equalTo(1.0f));

        searchResponse = client()
                .prepareSearch("test")
                .setQuery(
                        functionScoreQuery(matchAllQuery()).scoreMode("sum")
                                .add(termFilter("field", "value4"), factorFunction(2))
                                .add(termFilter("field", "value1"), factorFunction(3))
                                .add(termFilter("color", "red"), factorFunction(5))).setExplain(true).execute()
                .actionGet();

        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(4l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(0).score(), equalTo(8.0f));
        logger.info("--> Hit[0] {} Explanation {}", searchResponse.getHits().getAt(0).id(), searchResponse.getHits().getAt(0).explanation());

        searchResponse = client()
                .prepareSearch("test")
                .setQuery(
                        functionScoreQuery(matchAllQuery()).scoreMode("max")
                                .add(termFilter("field", "value4"), factorFunction(2))
                                .add(termFilter("field", "value1"), factorFunction(3))
                                .add(termFilter("color", "red"), factorFunction(5))).setExplain(true).execute()
                .actionGet();

        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(4l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("1"), equalTo("3"))); // could
                                                                                               // be
                                                                                               // both
                                                                                               // depending
                                                                                               // on
                                                                                               // the
                                                                                               // order
                                                                                               // of
                                                                                               // the
                                                                                               // docs
                                                                                               // internally
                                                                                               // (lucene
                                                                                               // order)
        assertThat(searchResponse.getHits().getAt(0).score(), equalTo(5.0f));
        logger.info("--> Hit[0] {} Explanation {}", searchResponse.getHits().getAt(0).id(), searchResponse.getHits().getAt(0).explanation());

        searchResponse = client()
                .prepareSearch("test")
                .setQuery(
                        functionScoreQuery(matchAllQuery()).scoreMode("avg")
                                .add(termFilter("field", "value4"), factorFunction(2))
                                .add(termFilter("field", "value1"), factorFunction(3))
                                .add(termFilter("color", "red"), factorFunction(5))).setExplain(true).execute()
                .actionGet();

        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(4l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(0).score(), equalTo(5.0f));
        logger.info("--> Hit[0] {} Explanation {}", searchResponse.getHits().getAt(0).id(), searchResponse.getHits().getAt(0).explanation());
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).score(), equalTo(4.0f));
        logger.info("--> Hit[1] {} Explanation {}", searchResponse.getHits().getAt(1).id(), searchResponse.getHits().getAt(1).explanation());

        searchResponse = client()
                .prepareSearch("test")
                .setQuery(
                        functionScoreQuery(matchAllQuery()).scoreMode("min")
                                .add(termFilter("field", "value4"), factorFunction(2))
                                .add(termFilter("field", "value1"), factorFunction(3))
                                .add(termFilter("color", "red"), factorFunction(5))).setExplain(true).execute()
                .actionGet();

        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(4l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(0).score(), equalTo(5.0f));
        logger.info("--> Hit[0] {} Explanation {}", searchResponse.getHits().getAt(0).id(), searchResponse.getHits().getAt(0).explanation());
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).score(), equalTo(3.0f));
        assertThat(searchResponse.getHits().getAt(2).id(), equalTo("4"));
        assertThat(searchResponse.getHits().getAt(2).score(), equalTo(2.0f));
        assertThat(searchResponse.getHits().getAt(3).id(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(3).score(), equalTo(1.0f));

        searchResponse = client()
                .prepareSearch("test")
                .setQuery(
                        functionScoreQuery(matchAllQuery()).scoreMode("multiply")
                                .add(termFilter("field", "value4"), factorFunction(2))
                                .add(termFilter("field", "value1"), factorFunction(3))
                                .add(termFilter("color", "red"), factorFunction(5))).setExplain(true).execute()
                .actionGet();

        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(4l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(0).score(), equalTo(15.0f));
        logger.info("--> Hit[0] {} Explanation {}", searchResponse.getHits().getAt(0).id(), searchResponse.getHits().getAt(0).explanation());
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(1).score(), equalTo(5.0f));
        assertThat(searchResponse.getHits().getAt(2).id(), equalTo("4"));
        assertThat(searchResponse.getHits().getAt(2).score(), equalTo(2.0f));
        assertThat(searchResponse.getHits().getAt(3).id(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(3).score(), equalTo(1.0f));

        searchResponse = client()
                .prepareSearch("test")
                .setQuery(
                        functionScoreQuery(termsQuery("field", "value1", "value2", "value3", "value4")).scoreMode("first")
                                .add(termFilter("field", "value4"), factorFunction(2))
                                .add(termFilter("field", "value3"), factorFunction(3))
                                .add(termFilter("field", "value2"), factorFunction(4))).setExplain(true).execute()
                .actionGet();

        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(4l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(0).score(), equalTo(searchResponse.getHits().getAt(0).explanation().getValue()));
        logger.info("--> Hit[0] {} Explanation {}", searchResponse.getHits().getAt(0).id(), searchResponse.getHits().getAt(0).explanation());
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(1).score(), equalTo(searchResponse.getHits().getAt(1).explanation().getValue()));
        assertThat(searchResponse.getHits().getAt(2).id(), equalTo("4"));
        assertThat(searchResponse.getHits().getAt(2).score(), equalTo(searchResponse.getHits().getAt(2).explanation().getValue()));
        assertThat(searchResponse.getHits().getAt(3).id(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(3).score(), equalTo(searchResponse.getHits().getAt(3).explanation().getValue()));

        searchResponse = client()
                .prepareSearch("test")
                .setQuery(
                        functionScoreQuery(termsQuery("field", "value1", "value2", "value3", "value4")).scoreMode("multiply")
                                .add(termFilter("field", "value4"), factorFunction(2))
                                .add(termFilter("field", "value1"), factorFunction(3))
                                .add(termFilter("color", "red"), factorFunction(5))).setExplain(true).execute()
                .actionGet();

        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(4l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(0).score(), equalTo(searchResponse.getHits().getAt(0).explanation().getValue()));
        logger.info("--> Hit[0] {} Explanation {}", searchResponse.getHits().getAt(0).id(), searchResponse.getHits().getAt(0).explanation());
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(1).score(), equalTo(searchResponse.getHits().getAt(1).explanation().getValue()));
        assertThat(searchResponse.getHits().getAt(2).id(), equalTo("4"));
        assertThat(searchResponse.getHits().getAt(2).score(), equalTo(searchResponse.getHits().getAt(2).explanation().getValue()));
        assertThat(searchResponse.getHits().getAt(3).id(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(3).score(), equalTo(searchResponse.getHits().getAt(3).explanation().getValue()));
    }
}
