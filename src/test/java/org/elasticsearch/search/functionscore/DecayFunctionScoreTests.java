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

package org.elasticsearch.search.functionscore;

import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.index.query.MatchAllFilterBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.DecayFunctionBuilder;
import org.elasticsearch.index.query.functionscore.gauss.GaussDecayFunctionBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.client.Requests.searchRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.*;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.*;

public class DecayFunctionScoreTests extends ElasticsearchIntegrationTest {

    @Test
    public void testDistanceScoreGeoLinGaussExp() throws Exception {
        assertAcked(prepareCreate("test").addMapping(
                "type1",
                jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("test").field("type", "string")
                        .endObject().startObject("loc").field("type", "geo_point").endObject().endObject().endObject().endObject()));
        ensureYellow();

        List<IndexRequestBuilder> indexBuilders = new ArrayList<IndexRequestBuilder>();
        indexBuilders.add(client().prepareIndex()
                .setType("type1")
                .setId("1")
                .setIndex("test")
                .setSource(
                        jsonBuilder().startObject().field("test", "value").startObject("loc").field("lat", 10).field("lon", 20).endObject()
                                .endObject()));
        indexBuilders.add(client().prepareIndex()
                .setType("type1")
                .setId("2")
                .setIndex("test")
                .setSource(
                        jsonBuilder().startObject().field("test", "value").startObject("loc").field("lat", 11).field("lon", 22).endObject()
                                .endObject()));

        int numDummyDocs = 20;
        for (int i = 1; i <= numDummyDocs; i++) {
            indexBuilders.add(client().prepareIndex()
                    .setType("type1")
                    .setId(Integer.toString(i + 3))
                    .setIndex("test")
                    .setSource(
                            jsonBuilder().startObject().field("test", "value").startObject("loc").field("lat", 11 + i).field("lon", 22 + i)
                                    .endObject().endObject()));
        }
        IndexRequestBuilder[] builders = indexBuilders.toArray(new IndexRequestBuilder[indexBuilders.size()]);

        indexRandom(false, builders);
        refresh();

        // Test Gauss
        List<Float> lonlat = new ArrayList<Float>();
        lonlat.add(new Float(20));
        lonlat.add(new Float(11));

        ActionFuture<SearchResponse> response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(false).query(termQuery("test", "value"))));
        SearchResponse sr = response.actionGet();
        SearchHits sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo((long) (numDummyDocs + 2)));

        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(
                                functionScoreQuery(termQuery("test", "value"), gaussDecayFunction("loc", lonlat, "1000km")))));
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo((long) (numDummyDocs + 2)));

        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat(sh.getAt(1).getId(), equalTo("2"));
        // Test Exp

        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(false).query(termQuery("test", "value"))));
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo((long) (numDummyDocs + 2)));

        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(
                                functionScoreQuery(termQuery("test", "value"), linearDecayFunction("loc", lonlat, "1000km")))));
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo((long) (numDummyDocs + 2)));

        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat(sh.getAt(1).getId(), equalTo("2"));
        // Test Lin

        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(false).query(termQuery("test", "value"))));
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo((long) (numDummyDocs + 2)));

        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(
                                functionScoreQuery(termQuery("test", "value"), exponentialDecayFunction("loc", lonlat, "1000km")))));
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo((long) (numDummyDocs + 2)));

        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat(sh.getAt(1).getId(), equalTo("2"));
    }

    @Test
    public void testDistanceScoreGeoLinGaussExpWithOffset() throws Exception {
        assertAcked(prepareCreate("test").addMapping(
                "type1",
                jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("test").field("type", "string")
                        .endObject().startObject("num").field("type", "double").endObject().endObject().endObject().endObject()));
        ensureYellow();

        // add tw docs within offset
        List<IndexRequestBuilder> indexBuilders = new ArrayList<IndexRequestBuilder>();
        indexBuilders.add(client().prepareIndex().setType("type1").setId("1").setIndex("test")
                .setSource(jsonBuilder().startObject().field("test", "value").field("num", 0.5).endObject()));
        indexBuilders.add(client().prepareIndex().setType("type1").setId("2").setIndex("test")
                .setSource(jsonBuilder().startObject().field("test", "value").field("num", 1.7).endObject()));

        // add docs outside offset
        int numDummyDocs = 20;
        for (int i = 0; i < numDummyDocs; i++) {
            indexBuilders.add(client().prepareIndex().setType("type1").setId(Integer.toString(i + 3)).setIndex("test")
                    .setSource(jsonBuilder().startObject().field("test", "value").field("num", 3.0 + i).endObject()));
        }
        IndexRequestBuilder[] builders = indexBuilders.toArray(new IndexRequestBuilder[indexBuilders.size()]);

        indexRandom(false, builders);
        refresh();

        // Test Gauss

        ActionFuture<SearchResponse> response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource()
                                .explain(true)
                                .size(numDummyDocs + 2)
                                .query(functionScoreQuery(termQuery("test", "value"), gaussDecayFunction("num", 1.0, 5.0).setOffset(1.0))
                                        .boostMode(CombineFunction.REPLACE.getName()))));
        SearchResponse sr = response.actionGet();
        SearchHits sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo((long) (numDummyDocs + 2)));
        assertThat(sh.getAt(0).getId(), anyOf(equalTo("1"), equalTo("2")));
        assertThat(sh.getAt(1).getId(), anyOf(equalTo("1"), equalTo("2")));
        assertThat(sh.getAt(1).score(), equalTo(sh.getAt(0).score()));
        for (int i = 0; i < numDummyDocs; i++) {
            assertThat(sh.getAt(i + 2).getId(), equalTo(Integer.toString(i + 3)));
        }

        // Test Exp

        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource()
                                .explain(true)
                                .size(numDummyDocs + 2)
                                .query(functionScoreQuery(termQuery("test", "value"),
                                        exponentialDecayFunction("num", 1.0, 5.0).setOffset(1.0)).boostMode(
                                        CombineFunction.REPLACE.getName()))));
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo((long) (numDummyDocs + 2)));
        assertThat(sh.getAt(0).getId(), anyOf(equalTo("1"), equalTo("2")));
        assertThat(sh.getAt(1).getId(), anyOf(equalTo("1"), equalTo("2")));
        assertThat(sh.getAt(1).score(), equalTo(sh.getAt(0).score()));
        for (int i = 0; i < numDummyDocs; i++) {
            assertThat(sh.getAt(i + 2).getId(), equalTo(Integer.toString(i + 3)));
        }
        // Test Lin
        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource()
                                .explain(true)
                                .size(numDummyDocs + 2)
                                .query(functionScoreQuery(termQuery("test", "value"), linearDecayFunction("num", 1.0, 20.0).setOffset(1.0))
                                        .boostMode(CombineFunction.REPLACE.getName()))));
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo((long) (numDummyDocs + 2)));
        assertThat(sh.getAt(0).getId(), anyOf(equalTo("1"), equalTo("2")));
        assertThat(sh.getAt(1).getId(), anyOf(equalTo("1"), equalTo("2")));
        assertThat(sh.getAt(1).score(), equalTo(sh.getAt(0).score()));
    }

    @Test
    public void testBoostModeSettingWorks() throws Exception {
        assertAcked(prepareCreate("test").addMapping(
                "type1",
                jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("test").field("type", "string")
                        .endObject().startObject("loc").field("type", "geo_point").endObject().endObject().endObject().endObject()));
        ensureYellow();

        List<IndexRequestBuilder> indexBuilders = new ArrayList<IndexRequestBuilder>();
        indexBuilders.add(client().prepareIndex()
                .setType("type1")
                .setId("1")
                .setIndex("test")
                .setSource(
                        jsonBuilder().startObject().field("test", "value").startObject("loc").field("lat", 11).field("lon", 21).endObject()
                                .endObject()));
        indexBuilders.add(client().prepareIndex()
                .setType("type1")
                .setId("2")
                .setIndex("test")
                .setSource(
                        jsonBuilder().startObject().field("test", "value value").startObject("loc").field("lat", 11).field("lon", 20)
                                .endObject().endObject()));
        IndexRequestBuilder[] builders = indexBuilders.toArray(new IndexRequestBuilder[indexBuilders.size()]);

        indexRandom(false, builders);
        refresh();

        // Test Gauss
        List<Float> lonlat = new ArrayList<Float>();
        lonlat.add(new Float(20));
        lonlat.add(new Float(11));

        ActionFuture<SearchResponse> response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(
                                functionScoreQuery(termQuery("test", "value"), gaussDecayFunction("loc", lonlat, "1000km")).boostMode(
                                        CombineFunction.MULT.getName()))));
        SearchResponse sr = response.actionGet();
        SearchHits sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo((long) (2)));
        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat(sh.getAt(1).getId(), equalTo("2"));

        // Test Exp
        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(
                                functionScoreQuery(termQuery("test", "value"), gaussDecayFunction("loc", lonlat, "1000km")).boostMode(
                                        CombineFunction.REPLACE.getName()))));
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo((long) (2)));
        assertThat(sh.getAt(0).getId(), equalTo("2"));
        assertThat(sh.getAt(1).getId(), equalTo("1"));

    }

    @Test
    public void testParseGeoPoint() throws Exception {
        assertAcked(prepareCreate("test").addMapping(
                "type1",
                jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("test").field("type", "string")
                        .endObject().startObject("loc").field("type", "geo_point").endObject().endObject().endObject().endObject()));
        ensureYellow();

        List<IndexRequestBuilder> indexBuilders = new ArrayList<IndexRequestBuilder>();
        indexBuilders.add(client().prepareIndex()
                .setType("type1")
                .setId("1")
                .setIndex("test")
                .setSource(
                        jsonBuilder().startObject().field("test", "value").startObject("loc").field("lat", 20).field("lon", 11).endObject()
                                .endObject()));
        IndexRequestBuilder[] builders = indexBuilders.toArray(new IndexRequestBuilder[indexBuilders.size()]);

        indexRandom(false, builders);
        refresh();
        GeoPoint point = new GeoPoint(20, 11);
        ActionFuture<SearchResponse> response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(
                                functionScoreQuery(termQuery("test", "value"), gaussDecayFunction("loc", point, "1000km")).boostMode(
                                        CombineFunction.MULT.getName()))));
        SearchResponse sr = response.actionGet();
        SearchHits sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo((long) (1)));
        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat((double) sh.getAt(0).score(), closeTo(0.30685282, 1.e-5));
        float[] coords = { 11, 20 };

        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(
                                functionScoreQuery(termQuery("test", "value"), gaussDecayFunction("loc", coords, "1000km")).boostMode(
                                        CombineFunction.MULT.getName()))));
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo((long) (1)));
        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat((double) sh.getAt(0).score(), closeTo(0.30685282, 1.e-5));
    }

    @Test
    public void testCombineModes() throws Exception {

        assertAcked(prepareCreate("test").addMapping(
                "type1",
                jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("test").field("type", "string")
                        .endObject().startObject("num").field("type", "double").endObject().endObject().endObject().endObject()));
        ensureYellow();

        List<IndexRequestBuilder> indexBuilders = new ArrayList<IndexRequestBuilder>();
        indexBuilders.add(client().prepareIndex().setType("type1").setId("1").setIndex("test")
                .setSource(jsonBuilder().startObject().field("test", "value").field("num", 1.0).endObject()));
        IndexRequestBuilder[] builders = indexBuilders.toArray(new IndexRequestBuilder[indexBuilders.size()]);

        indexRandom(false, builders);
        refresh();

        // function score should return 0.5 for this function

        ActionFuture<SearchResponse> response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(
                                functionScoreQuery(termQuery("test", "value"), gaussDecayFunction("num", 0.0, 1.0).setDecay(0.5)).boost(
                                        2.0f).boostMode(CombineFunction.MULT))));
        SearchResponse sr = response.actionGet();
        SearchHits sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo((long) (1)));
        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat((double) sh.getAt(0).score(), closeTo(0.30685282, 1.e-5));
        logger.info("--> Hit[0] {} Explanation:\n {}", sr.getHits().getAt(0).id(), sr.getHits().getAt(0).explanation());

        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(
                                functionScoreQuery(termQuery("test", "value"), gaussDecayFunction("num", 0.0, 1.0).setDecay(0.5)).boost(
                                        2.0f).boostMode(CombineFunction.REPLACE))));
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo((long) (1)));
        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat((double) sh.getAt(0).score(), closeTo(1.0, 1.e-5));
        logger.info("--> Hit[0] {} Explanation:\n {}", sr.getHits().getAt(0).id(), sr.getHits().getAt(0).explanation());

        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(
                                functionScoreQuery(termQuery("test", "value"), gaussDecayFunction("num", 0.0, 1.0).setDecay(0.5)).boost(
                                        2.0f).boostMode(CombineFunction.SUM))));
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo((long) (1)));
        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat((double) sh.getAt(0).score(), closeTo(2.0 * (0.30685282 + 0.5), 1.e-5));
        logger.info("--> Hit[0] {} Explanation:\n {}", sr.getHits().getAt(0).id(), sr.getHits().getAt(0).explanation());

        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(
                                functionScoreQuery(termQuery("test", "value"), gaussDecayFunction("num", 0.0, 1.0).setDecay(0.5)).boost(
                                        2.0f).boostMode(CombineFunction.AVG))));
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo((long) (1)));
        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat((double) sh.getAt(0).score(), closeTo((0.30685282 + 0.5), 1.e-5));
        logger.info("--> Hit[0] {} Explanation:\n {}", sr.getHits().getAt(0).id(), sr.getHits().getAt(0).explanation());

        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(
                                functionScoreQuery(termQuery("test", "value"), gaussDecayFunction("num", 0.0, 1.0).setDecay(0.5)).boost(
                                        2.0f).boostMode(CombineFunction.MIN))));
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo((long) (1)));
        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat((double) sh.getAt(0).score(), closeTo(2.0 * (0.30685282), 1.e-5));
        logger.info("--> Hit[0] {} Explanation:\n {}", sr.getHits().getAt(0).id(), sr.getHits().getAt(0).explanation());

        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(
                                functionScoreQuery(termQuery("test", "value"), gaussDecayFunction("num", 0.0, 1.0).setDecay(0.5)).boost(
                                        2.0f).boostMode(CombineFunction.MAX))));
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo((long) (1)));
        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat((double) sh.getAt(0).score(), closeTo(1.0, 1.e-5));
        logger.info("--> Hit[0] {} Explanation:\n {}", sr.getHits().getAt(0).id(), sr.getHits().getAt(0).explanation());

    }

    @Test(expected = SearchPhaseExecutionException.class)
    public void testExceptionThrownIfScaleLE0() throws Exception {
        assertAcked(prepareCreate("test").addMapping(
                "type1",
                jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("test").field("type", "string")
                        .endObject().startObject("num1").field("type", "date").endObject().endObject().endObject().endObject()));
        ensureYellow();
        client().index(
                indexRequest("test").type("type1").id("1")
                        .source(jsonBuilder().startObject().field("test", "value").field("num1", "2013-05-27").endObject())).actionGet();
        client().index(
                indexRequest("test").type("type1").id("2")
                        .source(jsonBuilder().startObject().field("test", "value").field("num1", "2013-05-28").endObject())).actionGet();
        refresh();

        ActionFuture<SearchResponse> response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(
                                functionScoreQuery(termQuery("test", "value"), gaussDecayFunction("num1", "2013-05-28", "-1d")))));

        SearchResponse sr = response.actionGet();
        assertOrderedSearchHits(sr, "2", "1");
    }
    
    @Test
    public void testParseDateMath() throws Exception {
        
        assertAcked(prepareCreate("test").addMapping(
                "type1",
                jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("test").field("type", "string")
                        .endObject().startObject("num1").field("type", "date").endObject().endObject().endObject().endObject()));
        ensureYellow();
        client().index(
                indexRequest("test").type("type1").id("1")
                        .source(jsonBuilder().startObject().field("test", "value").field("num1", System.currentTimeMillis()).endObject())).actionGet();
        client().index(
                indexRequest("test").type("type1").id("2")
                        .source(jsonBuilder().startObject().field("test", "value").field("num1", System.currentTimeMillis() - (1000 * 60 * 60 * 24)).endObject())).actionGet();
        refresh();

        SearchResponse sr = client().search(
                searchRequest().source(
                        searchSource().explain(true).query(
                                functionScoreQuery(termQuery("test", "value"), gaussDecayFunction("num1", "now", "2d"))))).get();

        assertNoFailures(sr);
        assertOrderedSearchHits(sr, "1", "2");
        
        sr = client().search(
                searchRequest().source(
                        searchSource().explain(true).query(
                                functionScoreQuery(termQuery("test", "value"), gaussDecayFunction("num1", "now-1d", "2d"))))).get();

        assertNoFailures(sr);
        assertOrderedSearchHits(sr, "2", "1");

    }


    @Test(expected = ElasticSearchIllegalStateException.class)
    public void testExceptionThrownIfScaleRefNotBetween0And1() throws Exception {
        DecayFunctionBuilder gfb = new GaussDecayFunctionBuilder("num1", "2013-05-28", "1d").setDecay(100);

    }

    @Test
    public void testValueMissingLin() throws Exception {

        assertAcked(prepareCreate("test").addMapping(
                "type1",
                jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("test").field("type", "string")
                        .endObject().startObject("num1").field("type", "date").endObject().startObject("num2").field("type", "double")
                        .endObject().endObject().endObject().endObject()));
        ensureYellow();
        client().index(
                indexRequest("test")
                        .type("type1")
                        .id("1")
                        .source(jsonBuilder().startObject().field("test", "value").field("num1", "2013-05-27").field("num2", "1.0")
                                .endObject())).actionGet();
        client().index(
                indexRequest("test").type("type1").id("2")
                        .source(jsonBuilder().startObject().field("test", "value").field("num2", "1.0").endObject())).actionGet();
        client().index(
                indexRequest("test")
                        .type("type1")
                        .id("3")
                        .source(jsonBuilder().startObject().field("test", "value").field("num1", "2013-05-30").field("num2", "1.0")
                                .endObject())).actionGet();
        client().index(
                indexRequest("test").type("type1").id("4")
                        .source(jsonBuilder().startObject().field("test", "value").field("num1", "2013-05-30").endObject())).actionGet();

        refresh();

        ActionFuture<SearchResponse> response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(false).query(
                                functionScoreQuery(termQuery("test", "value")).add(linearDecayFunction("num1", "2013-05-28", "+3d"))
                                        .add(linearDecayFunction("num2", "0.0", "1")).scoreMode("multiply"))));

        SearchResponse sr = response.actionGet();
        assertNoFailures(sr);
        SearchHits sh = sr.getHits();
        assertThat(sh.hits().length, equalTo(4));
        double[] scores = new double[4];
        for (int i = 0; i < sh.hits().length; i++) {
            scores[Integer.parseInt(sh.getAt(i).getId()) - 1] = sh.getAt(i).getScore();
        }
        assertThat(scores[0], lessThan(scores[1]));
        assertThat(scores[2], lessThan(scores[3]));

    }

    @Test
    public void testDateWithoutOrigin() throws Exception {
        DateTime dt = new DateTime();

        assertAcked(prepareCreate("test").addMapping(
                "type1",
                jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("test").field("type", "string")
                        .endObject().startObject("num1").field("type", "date").endObject().endObject().endObject().endObject()));
        ensureYellow();

        DateTime docDate = dt.minusDays(1);
        String docDateString = docDate.getYear() + "-" + docDate.getMonthOfYear() + "-" + docDate.getDayOfMonth();
        client().index(
                indexRequest("test").type("type1").id("1")
                        .source(jsonBuilder().startObject().field("test", "value").field("num1", docDateString).endObject())).actionGet();
        docDate = dt.minusDays(2);
        docDateString = docDate.getYear() + "-" + docDate.getMonthOfYear() + "-" + docDate.getDayOfMonth();
        client().index(
                indexRequest("test").type("type1").id("2")
                        .source(jsonBuilder().startObject().field("test", "value").field("num1", docDateString).endObject())).actionGet();
        docDate = dt.minusDays(3);
        docDateString = docDate.getYear() + "-" + docDate.getMonthOfYear() + "-" + docDate.getDayOfMonth();
        client().index(
                indexRequest("test").type("type1").id("3")
                        .source(jsonBuilder().startObject().field("test", "value").field("num1", docDateString).endObject())).actionGet();

        refresh();

        ActionFuture<SearchResponse> response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(false).query(
                                functionScoreQuery(QueryBuilders.matchAllQuery()).add(linearDecayFunction("num1", "1000w"))
                                        .add(gaussDecayFunction("num1", "1d")).add(exponentialDecayFunction("num1", "1000w"))
                                        .scoreMode("multiply"))));

        SearchResponse sr = response.actionGet();
        assertNoFailures(sr);
        SearchHits sh = sr.getHits();
        assertThat(sh.hits().length, equalTo(3));
        double[] scores = new double[4];
        for (int i = 0; i < sh.hits().length; i++) {
            scores[Integer.parseInt(sh.getAt(i).getId()) - 1] = sh.getAt(i).getScore();
        }
        assertThat(scores[1], lessThan(scores[0]));
        assertThat(scores[2], lessThan(scores[1]));

    }

    @Test
    public void testManyDocsLin() throws Exception {
        assertAcked(prepareCreate("test").addMapping(
                "type",
                jsonBuilder().startObject().startObject("type").startObject("properties").startObject("test").field("type", "string")
                        .endObject().startObject("date").field("type", "date").endObject().startObject("num").field("type", "double")
                        .endObject().startObject("geo").field("type", "geo_point").endObject().endObject().endObject().endObject()));
        ensureYellow();
        int numDocs = 200;
        List<IndexRequestBuilder> indexBuilders = new ArrayList<IndexRequestBuilder>();

        for (int i = 0; i < numDocs; i++) {
            double lat = 100 + (int) (10.0 * (float) (i) / (float) (numDocs));
            double lon = 100;
            int day = (int) (29.0 * (float) (i) / (float) (numDocs)) + 1;
            String dayString = day < 10 ? "0" + Integer.toString(day) : Integer.toString(day);
            String date = "2013-05-" + dayString;

            indexBuilders.add(client().prepareIndex()
                    .setType("type")
                    .setId(Integer.toString(i))
                    .setIndex("test")
                    .setSource(
                            jsonBuilder().startObject().field("test", "value").field("date", date).field("num", i).startObject("geo")
                                    .field("lat", lat).field("lon", lon).endObject().endObject()));
        }
        IndexRequestBuilder[] builders = indexBuilders.toArray(new IndexRequestBuilder[indexBuilders.size()]);
        indexRandom(true, builders);
        List<Float> lonlat = new ArrayList<Float>();
        lonlat.add(new Float(100));
        lonlat.add(new Float(110));
        ActionFuture<SearchResponse> response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().size(numDocs).query(
                                functionScoreQuery(termQuery("test", "value"))
                                        .add(new MatchAllFilterBuilder(), linearDecayFunction("date", "2013-05-30", "+15d"))
                                        .add(new MatchAllFilterBuilder(), linearDecayFunction("geo", lonlat, "1000km"))
                                        .add(new MatchAllFilterBuilder(), linearDecayFunction("num", numDocs, numDocs / 2.0))
                                        .scoreMode("multiply").boostMode(CombineFunction.REPLACE.getName()))));

        SearchResponse sr = response.actionGet();
        assertNoFailures(sr);
        SearchHits sh = sr.getHits();
        assertThat(sh.hits().length, equalTo(numDocs));
        double[] scores = new double[numDocs];
        for (int i = 0; i < numDocs; i++) {
            scores[Integer.parseInt(sh.getAt(i).getId())] = sh.getAt(i).getScore();
        }
        for (int i = 0; i < numDocs - 1; i++) {
            assertThat(scores[i], lessThan(scores[i + 1]));

        }

    }

    @Test(expected = SearchPhaseExecutionException.class)
    public void testParsingExceptionIfFieldDoesNotExist() throws Exception {
        assertAcked(prepareCreate("test").addMapping(
                "type",
                jsonBuilder().startObject().startObject("type").startObject("properties").startObject("test").field("type", "string")
                        .endObject().startObject("geo").field("type", "geo_point").endObject().endObject().endObject().endObject()));
        ensureYellow();
        int numDocs = 2;
        client().index(
                indexRequest("test").type("type1").source(
                        jsonBuilder().startObject().field("test", "value").startObject("geo").field("lat", 1).field("lon", 2).endObject()
                                .endObject())).actionGet();
        refresh();
        List<Float> lonlat = new ArrayList<Float>();
        lonlat.add(new Float(100));
        lonlat.add(new Float(110));
        ActionFuture<SearchResponse> response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource()
                                .explain(true)
                                .size(numDocs)
                                .query(functionScoreQuery(termQuery("test", "value")).add(new MatchAllFilterBuilder(),
                                        linearDecayFunction("type1.geo", lonlat, "1000km")).scoreMode("multiply"))));
        SearchResponse sr = response.actionGet();

    }

    @Test(expected = SearchPhaseExecutionException.class)
    public void testParsingExceptionIfFieldTypeDoesNotMatch() throws Exception {
        assertAcked(prepareCreate("test").addMapping(
                "type",
                jsonBuilder().startObject().startObject("type").startObject("properties").startObject("test").field("type", "string")
                        .endObject().startObject("num").field("type", "string").endObject().endObject().endObject().endObject()));
        ensureYellow();
        client().index(
                indexRequest("test").type("type").source(
                        jsonBuilder().startObject().field("test", "value").field("num", Integer.toString(1)).endObject())).actionGet();
        refresh();
        // so, we indexed a string field, but now we try to score a num field
        ActionFuture<SearchResponse> response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(
                                functionScoreQuery(termQuery("test", "value")).add(new MatchAllFilterBuilder(),
                                        linearDecayFunction("num", 1.0, 0.5)).scoreMode("multiply"))));
        response.actionGet();
    }

    @Test
    public void testNoQueryGiven() throws Exception {
        assertAcked(prepareCreate("test").addMapping(
                "type",
                jsonBuilder().startObject().startObject("type").startObject("properties").startObject("test").field("type", "string")
                        .endObject().startObject("num").field("type", "double").endObject().endObject().endObject().endObject()));
        ensureYellow();
        client().index(
                indexRequest("test").type("type").source(jsonBuilder().startObject().field("test", "value").field("num", 1.0).endObject()))
                .actionGet();
        refresh();
        // so, we indexed a string field, but now we try to score a num field
        ActionFuture<SearchResponse> response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(
                                functionScoreQuery().add(new MatchAllFilterBuilder(), linearDecayFunction("num", 1, 0.5)).scoreMode(
                                        "multiply"))));
        response.actionGet();
    }

}
