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

package org.elasticsearch.search.functionscore;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.DecayFunctionBuilder;
import org.elasticsearch.index.query.functionscore.gauss.GaussDecayFunctionBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESIntegTestCase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.Locale;

import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.client.Requests.searchRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.*;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.*;


public class DecayFunctionScoreIT extends ESIntegTestCase {

    @Test
    public void testDistanceScoreGeoLinGaussExp() throws Exception {
        assertAcked(prepareCreate("test").addMapping(
                "type1",
                jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("test").field("type", "string")
                        .endObject().startObject("loc").field("type", "geo_point").endObject().endObject().endObject().endObject()));
        ensureYellow();

        List<IndexRequestBuilder> indexBuilders = new ArrayList<>();
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

        indexRandom(true, indexBuilders);

        // Test Gauss
        List<Float> lonlat = new ArrayList<>();
        lonlat.add(20f);
        lonlat.add(11f);

        ActionFuture<SearchResponse> response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().query(constantScoreQuery(termQuery("test", "value")))));
        SearchResponse sr = response.actionGet();
        SearchHits sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo((long) (numDummyDocs + 2)));

        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().query(
                                functionScoreQuery(constantScoreQuery(termQuery("test", "value")), gaussDecayFunction("loc", lonlat, "1000km")))));
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo((long) (numDummyDocs + 2)));

        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat(sh.getAt(1).getId(), equalTo("2"));
        // Test Exp

        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().query(constantScoreQuery(termQuery("test", "value")))));
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo((long) (numDummyDocs + 2)));

        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().query(
                                functionScoreQuery(constantScoreQuery(termQuery("test", "value")), linearDecayFunction("loc", lonlat, "1000km")))));
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo((long) (numDummyDocs + 2)));

        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat(sh.getAt(1).getId(), equalTo("2"));
        // Test Lin

        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().query(constantScoreQuery(termQuery("test", "value")))));
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo((long) (numDummyDocs + 2)));

        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().query(
                                functionScoreQuery(constantScoreQuery(termQuery("test", "value")), exponentialDecayFunction("loc", lonlat, "1000km")))));
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
        List<IndexRequestBuilder> indexBuilders = new ArrayList<>();
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

        indexRandom(true, indexBuilders);

        // Test Gauss

        ActionFuture<SearchResponse> response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource()
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

        List<IndexRequestBuilder> indexBuilders = new ArrayList<>();
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
        indexRandom(true, false, indexBuilders); // force no dummy docs

        // Test Gauss
        List<Float> lonlat = new ArrayList<>();
        lonlat.add(20f);
        lonlat.add(11f);

        ActionFuture<SearchResponse> response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().query(
                                functionScoreQuery(termQuery("test", "value"), gaussDecayFunction("loc", lonlat, "1000km")).boostMode(
                                        CombineFunction.MULT.getName()))));
        SearchResponse sr = response.actionGet();
        SearchHits sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo((long) (2)));
        assertThat(sh.getAt(0).getId(), isOneOf("1"));
        assertThat(sh.getAt(1).getId(), equalTo("2"));

        // Test Exp
        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().query(
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

        client().prepareIndex()
                .setType("type1")
                .setId("1")
                .setIndex("test")
                .setSource(
                        jsonBuilder().startObject().field("test", "value").startObject("loc").field("lat", 20).field("lon", 11).endObject()
                                .endObject()).setRefresh(true).get();

        GeoPoint point = new GeoPoint(20, 11);
        ActionFuture<SearchResponse> response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().query(
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
                        searchSource().query(
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

        client().prepareIndex().setType("type1").setId("1").setIndex("test")
                .setSource(jsonBuilder().startObject().field("test", "value").field("num", 1.0).endObject()).setRefresh(true).get();

        // function score should return 0.5 for this function

        ActionFuture<SearchResponse> response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().query(
                                functionScoreQuery(termQuery("test", "value"), gaussDecayFunction("num", 0.0, 1.0).setDecay(0.5)).boost(
                                        2.0f).boostMode(CombineFunction.MULT))));
        SearchResponse sr = response.actionGet();
        SearchHits sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo((long) (1)));
        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat((double) sh.getAt(0).score(), closeTo(0.153426408, 1.e-5));

        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().query(
                                functionScoreQuery(termQuery("test", "value"), gaussDecayFunction("num", 0.0, 1.0).setDecay(0.5)).boost(
                                        2.0f).boostMode(CombineFunction.REPLACE))));
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo((long) (1)));
        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat((double) sh.getAt(0).score(), closeTo(0.5, 1.e-5));

        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().query(
                                functionScoreQuery(termQuery("test", "value"), gaussDecayFunction("num", 0.0, 1.0).setDecay(0.5)).boost(
                                        2.0f).boostMode(CombineFunction.SUM))));
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo((long) (1)));
        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat((double) sh.getAt(0).score(), closeTo(0.30685282 + 0.5, 1.e-5));
        logger.info("--> Hit[0] {} Explanation:\n {}", sr.getHits().getAt(0).id(), sr.getHits().getAt(0).explanation());

        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().query(
                                functionScoreQuery(termQuery("test", "value"), gaussDecayFunction("num", 0.0, 1.0).setDecay(0.5)).boost(
                                        2.0f).boostMode(CombineFunction.AVG))));
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo((long) (1)));
        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat((double) sh.getAt(0).score(), closeTo((0.30685282 + 0.5) / 2, 1.e-5));

        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().query(
                                functionScoreQuery(termQuery("test", "value"), gaussDecayFunction("num", 0.0, 1.0).setDecay(0.5)).boost(
                                        2.0f).boostMode(CombineFunction.MIN))));
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo((long) (1)));
        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat((double) sh.getAt(0).score(), closeTo(0.30685282, 1.e-5));

        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().query(
                                functionScoreQuery(termQuery("test", "value"), gaussDecayFunction("num", 0.0, 1.0).setDecay(0.5)).boost(
                                        2.0f).boostMode(CombineFunction.MAX))));
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo((long) (1)));
        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat((double) sh.getAt(0).score(), closeTo(0.5, 1.e-5));

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
                        searchSource().query(
                                functionScoreQuery(termQuery("test", "value"), gaussDecayFunction("num1", "2013-05-28", "-1d")))));

        SearchResponse sr = response.actionGet();
        assertOrderedSearchHits(sr, "2", "1");
    }
    
    @Test
    public void testParseDateMath() throws Exception {
        
        assertAcked(prepareCreate("test").addMapping(
                "type1",
                jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("test").field("type", "string")
                        .endObject().startObject("num1").field("type", "date").field("format", "epoch_millis").endObject().endObject().endObject().endObject()));
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
                        searchSource().query(
                                functionScoreQuery(termQuery("test", "value"), gaussDecayFunction("num1", "now", "2d"))))).get();

        assertNoFailures(sr);
        assertOrderedSearchHits(sr, "1", "2");
        
        sr = client().search(
                searchRequest().source(
                        searchSource().query(
                                functionScoreQuery(termQuery("test", "value"), gaussDecayFunction("num1", "now-1d", "2d"))))).get();

        assertNoFailures(sr);
        assertOrderedSearchHits(sr, "2", "1");

    }

    @Test(expected = IllegalStateException.class)
    public void testExceptionThrownIfScaleRefNotBetween0And1() throws Exception {
        DecayFunctionBuilder gfb = new GaussDecayFunctionBuilder("num1", "2013-05-28", "1d").setDecay(100);
    }

    @Test
    public void testValueMissingLin() throws Exception {

        assertAcked(prepareCreate("test").addMapping(
                "type1",
                jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("test").field("type", "string")
                        .endObject().startObject("num1").field("type", "date").endObject().startObject("num2").field("type", "double")
                        .endObject().endObject().endObject().endObject())
        );

        ensureYellow();

        client().index(
                indexRequest("test").type("type1").id("1")
                        .source(jsonBuilder().startObject().field("test", "value").field("num1", "2013-05-27").field("num2", "1.0")
                                .endObject())).actionGet();
        client().index(
                indexRequest("test").type("type1").id("2")
                        .source(jsonBuilder().startObject().field("test", "value").field("num2", "1.0").endObject())).actionGet();
        client().index(
                indexRequest("test").type("type1").id("3")
                        .source(jsonBuilder().startObject().field("test", "value").field("num1", "2013-05-30").field("num2", "1.0")
                                .endObject())).actionGet();
        client().index(
                indexRequest("test").type("type1").id("4")
                        .source(jsonBuilder().startObject().field("test", "value").field("num1", "2013-05-30").endObject())).actionGet();

        refresh();

        ActionFuture<SearchResponse> response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().query(
                                functionScoreQuery(constantScoreQuery(termQuery("test", "value"))).add(linearDecayFunction("num1", "2013-05-28", "+3d"))
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
        DateTime dt = new DateTime(DateTimeZone.UTC);

        assertAcked(prepareCreate("test").addMapping(
                "type1",
                jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("test").field("type", "string")
                        .endObject().startObject("num1").field("type", "date").endObject().endObject().endObject().endObject()));
        ensureYellow();

        DateTime docDate = dt.minusDays(1);
        String docDateString = docDate.getYear() + "-" + String.format(Locale.ROOT, "%02d", docDate.getMonthOfYear()) + "-" + String.format(Locale.ROOT, "%02d", docDate.getDayOfMonth());
        client().index(
                indexRequest("test").type("type1").id("1")
                        .source(jsonBuilder().startObject().field("test", "value").field("num1", docDateString).endObject())).actionGet();
        docDate = dt.minusDays(2);
        docDateString = docDate.getYear() + "-" + String.format(Locale.ROOT, "%02d", docDate.getMonthOfYear()) + "-" + String.format(Locale.ROOT, "%02d", docDate.getDayOfMonth());
        client().index(
                indexRequest("test").type("type1").id("2")
                        .source(jsonBuilder().startObject().field("test", "value").field("num1", docDateString).endObject())).actionGet();
        docDate = dt.minusDays(3);
        docDateString = docDate.getYear() + "-" + String.format(Locale.ROOT, "%02d", docDate.getMonthOfYear()) + "-" + String.format(Locale.ROOT, "%02d", docDate.getDayOfMonth());
        client().index(
                indexRequest("test").type("type1").id("3")
                        .source(jsonBuilder().startObject().field("test", "value").field("num1", docDateString).endObject())).actionGet();

        refresh();

        ActionFuture<SearchResponse> response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().query(
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
                        .endObject().startObject("geo").field("type", "geo_point").field("coerce", true).endObject().endObject()
                        .endObject().endObject()));
        ensureYellow();
        int numDocs = 200;
        List<IndexRequestBuilder> indexBuilders = new ArrayList<>();

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
        indexRandom(true, indexBuilders);
        List<Float> lonlat = new ArrayList<>();
        lonlat.add(100f);
        lonlat.add(110f);
        ActionFuture<SearchResponse> response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().size(numDocs).query(
                                functionScoreQuery(termQuery("test", "value"))
                                        .add(new MatchAllQueryBuilder(), linearDecayFunction("date", "2013-05-30", "+15d"))
                                        .add(new MatchAllQueryBuilder(), linearDecayFunction("geo", lonlat, "1000km"))
                                        .add(new MatchAllQueryBuilder(), linearDecayFunction("num", numDocs, numDocs / 2.0))
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
        List<Float> lonlat = new ArrayList<>();
        lonlat.add(100f);
        lonlat.add(110f);
        ActionFuture<SearchResponse> response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource()
                                .size(numDocs)
                                .query(functionScoreQuery(termQuery("test", "value")).add(new MatchAllQueryBuilder(),
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
                        searchSource().query(
                                functionScoreQuery(termQuery("test", "value")).add(new MatchAllQueryBuilder(),
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
                        searchSource().query(
                                functionScoreQuery().add(new MatchAllQueryBuilder(), linearDecayFunction("num", 1, 0.5)).scoreMode(
                                        "multiply"))));
        response.actionGet();
    }

    @Test
    public void testMultiFieldOptions() throws Exception {
        assertAcked(prepareCreate("test").addMapping(
                "type1",
                jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("test").field("type", "string")
                        .endObject().startObject("loc").field("type", "geo_point").endObject().startObject("num").field("type", "float").endObject().endObject().endObject().endObject()));
        ensureYellow();

        // Index for testing MIN and MAX
        IndexRequestBuilder doc1 = client().prepareIndex()
                .setType("type1")
                .setId("1")
                .setIndex("test")
                .setSource(
                        jsonBuilder().startObject().field("test", "value").startArray("loc").startObject().field("lat", 10).field("lon", 20).endObject().startObject().field("lat", 12).field("lon", 23).endObject().endArray()
                                .endObject());
        IndexRequestBuilder doc2 = client().prepareIndex()
                .setType("type1")
                .setId("2")
                .setIndex("test")
                .setSource(
                        jsonBuilder().startObject().field("test", "value").startObject("loc").field("lat", 11).field("lon", 22).endObject()
                                .endObject());

        indexRandom(true, doc1, doc2);

        ActionFuture<SearchResponse> response = client().search(
                searchRequest().source(
                        searchSource().query(constantScoreQuery(termQuery("test", "value")))));
        SearchResponse sr = response.actionGet();
        assertSearchHits(sr, "1", "2");
        SearchHits sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo((long) (2)));

        List<Float> lonlat = new ArrayList<>();
        lonlat.add(20f);
        lonlat.add(10f);
        response = client().search(
                searchRequest().source(
                        searchSource().query(
                                functionScoreQuery(constantScoreQuery(termQuery("test", "value")), gaussDecayFunction("loc", lonlat, "1000km").setMultiValueMode("min")))));
        sr = response.actionGet();
        assertSearchHits(sr, "1", "2");
        sh = sr.getHits();

        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat(sh.getAt(1).getId(), equalTo("2"));
        response = client().search(
                searchRequest().source(
                        searchSource().query(
                                functionScoreQuery(constantScoreQuery(termQuery("test", "value")), gaussDecayFunction("loc", lonlat, "1000km").setMultiValueMode("max")))));
        sr = response.actionGet();
        assertSearchHits(sr, "1", "2");
        sh = sr.getHits();

        assertThat(sh.getAt(0).getId(), equalTo("2"));
        assertThat(sh.getAt(1).getId(), equalTo("1"));

        // Now test AVG and SUM

        doc1 = client().prepareIndex()
                .setType("type1")
                .setId("1")
                .setIndex("test")
                .setSource(
                        jsonBuilder().startObject().field("test", "value").startArray("num").value(0.0).value(1.0).value(2.0).endArray()
                                .endObject());
        doc2 = client().prepareIndex()
                .setType("type1")
                .setId("2")
                .setIndex("test")
                .setSource(
                        jsonBuilder().startObject().field("test", "value").field("num", 1.0)
                                .endObject());

        indexRandom(true, doc1, doc2);
        response = client().search(
                searchRequest().source(
                        searchSource().query(
                                functionScoreQuery(constantScoreQuery(termQuery("test", "value")), linearDecayFunction("num", "0", "10").setMultiValueMode("sum")))));
        sr = response.actionGet();
        assertSearchHits(sr, "1", "2");
        sh = sr.getHits();

        assertThat(sh.getAt(0).getId(), equalTo("2"));
        assertThat(sh.getAt(1).getId(), equalTo("1"));
        assertThat((double)(1.0 - sh.getAt(0).getScore()), closeTo((double)((1.0 - sh.getAt(1).getScore())/3.0), 1.e-6d));
        response = client().search(
                searchRequest().source(
                        searchSource().query(
                                functionScoreQuery(constantScoreQuery(termQuery("test", "value")), linearDecayFunction("num", "0", "10").setMultiValueMode("avg")))));
        sr = response.actionGet();
        assertSearchHits(sr, "1", "2");
        sh = sr.getHits();
        assertThat((double) (sh.getAt(0).getScore()), closeTo((double) (sh.getAt(1).getScore()), 1.e-6d));
    }

    @Test
    public void errorMessageForFaultyFunctionScoreBody() throws Exception {
        assertAcked(prepareCreate("test").addMapping(
                "type",
                jsonBuilder().startObject().startObject("type").startObject("properties").startObject("test").field("type", "string")
                        .endObject().startObject("num").field("type", "double").endObject().endObject().endObject().endObject()));
        ensureYellow();
        client().index(
                indexRequest("test").type("type").source(jsonBuilder().startObject().field("test", "value").field("num", 1.0).endObject()))
                .actionGet();
        refresh();

        XContentBuilder query = XContentFactory.jsonBuilder();
        // query that contains a single function and a functions[] array
        query.startObject().startObject("function_score").field("weight", "1").startArray("functions").startObject().startObject("script_score").field("script", "3").endObject().endObject().endArray().endObject().endObject();
        try {
            client().search(
                    searchRequest().source(
                            searchSource().query(query))).actionGet();
            fail("Search should result in SearchPhaseExecutionException");
        } catch (SearchPhaseExecutionException e) {
            logger.info(e.shardFailures()[0].reason());
            assertThat(e.shardFailures()[0].reason(), containsString("already found [weight], now encountering [functions]."));
        }

        query = XContentFactory.jsonBuilder();
        // query that contains a single function (but not boost factor) and a functions[] array
        query.startObject().startObject("function_score").startObject("random_score").field("seed", 3).endObject().startArray("functions").startObject().startObject("random_score").field("seed", 3).endObject().endObject().endArray().endObject().endObject();
        try {
            client().search(
                    searchRequest().source(
                            searchSource().query(query))).actionGet();
            fail("Search should result in SearchPhaseExecutionException");
        } catch (SearchPhaseExecutionException e) {
            logger.info(e.shardFailures()[0].reason());
            assertThat(e.shardFailures()[0].reason(), containsString("already found [random_score], now encountering [functions]"));
            assertThat(e.shardFailures()[0].reason(), not(containsString("did you mean [boost] instead?")));

        }
    }

    // issue https://github.com/elasticsearch/elasticsearch/issues/6292
    @Test
    public void testMissingFunctionThrowsElasticsearchParseException() throws IOException {

        // example from issue https://github.com/elasticsearch/elasticsearch/issues/6292
        String doc = "{\n" +
                "  \"text\": \"baseball bats\"\n" +
                "}\n";

        String query = "{\n" +
                "    \"function_score\": {\n" +
                "      \"score_mode\": \"sum\",\n" +
                "      \"boost_mode\": \"replace\",\n" +
                "      \"functions\": [\n" +
                "        {\n" +
                "          \"filter\": {\n" +
                "            \"term\": {\n" +
                "              \"text\": \"baseball\"\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "}\n";

        client().prepareIndex("t", "test").setSource(doc).get();
        refresh();
        ensureYellow("t");
        try {
            client().search(
                    searchRequest().source(
                            searchSource().query(query))).actionGet();
            fail("Should fail with SearchPhaseExecutionException");
        } catch (SearchPhaseExecutionException failure) {
            assertThat(failure.toString(), containsString("SearchParseException"));
            assertThat(failure.toString(), not(containsString("NullPointerException")));
        }

        query = "{\n" +
                "    \"function_score\": {\n" +
                "      \"score_mode\": \"sum\",\n" +
                "      \"boost_mode\": \"replace\",\n" +
                "      \"functions\": [\n" +
                "        {\n" +
                "          \"filter\": {\n" +
                "            \"term\": {\n" +
                "              \"text\": \"baseball\"\n" +
                "            }\n" +
                "          },\n" +
                "          \"weight\": 2\n" +
                "        },\n" +
                "        {\n" +
                "          \"filter\": {\n" +
                "            \"term\": {\n" +
                "              \"text\": \"baseball\"\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "}";

        try {
            client().search(
                    searchRequest().source(
                            searchSource().query(query))).actionGet();
            fail("Should fail with SearchPhaseExecutionException");
        } catch (SearchPhaseExecutionException failure) {
            assertThat(failure.toString(), containsString("SearchParseException"));
            assertThat(failure.toString(), not(containsString("NullPointerException")));
            assertThat(failure.toString(), containsString("an entry in functions list is missing a function"));
        }

        // next test java client
        try {
            client().prepareSearch("t").setQuery(QueryBuilders.functionScoreQuery(QueryBuilders.matchAllQuery(), null)).get();
        } catch (IllegalArgumentException failure) {
            assertThat(failure.toString(), containsString("function must not be null"));
        }
        try {
            client().prepareSearch("t").setQuery(QueryBuilders.functionScoreQuery().add(QueryBuilders.matchAllQuery(), null)).get();
        } catch (IllegalArgumentException failure) {
            assertThat(failure.toString(), containsString("function must not be null"));
        }
        try {
            client().prepareSearch("t").setQuery(QueryBuilders.functionScoreQuery().add(null)).get();
        } catch (IllegalArgumentException failure) {
            assertThat(failure.toString(), containsString("function must not be null"));
        }
    }

    @Test
    public void testExplainString() throws IOException, ExecutionException, InterruptedException {
        assertAcked(prepareCreate("test").addMapping(
                "type1",
                jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("test").field("type", "string")
                        .endObject().startObject("num").field("type", "double").endObject().endObject().endObject().endObject()));
        ensureYellow();


        client().prepareIndex().setType("type1").setId("1").setIndex("test")
                .setSource(jsonBuilder().startObject().field("test", "value").array("num", 0.5, 0.7).endObject()).get();

        refresh();

        SearchResponse response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true)
                                .query(functionScoreQuery(termQuery("test", "value"))
                                        .add(gaussDecayFunction("num", 1.0, 5.0).setOffset(1.0))
                                        .add(linearDecayFunction("num", 1.0, 5.0).setOffset(1.0))
                                        .add(exponentialDecayFunction("num", 1.0, 5.0).setOffset(1.0))
                                        .boostMode(CombineFunction.REPLACE.getName())))).get();
        String explanation = response.getHits().getAt(0).getExplanation().toString();
        assertThat(explanation, containsString(" 1.0 = exp(-0.5*pow(MIN[Math.max(Math.abs(0.5(=doc value) - 1.0(=origin))) - 1.0(=offset), 0), Math.max(Math.abs(0.7(=doc value) - 1.0(=origin))) - 1.0(=offset), 0)],2.0)/18.033688011112044)"));
        assertThat(explanation, containsString("1.0 = max(0.0, ((10.0 - MIN[Math.max(Math.abs(0.5(=doc value) - 1.0(=origin))) - 1.0(=offset), 0), Math.max(Math.abs(0.7(=doc value) - 1.0(=origin))) - 1.0(=offset), 0)])/10.0)"));
        assertThat(explanation, containsString("1.0 = exp(- MIN[Math.max(Math.abs(0.5(=doc value) - 1.0(=origin))) - 1.0(=offset), 0), Math.max(Math.abs(0.7(=doc value) - 1.0(=origin))) - 1.0(=offset), 0)] * 0.13862943611198905)"));

    }
}
