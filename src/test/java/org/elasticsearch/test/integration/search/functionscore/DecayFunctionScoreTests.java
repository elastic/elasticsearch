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

package org.elasticsearch.test.integration.search.functionscore;

import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.MatchAllFilterBuilder;
import org.elasticsearch.index.query.functionscore.DecayFunctionBuilder;
import org.elasticsearch.index.query.functionscore.exp.ExponentialDecayFunctionBuilder;
import org.elasticsearch.index.query.functionscore.gauss.GaussDecayFunctionBuilder;
import org.elasticsearch.index.query.functionscore.lin.LinearDecayFunctionBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.test.integration.AbstractSharedClusterTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.client.Requests.searchRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

public class DecayFunctionScoreTests extends AbstractSharedClusterTest {

    @Test
    public void testDistanceScoreGeoLinGaussExp() throws Exception {

        createIndexMapped("test", "type1", "test", "string", "loc", "geo_point");
        ensureYellow();

        List<IndexRequestBuilder> indexBuilders = new ArrayList<IndexRequestBuilder>();
        indexBuilders.add(new IndexRequestBuilder(client())
                .setType("type1")
                .setId("1")
                .setIndex("test")
                .setSource(
                        jsonBuilder().startObject().field("test", "value").startObject("loc").field("lat", 10).field("lon", 20).endObject()
                                .endObject()));
        indexBuilders.add(new IndexRequestBuilder(client())
                .setType("type1")
                .setId("2")
                .setIndex("test")
                .setSource(
                        jsonBuilder().startObject().field("test", "value").startObject("loc").field("lat", 11).field("lon", 22).endObject()
                                .endObject()));

        int numDummyDocs = 20;
        for (int i = 1; i <= numDummyDocs; i++) {
            indexBuilders.add(new IndexRequestBuilder(client())
                    .setType("type1")
                    .setId(Integer.toString(i + 3))
                    .setIndex("test")
                    .setSource(
                            jsonBuilder().startObject().field("test", "value").startObject("loc").field("lat", 11 + i).field("lon", 22 + i)
                                    .endObject().endObject()));
        }
        IndexRequestBuilder[] builders = indexBuilders.toArray(new IndexRequestBuilder[indexBuilders.size()]);

        indexRandom("test", false, builders);
        refresh();

        // Test Gauss
        List<Float> lonlat = new ArrayList<Float>();
        lonlat.add(new Float(20));
        lonlat.add(new Float(11));
        DecayFunctionBuilder fb = new GaussDecayFunctionBuilder("loc", lonlat, "1000km");

        ActionFuture<SearchResponse> response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(false).query(termQuery("test", "value"))));
        SearchResponse sr = response.actionGet();
        SearchHits sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo((long) (numDummyDocs + 2)));

        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(functionScoreQuery(termQuery("test", "value")).add(fb))));
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo((long) (numDummyDocs + 2)));

        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat(sh.getAt(1).getId(), equalTo("2"));
        // Test Exp
        fb = new ExponentialDecayFunctionBuilder("loc", lonlat, "1000km");

        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(false).query(termQuery("test", "value"))));
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo((long) (numDummyDocs + 2)));

        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(functionScoreQuery(termQuery("test", "value")).add(fb))));
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo((long) (numDummyDocs + 2)));

        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat(sh.getAt(1).getId(), equalTo("2"));
        // Test Lin
        fb = new LinearDecayFunctionBuilder("loc", lonlat, "1000km");

        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(false).query(termQuery("test", "value"))));
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo((long) (numDummyDocs + 2)));

        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(functionScoreQuery(termQuery("test", "value")).add(fb))));
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo((long) (numDummyDocs + 2)));

        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat(sh.getAt(1).getId(), equalTo("2"));
    }

    @Test(expected = SearchPhaseExecutionException.class)
    public void testExceptionThrownIfScaleLE0() throws Exception {

        createIndexMapped("test", "type1", "test", "string", "num1", "date");
        ensureYellow();
        client().index(
                indexRequest("test").type("type1").id("1")
                        .source(jsonBuilder().startObject().field("test", "value").field("num1", "2013-05-27").endObject())).actionGet();
        client().index(
                indexRequest("test").type("type1").id("2")
                        .source(jsonBuilder().startObject().field("test", "value").field("num1", "2013-05-28").endObject())).actionGet();
        refresh();

        DecayFunctionBuilder gfb = new GaussDecayFunctionBuilder("num1", "2013-05-28", "-1d");

        ActionFuture<SearchResponse> response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(functionScoreQuery(termQuery("test", "value")).add(gfb))));

        SearchResponse sr = response.actionGet();
        ElasticsearchAssertions.assertNoFailures(sr);
        SearchHits sh = sr.getHits();
        assertThat(sh.hits().length, equalTo(2));
        assertThat(sh.getAt(0).getId(), equalTo("2"));
        assertThat(sh.getAt(1).getId(), equalTo("1"));

    }

    @Test(expected = ElasticSearchIllegalStateException.class)
    public void testExceptionThrownIfScaleRefNotBetween0And1() throws Exception {

        DecayFunctionBuilder gfb = new GaussDecayFunctionBuilder("num1", "2013-05-28", "1d").setScaleWeight(100);

    }

    @Test
    public void testValueMissingLin() throws Exception {

        createIndexMapped("test", "type1", "test", "string", "num1", "date", "num2", "double");
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

        DecayFunctionBuilder gfb1 = new LinearDecayFunctionBuilder("num1", "2013-05-28", "+3d");
        DecayFunctionBuilder gfb2 = new LinearDecayFunctionBuilder("num2", "0.0", "1");

        ActionFuture<SearchResponse> response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(
                                functionScoreQuery(termQuery("test", "value")).add(new MatchAllFilterBuilder(), gfb1)
                                        .add(new MatchAllFilterBuilder(), gfb2).scoreMode("multiply"))));

        SearchResponse sr = response.actionGet();
        ElasticsearchAssertions.assertNoFailures(sr);
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
    public void testManyDocsLin() throws Exception {

        createIndexMapped("test", "type", "test", "string", "date", "date", "num", "double", "geo", "geo_point");
        ensureYellow();
        int numDocs = 200;
        List<IndexRequestBuilder> indexBuilders = new ArrayList<IndexRequestBuilder>();

        for (int i = 0; i < numDocs; i++) {
            double lat = 100 + (int) (10.0 * (float) (i) / (float) (numDocs));
            double lon = 100;
            int day = (int) (29.0 * (float) (i) / (float) (numDocs)) + 1;
            String dayString = day < 10 ? "0" + Integer.toString(day) : Integer.toString(day);
            String date = "2013-05-" + dayString;

            indexBuilders.add(new IndexRequestBuilder(client())
                    .setType("type")
                    .setId(Integer.toString(i))
                    .setIndex("test")
                    .setSource(
                            jsonBuilder().startObject().field("test", "value").field("date", date).field("num", i).startObject("geo")
                                    .field("lat", lat).field("lon", lon).endObject().endObject()));
        }
        IndexRequestBuilder[] builders = indexBuilders.toArray(new IndexRequestBuilder[indexBuilders.size()]);

        indexRandom("test", false, builders);
        refresh();
        
        List<Float> lonlat = new ArrayList<Float>();
        lonlat.add(new Float(100));
        lonlat.add(new Float(110));
        DecayFunctionBuilder gfb1 = new LinearDecayFunctionBuilder("date", "2013-05-30", "+15d");
        DecayFunctionBuilder gfb2 = new LinearDecayFunctionBuilder("geo", lonlat, "1000km");
        DecayFunctionBuilder gfb3 = new LinearDecayFunctionBuilder("num", Integer.toString(numDocs), Integer.toString(numDocs / 2));

        ActionFuture<SearchResponse> response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource()
                                .explain(true)
                                .size(numDocs)
                                .query(functionScoreQuery(termQuery("test", "value")).add(new MatchAllFilterBuilder(), gfb1)
                                        .add(new MatchAllFilterBuilder(), gfb2).add(new MatchAllFilterBuilder(), gfb3)
                                        .scoreMode("multiply"))));

        SearchResponse sr = response.actionGet();
        ElasticsearchAssertions.assertNoFailures(sr);
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

        createIndexMapped("test", "type", "test", "string", "geo", "geo_point");
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
        DecayFunctionBuilder gfb2 = new LinearDecayFunctionBuilder("type1.geo", lonlat, "1000km");
        ActionFuture<SearchResponse> response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource()
                                .explain(true)
                                .size(numDocs)
                                .query(functionScoreQuery(termQuery("test", "value")).add(new MatchAllFilterBuilder(), gfb2).scoreMode(
                                        "multiply"))));
        SearchResponse sr = response.actionGet();

    }

    @Test(expected = SearchPhaseExecutionException.class)
    public void testParsingExceptionIfFieldTypeDoesNotMatch() throws Exception {
        createIndexMapped("test", "type", "test", "string", "num", "string");
        ensureYellow();
        client().index(
                indexRequest("test").type("type").source(
                        jsonBuilder().startObject().field("test", "value").field("num", Integer.toString(1)).endObject())).actionGet();
        refresh();
        DecayFunctionBuilder lfb = new LinearDecayFunctionBuilder("num", Integer.toString(1), Integer.toString(1 / 2));
        // so, we indexed a string field, but now we try to score a num field
        ActionFuture<SearchResponse> response = client()
                .search(searchRequest()
                        .searchType(SearchType.QUERY_THEN_FETCH)
                        .source(searchSource().explain(true).query(
                                functionScoreQuery(termQuery("test", "value")).add(new MatchAllFilterBuilder(), lfb).scoreMode("multiply"))));
        response.actionGet();
    }

}
