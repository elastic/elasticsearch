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

package org.elasticsearch.messy.tests;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FieldValueFactorFunction;
import org.elasticsearch.common.lucene.search.function.FiltersFunctionScoreQuery;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.elasticsearch.index.query.functionscore.weight.WeightBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.groovy.GroovyPlugin;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.client.Requests.searchRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.*;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.*;

public class FunctionScoreTests extends ESIntegTestCase {

    static final String TYPE = "type";
    static final String INDEX = "index";
    static final String TEXT_FIELD = "text_field";
    static final String DOUBLE_FIELD = "double_field";
    static final String GEO_POINT_FIELD = "geo_point_field";
    static final XContentBuilder SIMPLE_DOC;
    static final XContentBuilder MAPPING_WITH_DOUBLE_AND_GEO_POINT_AND_TEXT_FIELD;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(GroovyPlugin.class);
    }

    static {
        XContentBuilder simpleDoc;
        XContentBuilder mappingWithDoubleAndGeoPointAndTestField;
        try {
            simpleDoc = jsonBuilder().startObject()
                    .field(TEXT_FIELD, "value")
                    .startObject(GEO_POINT_FIELD)
                    .field("lat", 10)
                    .field("lon", 20)
                    .endObject()
                    .field(DOUBLE_FIELD, Math.E)
                    .endObject();
        } catch (IOException e) {
            throw new ElasticsearchException("Exception while initializing FunctionScoreIT", e);
        }
        SIMPLE_DOC = simpleDoc;
        try {

            mappingWithDoubleAndGeoPointAndTestField = jsonBuilder().startObject()
                    .startObject(TYPE)
                    .startObject("properties")
                    .startObject(TEXT_FIELD)
                    .field("type", "string")
                    .endObject()
                    .startObject(GEO_POINT_FIELD)
                    .field("type", "geo_point")
                    .endObject()
                    .startObject(DOUBLE_FIELD)
                    .field("type", "double")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject();
        } catch (IOException e) {
            throw new ElasticsearchException("Exception while initializing FunctionScoreIT", e);
        }
        MAPPING_WITH_DOUBLE_AND_GEO_POINT_AND_TEXT_FIELD = mappingWithDoubleAndGeoPointAndTestField;
    }

    @Test
    public void simpleWeightedFunctionsTest() throws IOException, ExecutionException, InterruptedException {
        assertAcked(prepareCreate(INDEX).addMapping(
                TYPE, MAPPING_WITH_DOUBLE_AND_GEO_POINT_AND_TEXT_FIELD
        ));
        ensureYellow();

        index(INDEX, TYPE, "1", SIMPLE_DOC);
        refresh();
        SearchResponse response = client().search(
                searchRequest().source(
                        searchSource().query(
                                functionScoreQuery(constantScoreQuery(termQuery(TEXT_FIELD, "value")), new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(gaussDecayFunction(GEO_POINT_FIELD, new GeoPoint(10, 20), "1000km")),
                                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(fieldValueFactorFunction(DOUBLE_FIELD).modifier(FieldValueFactorFunction.Modifier.LN)),
                                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(scriptFunction(new Script("_index['" + TEXT_FIELD + "']['value'].tf()")))
                                })))).actionGet();
        SearchResponse responseWithWeights = client().search(
                searchRequest().source(
                        searchSource().query(
                                functionScoreQuery(constantScoreQuery(termQuery(TEXT_FIELD, "value")), new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(gaussDecayFunction(GEO_POINT_FIELD, new GeoPoint(10, 20), "1000km").setWeight(2)),
                                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(fieldValueFactorFunction(DOUBLE_FIELD).modifier(FieldValueFactorFunction.Modifier.LN).setWeight(2)),
                                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(scriptFunction(new Script("_index['" + TEXT_FIELD + "']['value'].tf()")).setWeight(2))
                                })))).actionGet();

        assertSearchResponse(response);
        assertThat(response.getHits().getAt(0).getScore(), is(1.0f));
        assertThat(responseWithWeights.getHits().getAt(0).getScore(), is(8.0f));
    }

    @Test
    public void simpleWeightedFunctionsTestWithRandomWeightsAndRandomCombineMode() throws IOException, ExecutionException, InterruptedException {
        assertAcked(prepareCreate(INDEX).addMapping(
                TYPE,
                MAPPING_WITH_DOUBLE_AND_GEO_POINT_AND_TEXT_FIELD));
        ensureYellow();

        XContentBuilder doc = SIMPLE_DOC;
        index(INDEX, TYPE, "1", doc);
        refresh();
        ScoreFunctionBuilder[] scoreFunctionBuilders = getScoreFunctionBuilders();
        float[] weights = createRandomWeights(scoreFunctionBuilders.length);
        float[] scores = getScores(scoreFunctionBuilders);
        int weightscounter = 0;
        FunctionScoreQueryBuilder.FilterFunctionBuilder[] filterFunctionBuilders = new FunctionScoreQueryBuilder.FilterFunctionBuilder[scoreFunctionBuilders.length];
        for (ScoreFunctionBuilder builder : scoreFunctionBuilders) {
            filterFunctionBuilders[weightscounter] = new FunctionScoreQueryBuilder.FilterFunctionBuilder(builder.setWeight(weights[weightscounter]));
            weightscounter++;
        }
        FiltersFunctionScoreQuery.ScoreMode scoreMode = randomFrom(FiltersFunctionScoreQuery.ScoreMode.AVG, FiltersFunctionScoreQuery.ScoreMode.SUM,
                FiltersFunctionScoreQuery.ScoreMode.MIN, FiltersFunctionScoreQuery.ScoreMode.MAX, FiltersFunctionScoreQuery.ScoreMode.MULTIPLY);
        FunctionScoreQueryBuilder withWeights = functionScoreQuery(constantScoreQuery(termQuery(TEXT_FIELD, "value")), filterFunctionBuilders).scoreMode(scoreMode);

        SearchResponse responseWithWeights = client().search(
                searchRequest().source(searchSource().query(withWeights))
        ).actionGet();

        double expectedScore = computeExpectedScore(weights, scores, scoreMode);
        assertThat((float) expectedScore / responseWithWeights.getHits().getAt(0).getScore(), is(1.0f));
    }

    protected double computeExpectedScore(float[] weights, float[] scores, FiltersFunctionScoreQuery.ScoreMode scoreMode) {
        double expectedScore;
        switch(scoreMode) {
            case MULTIPLY:
                expectedScore = 1.0;
                break;
            case MAX:
                expectedScore = Float.MAX_VALUE * -1.0;
                break;
            case MIN:
                expectedScore = Float.MAX_VALUE;
                break;
            default:
                expectedScore = 0.0;
                break;
        }

        float weightSum = 0;
        for (int i = 0; i < weights.length; i++) {
            double functionScore = (double) weights[i] * scores[i];
            weightSum += weights[i];
            switch(scoreMode) {
                case AVG:
                    expectedScore += functionScore;
                    break;
                case MAX:
                    expectedScore = Math.max(functionScore, expectedScore);
                    break;
                case MIN:
                    expectedScore = Math.min(functionScore, expectedScore);
                    break;
                case SUM:
                    expectedScore += functionScore;
                    break;
                case MULTIPLY:
                    expectedScore *= functionScore;
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }
        if (scoreMode == FiltersFunctionScoreQuery.ScoreMode.AVG) {
            expectedScore /= weightSum;
        }
        return expectedScore;
    }

    @Test
    public void simpleWeightedFunctionsTestSingleFunction() throws IOException, ExecutionException, InterruptedException {
        assertAcked(prepareCreate(INDEX).addMapping(
                TYPE,
                MAPPING_WITH_DOUBLE_AND_GEO_POINT_AND_TEXT_FIELD));
        ensureYellow();

        XContentBuilder doc = jsonBuilder().startObject()
                .field(TEXT_FIELD, "value")
                .startObject(GEO_POINT_FIELD)
                .field("lat", 12)
                .field("lon", 21)
                .endObject()
                .field(DOUBLE_FIELD, 10)
                .endObject();
        index(INDEX, TYPE, "1", doc);
        refresh();
        ScoreFunctionBuilder[] scoreFunctionBuilders = getScoreFunctionBuilders();
        ScoreFunctionBuilder scoreFunctionBuilder = scoreFunctionBuilders[randomInt(3)];
        float[] weights = createRandomWeights(1);
        float[] scores = getScores(scoreFunctionBuilder);
        FunctionScoreQueryBuilder withWeights = functionScoreQuery(constantScoreQuery(termQuery(TEXT_FIELD, "value")), scoreFunctionBuilder.setWeight(weights[0]));

        SearchResponse responseWithWeights = client().search(
                searchRequest().source(searchSource().query(withWeights))
        ).actionGet();

        assertThat( (double) scores[0] * weights[0]/ responseWithWeights.getHits().getAt(0).getScore(), closeTo(1.0, 1.e-6));

    }

    private float[] getScores(ScoreFunctionBuilder... scoreFunctionBuilders) {
        float[] scores = new float[scoreFunctionBuilders.length];
        int scorecounter = 0;
        for (ScoreFunctionBuilder builder : scoreFunctionBuilders) {
            SearchResponse response = client().search(
                    searchRequest().source(
                            searchSource().query(
                                    functionScoreQuery(constantScoreQuery(termQuery(TEXT_FIELD, "value")), builder)
                            ))).actionGet();
            scores[scorecounter] = response.getHits().getAt(0).getScore();
            scorecounter++;
        }
        return scores;
    }

    private float[] createRandomWeights(int size) {
        float[] weights = new float[size];
        for (int i = 0; i < weights.length; i++) {
            weights[i] = randomFloat() * (randomBoolean() ? 1.0f : -1.0f) * randomInt(100) + 1.e-6f;
        }
        return weights;
    }

    public ScoreFunctionBuilder[] getScoreFunctionBuilders() {
        ScoreFunctionBuilder[] builders = new ScoreFunctionBuilder[4];
        builders[0] = gaussDecayFunction(GEO_POINT_FIELD, new GeoPoint(10, 20), "1000km");
        builders[1] = randomFunction(10);
        builders[2] = fieldValueFactorFunction(DOUBLE_FIELD).modifier(FieldValueFactorFunction.Modifier.LN);
        builders[3] = scriptFunction(new Script("_index['" + TEXT_FIELD + "']['value'].tf()"));
        return builders;
    }

    @Test
    public void checkWeightOnlyCreatesBoostFunction() throws IOException {
        assertAcked(prepareCreate(INDEX).addMapping(
                TYPE,
                MAPPING_WITH_DOUBLE_AND_GEO_POINT_AND_TEXT_FIELD));
        ensureYellow();

        index(INDEX, TYPE, "1", SIMPLE_DOC);
        refresh();
        String query =jsonBuilder().startObject()
                .startObject("query")
                .startObject("function_score")
                .startArray("functions")
                .startObject()
                .field("weight",2)
                .endObject()
                .endArray()
                .endObject()
                .endObject()
                .endObject().string();
        SearchResponse response = client().search(
                searchRequest().source(new BytesArray(query))
                ).actionGet();
        assertSearchResponse(response);
        assertThat(response.getHits().getAt(0).score(), equalTo(2.0f));

        query =jsonBuilder().startObject()
                .startObject("query")
                .startObject("function_score")
                .field("weight",2)
                .endObject()
                .endObject()
                .endObject().string();
        response = client().search(
                searchRequest().source(new BytesArray(query))
        ).actionGet();
        assertSearchResponse(response);
        assertThat(response.getHits().getAt(0).score(), equalTo(2.0f));
        response = client().search(
                searchRequest().source(searchSource().query(functionScoreQuery(new WeightBuilder().setWeight(2.0f))))
        ).actionGet();
        assertSearchResponse(response);
        assertThat(response.getHits().getAt(0).score(), equalTo(2.0f));
        response = client().search(
                searchRequest().source(searchSource().query(functionScoreQuery(weightFactorFunction(2.0f))))
        ).actionGet();
        assertSearchResponse(response);
        assertThat(response.getHits().getAt(0).score(), equalTo(2.0f));
    }

    @Test
    public void testScriptScoresNested() throws IOException {
        createIndex(INDEX);
        ensureYellow();
        index(INDEX, TYPE, "1", jsonBuilder().startObject().field("dummy_field", 1).endObject());
        refresh();
        SearchResponse response = client().search(
                searchRequest().source(
                        searchSource().query(
                                functionScoreQuery(
                                        functionScoreQuery(
                                                functionScoreQuery(scriptFunction(new Script("1"))),
                                                scriptFunction(new Script("_score.doubleValue()"))),
                                        scriptFunction(new Script("_score.doubleValue()"))
                                )
                        )
                )
        ).actionGet();
        assertSearchResponse(response);
        assertThat(response.getHits().getAt(0).score(), equalTo(1.0f));
    }

    @Test
    public void testScriptScoresWithAgg() throws IOException {
        createIndex(INDEX);
        ensureYellow();
        index(INDEX, TYPE, "1", jsonBuilder().startObject().field("dummy_field", 1).endObject());
        refresh();
        SearchResponse response = client().search(
                searchRequest().source(
                        searchSource().query(functionScoreQuery(scriptFunction(new Script("_score.doubleValue()")))).aggregation(
                                terms("score_agg").script(new Script("_score.doubleValue()")))
                )
        ).actionGet();
        assertSearchResponse(response);
        assertThat(response.getHits().getAt(0).score(), equalTo(1.0f));
        assertThat(((Terms) response.getAggregations().asMap().get("score_agg")).getBuckets().get(0).getKeyAsString(), equalTo("1.0"));
        assertThat(((Terms) response.getAggregations().asMap().get("score_agg")).getBuckets().get(0).getDocCount(), is(1l));
    }

    public void testMinScoreFunctionScoreBasic() throws IOException {
        index(INDEX, TYPE, jsonBuilder().startObject().field("num", 2).endObject());
        refresh();
        ensureYellow();
        float score = randomFloat();
        float minScore = randomFloat();
        SearchResponse searchResponse = client().search(
                searchRequest().source(
                        searchSource().query(
                                functionScoreQuery(scriptFunction(new Script(Float.toString(score)))).setMinScore(minScore)))
        ).actionGet();
        if (score < minScore) {
            assertThat(searchResponse.getHits().getTotalHits(), is(0l));
        } else {
            assertThat(searchResponse.getHits().getTotalHits(), is(1l));
        }

        searchResponse = client().search(
                searchRequest().source(searchSource().query(functionScoreQuery(new MatchAllQueryBuilder(), new FunctionScoreQueryBuilder.FilterFunctionBuilder[] {
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder(scriptFunction(new Script(Float.toString(score)))),
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder(scriptFunction(new Script(Float.toString(score))))
                        }).scoreMode(FiltersFunctionScoreQuery.ScoreMode.AVG).setMinScore(minScore)))
                ).actionGet();
        if (score < minScore) {
            assertThat(searchResponse.getHits().getTotalHits(), is(0l));
        } else {
            assertThat(searchResponse.getHits().getTotalHits(), is(1l));
        }
    }

    @Test
    public void testMinScoreFunctionScoreManyDocsAndRandomMinScore() throws IOException, ExecutionException, InterruptedException {
        List<IndexRequestBuilder> docs = new ArrayList<>();
        int numDocs = randomIntBetween(1, 100);
        int scoreOffset = randomIntBetween(-2 * numDocs, 2 * numDocs);
        int minScore = randomIntBetween(-2 * numDocs, 2 * numDocs);
        for (int i = 0; i < numDocs; i++) {
            docs.add(client().prepareIndex(INDEX, TYPE, Integer.toString(i)).setSource("num", i + scoreOffset));
        }
        indexRandom(true, docs);
        ensureYellow();
        Script script = new Script("return (doc['num'].value)");
        int numMatchingDocs = numDocs + scoreOffset - minScore;
        if (numMatchingDocs < 0) {
            numMatchingDocs = 0;
        }
        if (numMatchingDocs > numDocs) {
            numMatchingDocs = numDocs;
        }

        SearchResponse searchResponse = client().search(
                searchRequest().source(searchSource().query(functionScoreQuery(scriptFunction(script))
                        .setMinScore(minScore)).size(numDocs))).actionGet();
        assertMinScoreSearchResponses(numDocs, searchResponse, numMatchingDocs);

        searchResponse = client().search(
                searchRequest().source(searchSource().query(functionScoreQuery(new MatchAllQueryBuilder(), new FunctionScoreQueryBuilder.FilterFunctionBuilder[] {
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(scriptFunction(script)),
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(scriptFunction(script))
                }).scoreMode(FiltersFunctionScoreQuery.ScoreMode.AVG).setMinScore(minScore)).size(numDocs))).actionGet();
        assertMinScoreSearchResponses(numDocs, searchResponse, numMatchingDocs);
    }

    protected void assertMinScoreSearchResponses(int numDocs, SearchResponse searchResponse, int numMatchingDocs) {
        assertSearchResponse(searchResponse);
        assertThat((int) searchResponse.getHits().totalHits(), is(numMatchingDocs));
        int pos = 0;
        for (int hitId = numDocs - 1; (numDocs - hitId) < searchResponse.getHits().totalHits(); hitId--) {
            assertThat(searchResponse.getHits().getAt(pos).getId(), equalTo(Integer.toString(hitId)));
            pos++;
        }
    }

    @Test
    public void testWithEmptyFunctions() throws IOException, ExecutionException, InterruptedException {
        assertAcked(prepareCreate("test"));
        ensureYellow();
        index("test", "testtype", "1", jsonBuilder().startObject().field("text", "test text").endObject());
        refresh();

        // make sure that min_score works if functions is empty, see https://github.com/elastic/elasticsearch/issues/10253
        float termQueryScore = 0.19178301f;
        for (CombineFunction combineFunction : CombineFunction.values()) {
            testMinScoreApplied(combineFunction, termQueryScore);
        }
    }

    protected void testMinScoreApplied(CombineFunction boostMode, float expectedScore) throws InterruptedException, ExecutionException {
        SearchResponse response = client().search(
                searchRequest().source(
                        searchSource().explain(true).query(
                                functionScoreQuery(termQuery("text", "text")).boostMode(boostMode).setMinScore(0.1f)))).get();
        assertSearchResponse(response);
        assertThat(response.getHits().totalHits(), equalTo(1l));
        assertThat(response.getHits().getAt(0).getScore(), equalTo(expectedScore));

        response = client().search(
                searchRequest().source(
                        searchSource().explain(true).query(
                                functionScoreQuery(termQuery("text", "text")).boostMode(boostMode).setMinScore(2f)))).get();

        assertSearchResponse(response);
        assertThat(response.getHits().totalHits(), equalTo(0l));
    }
}

