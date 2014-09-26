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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.lucene.search.function.FieldValueFactorFunction;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.elasticsearch.index.query.functionscore.weight.WeightBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.client.Requests.searchRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.termFilter;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.*;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.*;

public class FunctionScoreTests extends ElasticsearchIntegrationTest {

    static final String TYPE = "type";
    static final String INDEX = "index";
    static final String TEXT_FIELD = "text_field";
    static final String DOUBLE_FIELD = "double_field";
    static final String GEO_POINT_FIELD = "geo_point_field";
    static final XContentBuilder SIMPLE_DOC;
    static final XContentBuilder MAPPING_WITH_DOUBLE_AND_GEO_POINT_AND_TEXT_FIELD;

    @Test
    public void testExplainQueryOnlyOnce() throws IOException, ExecutionException, InterruptedException {
        assertAcked(prepareCreate("test").addMapping(
                "type1",
                jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("test").field("type", "string")
                        .endObject().startObject("num").field("type", "float").endObject().endObject().endObject().endObject()));
        ensureYellow();

        client().prepareIndex()
                .setType("type1")
                .setId("1")
                .setIndex("test")
                .setSource(
                        jsonBuilder().startObject().field("test", "value").field("num", 10).endObject()).get();
        refresh();

        SearchResponse response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(
                                functionScoreQuery(termQuery("test", "value")).add(gaussDecayFunction("num", 5, 5)).add(exponentialDecayFunction("num", 5, 5)).add(linearDecayFunction("num", 5, 5))))).get();
        String explanation = response.getHits().getAt(0).explanation().toString();

        checkQueryExplanationAppearsOnlyOnce(explanation);
        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(
                                functionScoreQuery(termQuery("test", "value")).add(fieldValueFactorFunction("num"))))).get();
        explanation = response.getHits().getAt(0).explanation().toString();
        checkQueryExplanationAppearsOnlyOnce(explanation);

        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(
                                functionScoreQuery(termQuery("test", "value")).add(randomFunction(10))))).get();
        explanation = response.getHits().getAt(0).explanation().toString();

        checkQueryExplanationAppearsOnlyOnce(explanation);
    }

    private void checkQueryExplanationAppearsOnlyOnce(String explanation) {
        // use some substring of the query explanation and see if it appears twice
        String queryExplanation = "idf(docFreq=1, maxDocs=1)";
        int queryExplanationIndex = explanation.indexOf(queryExplanation, 0);
        assertThat(queryExplanationIndex, greaterThan(-1));
        queryExplanationIndex = explanation.indexOf(queryExplanation, queryExplanationIndex + 1);
        assertThat(queryExplanationIndex, equalTo(-1));
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
            throw new ElasticsearchException("Exception while initializing FunctionScoreTests", e);
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
            throw new ElasticsearchException("Exception while initializing FunctionScoreTests", e);
        }
        MAPPING_WITH_DOUBLE_AND_GEO_POINT_AND_TEXT_FIELD = mappingWithDoubleAndGeoPointAndTestField;
    }

    @Test
    public void testExplain() throws IOException, ExecutionException, InterruptedException {
        assertAcked(prepareCreate(INDEX).addMapping(
                TYPE, MAPPING_WITH_DOUBLE_AND_GEO_POINT_AND_TEXT_FIELD
        ));
        ensureYellow();

        index(INDEX, TYPE, "1", SIMPLE_DOC);
        refresh();

        SearchResponse responseWithWeights = client().search(
                searchRequest().source(
                        searchSource().query(
                                functionScoreQuery(termFilter(TEXT_FIELD, "value").cache(false))
                                        .add(gaussDecayFunction(GEO_POINT_FIELD, new GeoPoint(10, 20), "1000km"))
                                        .add(fieldValueFactorFunction(DOUBLE_FIELD).modifier(FieldValueFactorFunction.Modifier.LN).setWeight(2))
                                        .add(scriptFunction("_index['" + TEXT_FIELD + "']['value'].tf()").setWeight(3))
                        ).explain(true))).actionGet();

        assertThat(responseWithWeights.getHits().getAt(0).getExplanation().toString(),
                equalTo("6.0 = (MATCH) function score, product of:\n  1.0 = (MATCH) ConstantScore(text_field:value), product of:\n    1.0 = boost\n    1.0 = queryNorm\n  6.0 = (MATCH) Math.min of\n    6.0 = (MATCH) function score, score mode [multiply]\n      1.0 = (MATCH) function score, product of:\n        1.0 = match filter: *:*\n        1.0 = (MATCH) Function for field geo_point_field:\n          1.0 = exp(-0.5*pow(MIN of: [Math.max(arcDistance([10.0, 20.0](=doc value),[10.0, 20.0](=origin)) - 0.0(=offset), 0)],2.0)/7.213475204444817E11)\n      2.0 = (MATCH) function score, product of:\n        1.0 = match filter: *:*\n        2.0 = (MATCH) product of:\n          1.0 = field value function: ln(doc['double_field'].value * factor=1.0)\n          2.0 = weight\n      3.0 = (MATCH) function score, product of:\n        1.0 = match filter: *:*\n        3.0 = (MATCH) product of:\n          1.0 = script score function, computed with script:\"_index['text_field']['value'].tf()\n          3.0 = weight\n    3.4028235E38 = maxBoost\n  1.0 = queryBoost\n")
                );
        responseWithWeights = client().search(
                searchRequest().source(
                        searchSource().query(
                                functionScoreQuery(termFilter(TEXT_FIELD, "value").cache(false))
                                        .add(weightFactorFunction(4.0f))
                        ).explain(true))).actionGet();
        assertThat(responseWithWeights.getHits().getAt(0).getExplanation().toString(),
                equalTo("4.0 = (MATCH) function score, product of:\n  1.0 = (MATCH) ConstantScore(text_field:value), product of:\n    1.0 = boost\n    1.0 = queryNorm\n  4.0 = (MATCH) Math.min of\n    4.0 = (MATCH) product of:\n      1.0 = constant score 1.0 - no function provided\n      4.0 = weight\n    3.4028235E38 = maxBoost\n  1.0 = queryBoost\n")
        );

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
                                functionScoreQuery(termFilter(TEXT_FIELD, "value"))
                                        .add(gaussDecayFunction(GEO_POINT_FIELD, new GeoPoint(10, 20), "1000km"))
                                        .add(fieldValueFactorFunction(DOUBLE_FIELD).modifier(FieldValueFactorFunction.Modifier.LN))
                                        .add(scriptFunction("_index['" + TEXT_FIELD + "']['value'].tf()"))
                        ))).actionGet();
        SearchResponse responseWithWeights = client().search(
                searchRequest().source(
                        searchSource().query(
                                functionScoreQuery(termFilter(TEXT_FIELD, "value"))
                                        .add(gaussDecayFunction(GEO_POINT_FIELD, new GeoPoint(10, 20), "1000km").setWeight(2))
                                        .add(fieldValueFactorFunction(DOUBLE_FIELD).modifier(FieldValueFactorFunction.Modifier.LN).setWeight(2))
                                        .add(scriptFunction("_index['" + TEXT_FIELD + "']['value'].tf()").setWeight(2))
                        ))).actionGet();

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

        String scoreMode = getRandomScoreMode();
        FunctionScoreQueryBuilder withWeights = functionScoreQuery(termFilter(TEXT_FIELD, "value")).scoreMode(scoreMode);
        int weightscounter = 0;
        for (ScoreFunctionBuilder builder : scoreFunctionBuilders) {
            withWeights.add(builder.setWeight(weights[weightscounter]));
            weightscounter++;
        }
        SearchResponse responseWithWeights = client().search(
                searchRequest().source(searchSource().query(withWeights))
        ).actionGet();

        double expectedScore = computeExpectedScore(weights, scores, scoreMode);
        assertThat((float) expectedScore / responseWithWeights.getHits().getAt(0).getScore(), is(1.0f));
    }

    protected double computeExpectedScore(float[] weights, float[] scores, String scoreMode) {
        double expectedScore = 0.0;
        if ("multiply".equals(scoreMode)) {
            expectedScore = 1.0;
        }
        if ("max".equals(scoreMode)) {
            expectedScore = Float.MAX_VALUE * -1.0;
        }
        if ("min".equals(scoreMode)) {
            expectedScore = Float.MAX_VALUE;
        }

        for (int i = 0; i < weights.length; i++) {
            double functionScore = (double) weights[i] * scores[i];

            if ("avg".equals(scoreMode)) {
                expectedScore += functionScore;
            } else if ("max".equals(scoreMode)) {
                expectedScore = Math.max(functionScore, expectedScore);
            } else if ("min".equals(scoreMode)) {
                expectedScore = Math.min(functionScore, expectedScore);
            } else if ("sum".equals(scoreMode)) {
                expectedScore += functionScore;
            } else if ("multiply".equals(scoreMode)) {
                expectedScore *= functionScore;
            }

        }
        if ("avg".equals(scoreMode)) {
            expectedScore /= weights.length;
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
        FunctionScoreQueryBuilder withWeights = functionScoreQuery(termFilter(TEXT_FIELD, "value"));
        withWeights.add(scoreFunctionBuilder.setWeight(weights[0]));

        SearchResponse responseWithWeights = client().search(
                searchRequest().source(searchSource().query(withWeights))
        ).actionGet();

        assertThat( (double) scores[0] * weights[0]/ responseWithWeights.getHits().getAt(0).getScore(), closeTo(1.0, 1.e-6));

    }

    private String getRandomScoreMode() {
        String[] scoreModes = {"avg", "sum", "min", "max", "multiply"};
        return scoreModes[randomInt(scoreModes.length - 1)];
    }

    private float[] getScores(ScoreFunctionBuilder... scoreFunctionBuilders) {
        float[] scores = new float[scoreFunctionBuilders.length];
        int scorecounter = 0;
        for (ScoreFunctionBuilder builder : scoreFunctionBuilders) {
            SearchResponse response = client().search(
                    searchRequest().source(
                            searchSource().query(
                                    functionScoreQuery(termFilter(TEXT_FIELD, "value"))
                                            .add(builder)
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
        builders[3] = scriptFunction("_index['" + TEXT_FIELD + "']['value'].tf()");
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
                searchRequest().source(query)
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
                searchRequest().source(query)
        ).actionGet();
        assertSearchResponse(response);
        assertThat(response.getHits().getAt(0).score(), equalTo(2.0f));
        response = client().search(
                searchRequest().source(searchSource().query(functionScoreQuery().add(new WeightBuilder().setWeight(2.0f))))
        ).actionGet();
        assertSearchResponse(response);
        assertThat(response.getHits().getAt(0).score(), equalTo(2.0f));
        response = client().search(
                searchRequest().source(searchSource().query(functionScoreQuery().add(weightFactorFunction(2.0f))))
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
                                                functionScoreQuery().add(scriptFunction("1")))
                                                .add(scriptFunction("_score.doubleValue()")))
                                        .add(scriptFunction("_score.doubleValue()")
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
                        searchSource().query(
                                functionScoreQuery()
                                        .add(scriptFunction("_score.doubleValue()")
                                        )
                        ).aggregation(terms("score_agg").script("_score.doubleValue()"))
                )
        ).actionGet();
        assertSearchResponse(response);
        assertThat(response.getHits().getAt(0).score(), equalTo(1.0f));
        assertThat(((Terms) response.getAggregations().asMap().get("score_agg")).getBuckets().get(0).getKeyAsNumber().floatValue(), is(1f));
        assertThat(((Terms) response.getAggregations().asMap().get("score_agg")).getBuckets().get(0).getDocCount(), is(1l));
    }
}

