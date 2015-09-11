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
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.lucene.search.function.FieldValueFactorFunction;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.elasticsearch.index.query.functionscore.weight.WeightBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.client.Requests.searchRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.exponentialDecayFunction;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.fieldValueFactorFunction;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.gaussDecayFunction;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.linearDecayFunction;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.randomFunction;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.scriptFunction;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.weightFactorFunction;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class FunctionScoreIT extends ESIntegTestCase {

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
                                functionScoreQuery(constantScoreQuery(termQuery(TEXT_FIELD, "value")))
                                        .add(gaussDecayFunction(GEO_POINT_FIELD, new GeoPoint(10, 20), "1000km"))
                                        .add(fieldValueFactorFunction(DOUBLE_FIELD).modifier(FieldValueFactorFunction.Modifier.LN).setWeight(2))
                                        .add(scriptFunction(new Script("_index['" + TEXT_FIELD + "']['value'].tf()")).setWeight(3)))
                                .explain(true))).actionGet();

        assertThat(
                responseWithWeights.getHits().getAt(0).getExplanation().toString(),
                equalTo("6.0 = function score, product of:\n  1.0 = ConstantScore(text_field:value), product of:\n    1.0 = boost\n    1.0 = queryNorm\n  6.0 = min of:\n    6.0 = function score, score mode [multiply]\n      1.0 = function score, product of:\n        1.0 = match filter: *:*\n        1.0 = Function for field geo_point_field:\n          1.0 = exp(-0.5*pow(MIN of: [Math.max(arcDistance([10.0, 20.0](=doc value),[10.0, 20.0](=origin)) - 0.0(=offset), 0)],2.0)/7.213475204444817E11)\n      2.0 = function score, product of:\n        1.0 = match filter: *:*\n        2.0 = product of:\n          1.0 = field value function: ln(doc['double_field'].value * factor=1.0)\n          2.0 = weight\n      3.0 = function score, product of:\n        1.0 = match filter: *:*\n        3.0 = product of:\n          1.0 = script score function, computed with script:\"[script: _index['text_field']['value'].tf(), type: inline, lang: null, params: null]\n            1.0 = _score: \n              1.0 = ConstantScore(text_field:value), product of:\n                1.0 = boost\n                1.0 = queryNorm\n          3.0 = weight\n    3.4028235E38 = maxBoost\n"));
        responseWithWeights = client().search(
                searchRequest().source(
                        searchSource().query(
                                functionScoreQuery(constantScoreQuery(termQuery(TEXT_FIELD, "value"))).add(weightFactorFunction(4.0f)))
                                .explain(true))).actionGet();
        assertThat(
                responseWithWeights.getHits().getAt(0).getExplanation().toString(),
                equalTo("4.0 = function score, product of:\n  1.0 = ConstantScore(text_field:value), product of:\n    1.0 = boost\n    1.0 = queryNorm\n  4.0 = min of:\n    4.0 = product of:\n      1.0 = constant score 1.0 - no function provided\n      4.0 = weight\n    3.4028235E38 = maxBoost\n"));

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
                                functionScoreQuery(constantScoreQuery(termQuery(TEXT_FIELD, "value")))
                                        .add(gaussDecayFunction(GEO_POINT_FIELD, new GeoPoint(10, 20), "1000km"))
                                        .add(fieldValueFactorFunction(DOUBLE_FIELD).modifier(FieldValueFactorFunction.Modifier.LN))
                                        .add(scriptFunction(new Script("_index['" + TEXT_FIELD + "']['value'].tf()")))))).actionGet();
        SearchResponse responseWithWeights = client().search(
                searchRequest().source(
                        searchSource().query(
                                functionScoreQuery(constantScoreQuery(termQuery(TEXT_FIELD, "value")))
                                        .add(gaussDecayFunction(GEO_POINT_FIELD, new GeoPoint(10, 20), "1000km").setWeight(2))
                                        .add(fieldValueFactorFunction(DOUBLE_FIELD).modifier(FieldValueFactorFunction.Modifier.LN)
                                                .setWeight(2))
                                        .add(scriptFunction(new Script("_index['" + TEXT_FIELD + "']['value'].tf()")).setWeight(2)))))
                .actionGet();

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

        String scoreMode = getRandomScoreMode();
        FunctionScoreQueryBuilder withWeights = functionScoreQuery(constantScoreQuery(termQuery(TEXT_FIELD, "value"))).scoreMode(scoreMode);
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

        float weightSum = 0;

        for (int i = 0; i < weights.length; i++) {
            double functionScore = (double) weights[i] * scores[i];
            weightSum += weights[i];

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
        FunctionScoreQueryBuilder withWeights = functionScoreQuery(constantScoreQuery(termQuery(TEXT_FIELD, "value")));
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
                                    functionScoreQuery(constantScoreQuery(termQuery(TEXT_FIELD, "value")))
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
functionScoreQuery().add(scriptFunction(new Script("1")))).add(
                                                scriptFunction(new Script("_score.doubleValue()")))).add(
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
                        searchSource().query(functionScoreQuery().add(scriptFunction(new Script("_score.doubleValue()")))).aggregation(
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
                                functionScoreQuery().add(scriptFunction(new Script(Float.toString(score)))).setMinScore(minScore)))
        ).actionGet();
        if (score < minScore) {
            assertThat(searchResponse.getHits().getTotalHits(), is(0l));
        } else {
            assertThat(searchResponse.getHits().getTotalHits(), is(1l));
        }

        searchResponse = client().search(
                searchRequest().source(searchSource().query(functionScoreQuery()
.add(scriptFunction(new Script(Float.toString(score))))
                                        .add(scriptFunction(new Script(Float.toString(score))))
                        .scoreMode("avg").setMinScore(minScore)))
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
                searchRequest().source(searchSource().query(functionScoreQuery()
                        .add(scriptFunction(script))
                        .setMinScore(minScore)).size(numDocs))).actionGet();
        assertMinScoreSearchResponses(numDocs, searchResponse, numMatchingDocs);

        searchResponse = client().search(
                searchRequest().source(searchSource().query(functionScoreQuery()
                        .add(scriptFunction(script))
                        .add(scriptFunction(script))
                        .scoreMode("avg").setMinScore(minScore)).size(numDocs))).actionGet();
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
        testMinScoreApplied("sum", termQueryScore);
        testMinScoreApplied("avg", termQueryScore);
        testMinScoreApplied("max", termQueryScore);
        testMinScoreApplied("min", termQueryScore);
        testMinScoreApplied("multiply", termQueryScore);
        testMinScoreApplied("replace", termQueryScore);
    }

    protected void testMinScoreApplied(String boostMode, float expectedScore) throws InterruptedException, ExecutionException {
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

