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

package org.elasticsearch.index.query.functionscore;

import com.fasterxml.jackson.core.JsonParseException;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FieldValueFactorFunction;
import org.elasticsearch.common.lucene.search.function.FiltersFunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.WeightFactorFunction;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.AbstractQueryTestCase;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.RandomQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.functionscore.exp.ExponentialDecayFunctionBuilder;
import org.elasticsearch.index.query.functionscore.fieldvaluefactor.FieldValueFactorFunctionBuilder;
import org.elasticsearch.index.query.functionscore.gauss.GaussDecayFunctionBuilder;
import org.elasticsearch.index.query.functionscore.lin.LinearDecayFunctionBuilder;
import org.elasticsearch.index.query.functionscore.random.RandomScoreFunctionBuilder;
import org.elasticsearch.index.query.functionscore.script.ScriptScoreFunctionBuilder;
import org.elasticsearch.index.query.functionscore.weight.WeightBuilder;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class FunctionScoreQueryBuilderTests extends AbstractQueryTestCase<FunctionScoreQueryBuilder> {

    @Override
    protected FunctionScoreQueryBuilder doCreateTestQueryBuilder() {
        FunctionScoreQueryBuilder functionScoreQueryBuilder;
        switch(randomIntBetween(0, 3)) {
            case 0:
                int numFunctions = randomIntBetween(0, 3);
                FunctionScoreQueryBuilder.FilterFunctionBuilder[] filterFunctionBuilders = new FunctionScoreQueryBuilder.FilterFunctionBuilder[numFunctions];
                for (int i = 0; i < numFunctions; i++) {
                    filterFunctionBuilders[i] = new FunctionScoreQueryBuilder.FilterFunctionBuilder(RandomQueryBuilder.createQuery(random()), randomScoreFunction());
                }
                if (randomBoolean()) {
                    functionScoreQueryBuilder = new FunctionScoreQueryBuilder(RandomQueryBuilder.createQuery(random()), filterFunctionBuilders);
                } else {
                    functionScoreQueryBuilder = new FunctionScoreQueryBuilder(filterFunctionBuilders);
                }
                break;
            case 1:
                functionScoreQueryBuilder = new FunctionScoreQueryBuilder(randomScoreFunction());
                break;
            case 2:
                functionScoreQueryBuilder = new FunctionScoreQueryBuilder(RandomQueryBuilder.createQuery(random()), randomScoreFunction());
                break;
            case 3:
                functionScoreQueryBuilder = new FunctionScoreQueryBuilder(RandomQueryBuilder.createQuery(random()));
                break;
            default:
                throw new UnsupportedOperationException();
        }

        if (randomBoolean()) {
            functionScoreQueryBuilder.boostMode(randomFrom(CombineFunction.values()));
        }
        if (randomBoolean()) {
            functionScoreQueryBuilder.scoreMode(randomFrom(FiltersFunctionScoreQuery.ScoreMode.values()));
        }
        if (randomBoolean()) {
            functionScoreQueryBuilder.maxBoost(randomFloat());
        }
        if (randomBoolean()) {
            functionScoreQueryBuilder.setMinScore(randomFloat());
        }
        return functionScoreQueryBuilder;
    }

    private static ScoreFunctionBuilder randomScoreFunction() {
        if (randomBoolean()) {
            return new WeightBuilder().setWeight(randomFloat());
        }
        ScoreFunctionBuilder functionBuilder;
        //TODO random score function is temporarily disabled, it causes NPE in testToQuery when trying to access the shardId through SearchContext
        switch (randomIntBetween(0, 2)) {
            case 0:
                DecayFunctionBuilder decayFunctionBuilder;
                Float offset = randomBoolean() ? null : randomFloat();
                double decay = randomDouble();
                switch(randomIntBetween(0, 2)) {
                    case 0:
                        decayFunctionBuilder = new GaussDecayFunctionBuilder(INT_FIELD_NAME, randomFloat(), randomFloat(), offset, decay);
                        break;
                    case 1:
                        decayFunctionBuilder = new ExponentialDecayFunctionBuilder(INT_FIELD_NAME, randomFloat(), randomFloat(), offset, decay);
                        break;
                    case 2:
                        decayFunctionBuilder = new LinearDecayFunctionBuilder(INT_FIELD_NAME, randomFloat(), randomFloat(), offset, decay);
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
                if (randomBoolean()) {
                    decayFunctionBuilder.setMultiValueMode(randomFrom(MultiValueMode.values()));
                }
                functionBuilder = decayFunctionBuilder;
                break;
            case 1:
                FieldValueFactorFunctionBuilder fieldValueFactorFunctionBuilder = new FieldValueFactorFunctionBuilder(INT_FIELD_NAME);
                if (randomBoolean()) {
                    fieldValueFactorFunctionBuilder.factor(randomFloat());
                }
                if (randomBoolean()) {
                    fieldValueFactorFunctionBuilder.missing(randomDouble());
                }
                if (randomBoolean()) {
                    fieldValueFactorFunctionBuilder.modifier(randomFrom(FieldValueFactorFunction.Modifier.values()));
                }
                functionBuilder = fieldValueFactorFunctionBuilder;
                break;
            case 2:
                String script = "5";
                Map<String, Object> params = Collections.emptyMap();
                functionBuilder = new ScriptScoreFunctionBuilder(new Script(script, ScriptService.ScriptType.INLINE, MockScriptEngine.NAME, params));
                break;
            case 3:
                RandomScoreFunctionBuilder randomScoreFunctionBuilder = new RandomScoreFunctionBuilder();
                if (randomBoolean()) {
                    randomScoreFunctionBuilder.seed(randomLong());
                } else if(randomBoolean()) {
                    randomScoreFunctionBuilder.seed(randomInt());
                } else {
                    randomScoreFunctionBuilder.seed(randomAsciiOfLengthBetween(1, 10));
                }
                functionBuilder = randomScoreFunctionBuilder;
                break;
            default:
                throw new UnsupportedOperationException();
        }
        if (randomBoolean()) {
            functionBuilder.setWeight(randomFloat());
        }
        return functionBuilder;
    }

    @Override
    protected void doAssertLuceneQuery(FunctionScoreQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        assertThat(query, either(instanceOf(FunctionScoreQuery.class)).or(instanceOf(FiltersFunctionScoreQuery.class)));
    }

    /**
     * Overridden here to ensure the test is only run if at least one type is
     * present in the mappings. Functions require the field to be
     * explicitly mapped
     */
    @Override
    public void testToQuery() throws IOException {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        super.testToQuery();
    }

    public void testIllegalArguments() {
        try {
            new FunctionScoreQueryBuilder((QueryBuilder<?>)null);
            fail("must not be null");
        } catch(IllegalArgumentException e) {
            //all good
        }

        try {
            new FunctionScoreQueryBuilder((ScoreFunctionBuilder)null);
            fail("must not be null");
        } catch(IllegalArgumentException e) {
            //all good
        }

        try {
            new FunctionScoreQueryBuilder((FunctionScoreQueryBuilder.FilterFunctionBuilder[])null);
            fail("must not be null");
        } catch(IllegalArgumentException e) {
            //all good
        }

        try {
            new FunctionScoreQueryBuilder(null, ScoreFunctionBuilders.randomFunction(123));
            fail("must not be null");
        } catch(IllegalArgumentException e) {
            //all good
        }

        try {
            new FunctionScoreQueryBuilder(new MatchAllQueryBuilder(), (ScoreFunctionBuilder)null);
            fail("must not be null");
        } catch(IllegalArgumentException e) {
            //all good
        }

        try {
            new FunctionScoreQueryBuilder(new MatchAllQueryBuilder(), (FunctionScoreQueryBuilder.FilterFunctionBuilder[])null);
            fail("must not be null");
        } catch(IllegalArgumentException e) {
            //all good
        }

        try {
            new FunctionScoreQueryBuilder(null, new FunctionScoreQueryBuilder.FilterFunctionBuilder[0]);
            fail("must not be null");
        } catch(IllegalArgumentException e) {
            //all good
        }

        try {
            new FunctionScoreQueryBuilder(QueryBuilders.matchAllQuery(), new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{null});
            fail("content of array must not be null");
        } catch(IllegalArgumentException e) {
            //all good
        }

        try {
            new FunctionScoreQueryBuilder.FilterFunctionBuilder(null);
            fail("must not be null");
        } catch(IllegalArgumentException e) {
            //all good
        }

        try {
            new FunctionScoreQueryBuilder.FilterFunctionBuilder(null, ScoreFunctionBuilders.randomFunction(123));
            fail("must not be null");
        } catch(IllegalArgumentException e) {
            //all good
        }

        try {
            new FunctionScoreQueryBuilder.FilterFunctionBuilder(new MatchAllQueryBuilder(), null);
            fail("must not be null");
        } catch(IllegalArgumentException e) {
            //all good
        }

        try {
            new FunctionScoreQueryBuilder(new MatchAllQueryBuilder()).scoreMode(null);
            fail("must not be null");
        } catch(IllegalArgumentException e) {
            //all good
        }

        try {
            new FunctionScoreQueryBuilder(new MatchAllQueryBuilder()).boostMode(null);
            fail("must not be null");
        } catch(IllegalArgumentException e) {
            //all good
        }
    }

    public void testParseFunctionsArray() throws IOException {
        String functionScoreQuery = "{\n" +
                    "    \"function_score\":{\n" +
                    "        \"query\":{\n" +
                    "            \"term\":{\n" +
                    "                \"field1\":\"value1\"\n" +
                    "            }\n" +
                    "        },\n" +
                    "        \"functions\":  [\n" +
                    "            {\n" +
                    "                \"random_score\":  {\n" +
                    "                    \"seed\":123456\n" +
                    "                },\n" +
                    "                \"weight\": 3,\n" +
                    "                \"filter\": {\n" +
                    "                    \"term\":{\n" +
                    "                        \"field2\":\"value2\"\n" +
                    "                    }\n" +
                    "                }\n" +
                    "            },\n" +
                    "            {\n" +
                    "                \"filter\": {\n" +
                    "                    \"term\":{\n" +
                    "                        \"field3\":\"value3\"\n" +
                    "                    }\n" +
                    "                },\n" +
                    "                \"weight\": 9\n" +
                    "            },\n" +
                    "            {\n" +
                    "                \"gauss\":  {\n" +
                    "                    \"field_name\":  {\n" +
                    "                        \"origin\":0.5,\n" +
                    "                        \"scale\":0.6\n" +
                    "                    }\n" +
                    "                }\n" +
                    "            }\n" +
                    "        ],\n" +
                    "        \"boost\" : 3,\n" +
                    "        \"score_mode\" : \"avg\",\n" +
                    "        \"boost_mode\" : \"replace\",\n" +
                    "        \"max_boost\" : 10\n" +
                    "    }\n" +
                    "}";

        QueryBuilder<?> queryBuilder = parseQuery(functionScoreQuery);
        //given that we copy part of the decay functions as bytes, we test that fromXContent and toXContent both work no matter what the initial format was
        for (int i = 0; i <= XContentType.values().length; i++) {
            assertThat(queryBuilder, instanceOf(FunctionScoreQueryBuilder.class));
            FunctionScoreQueryBuilder functionScoreQueryBuilder = (FunctionScoreQueryBuilder) queryBuilder;
            assertThat(functionScoreQueryBuilder.query(), instanceOf(TermQueryBuilder.class));
            TermQueryBuilder termQueryBuilder = (TermQueryBuilder) functionScoreQueryBuilder.query();
            assertThat(termQueryBuilder.fieldName(), equalTo("field1"));
            assertThat(termQueryBuilder.value(), equalTo("value1"));
            assertThat(functionScoreQueryBuilder.filterFunctionBuilders().length, equalTo(3));
            assertThat(functionScoreQueryBuilder.filterFunctionBuilders()[0].getFilter(), instanceOf(TermQueryBuilder.class));
            termQueryBuilder = (TermQueryBuilder) functionScoreQueryBuilder.filterFunctionBuilders()[0].getFilter();
            assertThat(termQueryBuilder.fieldName(), equalTo("field2"));
            assertThat(termQueryBuilder.value(), equalTo("value2"));
            assertThat(functionScoreQueryBuilder.filterFunctionBuilders()[1].getFilter(), instanceOf(TermQueryBuilder.class));
            termQueryBuilder = (TermQueryBuilder) functionScoreQueryBuilder.filterFunctionBuilders()[1].getFilter();
            assertThat(termQueryBuilder.fieldName(), equalTo("field3"));
            assertThat(termQueryBuilder.value(), equalTo("value3"));
            assertThat(functionScoreQueryBuilder.filterFunctionBuilders()[2].getFilter(), instanceOf(MatchAllQueryBuilder.class));
            assertThat(functionScoreQueryBuilder.filterFunctionBuilders()[0].getScoreFunction(), instanceOf(RandomScoreFunctionBuilder.class));
            RandomScoreFunctionBuilder randomScoreFunctionBuilder = (RandomScoreFunctionBuilder) functionScoreQueryBuilder.filterFunctionBuilders()[0].getScoreFunction();
            assertThat(randomScoreFunctionBuilder.getSeed(), equalTo(123456));
            assertThat(randomScoreFunctionBuilder.getWeight(), equalTo(3f));
            assertThat(functionScoreQueryBuilder.filterFunctionBuilders()[1].getScoreFunction(), instanceOf(WeightBuilder.class));
            WeightBuilder weightBuilder = (WeightBuilder) functionScoreQueryBuilder.filterFunctionBuilders()[1].getScoreFunction();
            assertThat(weightBuilder.getWeight(), equalTo(9f));
            assertThat(functionScoreQueryBuilder.filterFunctionBuilders()[2].getScoreFunction(), instanceOf(GaussDecayFunctionBuilder.class));
            GaussDecayFunctionBuilder gaussDecayFunctionBuilder = (GaussDecayFunctionBuilder) functionScoreQueryBuilder.filterFunctionBuilders()[2].getScoreFunction();
            assertThat(gaussDecayFunctionBuilder.getFieldName(), equalTo("field_name"));
            assertThat(functionScoreQueryBuilder.boost(), equalTo(3f));
            assertThat(functionScoreQueryBuilder.scoreMode(), equalTo(FiltersFunctionScoreQuery.ScoreMode.AVG));
            assertThat(functionScoreQueryBuilder.boostMode(), equalTo(CombineFunction.REPLACE));
            assertThat(functionScoreQueryBuilder.maxBoost(), equalTo(10f));

            if (i < XContentType.values().length) {
                queryBuilder = parseQuery(((AbstractQueryBuilder)queryBuilder).buildAsBytes(XContentType.values()[i]));
            }
        }
    }

    public void testParseSingleFunction() throws IOException {
        String functionScoreQuery = "{\n" +
                "    \"function_score\":{\n" +
                "        \"query\":{\n" +
                "            \"term\":{\n" +
                "                \"field1\":\"value1\"\n" +
                "            }\n" +
                "        },\n" +
                "        \"gauss\":  {\n" +
                "            \"field_name\":  {\n" +
                "                \"origin\":0.5,\n" +
                "                \"scale\":0.6\n" +
                "            }\n" +
                "         },\n" +
                "        \"boost\" : 3,\n" +
                "        \"score_mode\" : \"avg\",\n" +
                "        \"boost_mode\" : \"replace\",\n" +
                "        \"max_boost\" : 10\n" +
                "    }\n" +
                "}";

        QueryBuilder<?> queryBuilder = parseQuery(functionScoreQuery);
        //given that we copy part of the decay functions as bytes, we test that fromXContent and toXContent both work no matter what the initial format was
        for (int i = 0; i <= XContentType.values().length; i++) {
            assertThat(queryBuilder, instanceOf(FunctionScoreQueryBuilder.class));
            FunctionScoreQueryBuilder functionScoreQueryBuilder = (FunctionScoreQueryBuilder) queryBuilder;
            assertThat(functionScoreQueryBuilder.query(), instanceOf(TermQueryBuilder.class));
            TermQueryBuilder termQueryBuilder = (TermQueryBuilder) functionScoreQueryBuilder.query();
            assertThat(termQueryBuilder.fieldName(), equalTo("field1"));
            assertThat(termQueryBuilder.value(), equalTo("value1"));
            assertThat(functionScoreQueryBuilder.filterFunctionBuilders().length, equalTo(1));
            assertThat(functionScoreQueryBuilder.filterFunctionBuilders()[0].getFilter(), instanceOf(MatchAllQueryBuilder.class));
            assertThat(functionScoreQueryBuilder.filterFunctionBuilders()[0].getScoreFunction(), instanceOf(GaussDecayFunctionBuilder.class));
            GaussDecayFunctionBuilder gaussDecayFunctionBuilder = (GaussDecayFunctionBuilder) functionScoreQueryBuilder.filterFunctionBuilders()[0].getScoreFunction();
            assertThat(gaussDecayFunctionBuilder.getFieldName(), equalTo("field_name"));
            assertThat(gaussDecayFunctionBuilder.getWeight(), nullValue());
            assertThat(functionScoreQueryBuilder.boost(), equalTo(3f));
            assertThat(functionScoreQueryBuilder.scoreMode(), equalTo(FiltersFunctionScoreQuery.ScoreMode.AVG));
            assertThat(functionScoreQueryBuilder.boostMode(), equalTo(CombineFunction.REPLACE));
            assertThat(functionScoreQueryBuilder.maxBoost(), equalTo(10f));

            if (i < XContentType.values().length) {
                queryBuilder = parseQuery(((AbstractQueryBuilder)queryBuilder).buildAsBytes(XContentType.values()[i]));
            }
        }
    }

    public void testProperErrorMessageWhenTwoFunctionsDefinedInQueryBody() throws IOException {
        //without a functions array, we support only a single function, weight can't be associated with the function either.
        String functionScoreQuery = "{\n" +
                "    \"function_score\": {\n" +
                "      \"script_score\": {\n" +
                "        \"script\": \"5\"\n" +
                "      },\n" +
                "      \"weight\": 2\n" +
                "    }\n" +
                "}";
        try {
            parseQuery(functionScoreQuery);
            fail("parsing should have failed");
        } catch(ParsingException e) {
            assertThat(e.getMessage(), containsString("use [functions] array if you want to define several functions."));
        }
    }

    public void testProperErrorMessageWhenTwoFunctionsDefinedInFunctionsArray() throws IOException {
        String functionScoreQuery = "{\n" +
                "    \"function_score\":{\n" +
                "        \"functions\":  [\n" +
                "            {\n" +
                "                \"random_score\":  {\n" +
                "                    \"seed\":123456\n" +
                "                },\n" +
                "                \"weight\": 3,\n" +
                "                \"script_score\": {\n" +
                "                    \"script\": \"_index['text']['foo'].tf()\"\n" +
                "                },\n" +
                "                \"filter\": {\n" +
                "                    \"term\":{\n" +
                "                        \"field2\":\"value2\"\n" +
                "                    }\n" +
                "                }\n" +
                "            }\n" +
                "        ]\n" +
                "    }\n" +
                "}";

        try {
            parseQuery(functionScoreQuery);
            fail("parsing should have failed");
        } catch(ParsingException e) {
            assertThat(e.getMessage(), containsString("failed to parse function_score functions. already found [random_score], now encountering [script_score]."));
        }
    }

    public void testProperErrorMessageWhenMissingFunction() throws IOException {
        String functionScoreQuery = "{\n" +
                "    \"function_score\":{\n" +
                "        \"functions\":  [\n" +
                "            {\n" +
                "                \"filter\": {\n" +
                "                    \"term\":{\n" +
                "                        \"field2\":\"value2\"\n" +
                "                    }\n" +
                "                }\n" +
                "            }\n" +
                "        ]\n" +
                "    }\n" +
                "}";
        try {
            parseQuery(functionScoreQuery);
            fail("parsing should have failed");
        } catch(ParsingException e) {
            assertThat(e.getMessage(), containsString("an entry in functions list is missing a function."));
        }
    }

    public void testWeight1fStillProducesWeightFunction() throws IOException {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        String queryString = jsonBuilder().startObject()
                .startObject("function_score")
                .startArray("functions")
                .startObject()
                .startObject("field_value_factor")
                .field("field", INT_FIELD_NAME)
                .endObject()
                .field("weight", 1.0)
                .endObject()
                .endArray()
                .endObject()
                .endObject().string();
        QueryBuilder<?> query = parseQuery(queryString);
        assertThat(query, instanceOf(FunctionScoreQueryBuilder.class));
        FunctionScoreQueryBuilder functionScoreQueryBuilder = (FunctionScoreQueryBuilder) query;
        assertThat(functionScoreQueryBuilder.filterFunctionBuilders()[0].getScoreFunction(), instanceOf(FieldValueFactorFunctionBuilder.class));
        FieldValueFactorFunctionBuilder fieldValueFactorFunctionBuilder = (FieldValueFactorFunctionBuilder) functionScoreQueryBuilder.filterFunctionBuilders()[0].getScoreFunction();
        assertThat(fieldValueFactorFunctionBuilder.fieldName(), equalTo(INT_FIELD_NAME));
        assertThat(fieldValueFactorFunctionBuilder.factor(), equalTo(FieldValueFactorFunctionBuilder.DEFAULT_FACTOR));
        assertThat(fieldValueFactorFunctionBuilder.modifier(), equalTo(FieldValueFactorFunctionBuilder.DEFAULT_MODIFIER));
        assertThat(fieldValueFactorFunctionBuilder.getWeight(), equalTo(1f));
        assertThat(fieldValueFactorFunctionBuilder.missing(), nullValue());

        Query luceneQuery = query.toQuery(createShardContext());
        assertThat(luceneQuery, instanceOf(FunctionScoreQuery.class));
        FunctionScoreQuery functionScoreQuery = (FunctionScoreQuery) luceneQuery;
        assertThat(functionScoreQuery.getFunction(), instanceOf(WeightFactorFunction.class));
        WeightFactorFunction weightFactorFunction = (WeightFactorFunction) functionScoreQuery.getFunction();
        assertThat(weightFactorFunction.getWeight(), equalTo(1.0f));
        assertThat(weightFactorFunction.getScoreFunction(), instanceOf(FieldValueFactorFunction.class));
    }

    public void testProperErrorMessagesForMisplacedWeightsAndFunctions() throws IOException {
        String query = jsonBuilder().startObject().startObject("function_score")
                .startArray("functions")
                .startObject().startObject("script_score").field("script", "3").endObject().endObject()
                .endArray()
                .field("weight", 2)
                .endObject().endObject().string();
        try {
            parseQuery(query);
            fail("Expect exception here because array of functions and one weight in body is not allowed.");
        } catch (ParsingException e) {
            assertThat(e.getMessage(), containsString("you can either define [functions] array or a single function, not both. already found [functions] array, now encountering [weight]."));
        }
        query = jsonBuilder().startObject().startObject("function_score")
                .field("weight", 2)
                .startArray("functions")
                .startObject().endObject()
                .endArray()
                .endObject().endObject().string();
        try {
            parseQuery(query);
            fail("Expect exception here because array of functions and one weight in body is not allowed.");
        } catch (ParsingException e) {
            assertThat(e.getMessage(), containsString("you can either define [functions] array or a single function, not both. already found [weight], now encountering [functions]."));
        }
    }

    public void testMalformedThrowsException() throws IOException {
        try {
            parseQuery(copyToStringFromClasspath("/org/elasticsearch/index/query/faulty-function-score-query.json"));
            fail("Expected JsonParseException");
        } catch (JsonParseException e) {
            assertThat(e.getMessage(), containsString("Unexpected character ('{"));
        }
    }

    public void testCustomWeightFactorQueryBuilderWithFunctionScore() throws IOException {
        Query parsedQuery = parseQuery(functionScoreQuery(termQuery("name.last", "banon"), ScoreFunctionBuilders.weightFactorFunction(1.3f)).buildAsBytes()).toQuery(createShardContext());
        assertThat(parsedQuery, instanceOf(FunctionScoreQuery.class));
        FunctionScoreQuery functionScoreQuery = (FunctionScoreQuery) parsedQuery;
        assertThat(((TermQuery) functionScoreQuery.getSubQuery()).getTerm(), equalTo(new Term("name.last", "banon")));
        assertThat((double) ((WeightFactorFunction) functionScoreQuery.getFunction()).getWeight(), closeTo(1.3, 0.001));
    }

    public void testCustomWeightFactorQueryBuilderWithFunctionScoreWithoutQueryGiven() throws IOException {
        Query parsedQuery = parseQuery(functionScoreQuery(ScoreFunctionBuilders.weightFactorFunction(1.3f)).buildAsBytes()).toQuery(createShardContext());
        assertThat(parsedQuery, instanceOf(FunctionScoreQuery.class));
        FunctionScoreQuery functionScoreQuery = (FunctionScoreQuery) parsedQuery;
        assertThat(functionScoreQuery.getSubQuery() instanceof MatchAllDocsQuery, equalTo(true));
        assertThat((double) ((WeightFactorFunction) functionScoreQuery.getFunction()).getWeight(), closeTo(1.3, 0.001));
    }

    public void testFieldValueFactorFactorArray() throws IOException {
        // don't permit an array of factors
        String querySource = "{" +
                "  \"function_score\": {" +
                "    \"query\": {" +
                "      \"match\": {\"name\": \"foo\"}" +
                "      }," +
                "      \"functions\": [" +
                "        {" +
                "          \"field_value_factor\": {" +
                "            \"field\": \"test\"," +
                "            \"factor\": [1.2,2]" +
                "          }" +
                "        }" +
                "      ]" +
                "    }" +
                "}";
        try {
            parseQuery(querySource);
            fail("parsing should have failed");
        } catch(ParsingException e) {
            assertThat(e.getMessage(), containsString("[field_value_factor] field 'factor' does not support lists or objects"));
        }
    }

    public void testFromJson() throws IOException {
        String json =
                "{\n" +
                "  \"function_score\" : {\n" +
                "    \"query\" : { },\n" +
                "    \"functions\" : [ {\n" +
                "      \"filter\" : { },\n" +
                "      \"weight\" : 23.0,\n" +
                "      \"random_score\" : { }\n" +
                "    }, {\n" +
                "      \"filter\" : { },\n" +
                "      \"weight\" : 5.0\n" +
                "    } ],\n" +
                "    \"score_mode\" : \"multiply\",\n" +
                "    \"boost_mode\" : \"multiply\",\n" +
                "    \"max_boost\" : 100.0,\n" +
                "    \"min_score\" : 1.0,\n" +
                "    \"boost\" : 42.0\n" +
                "  }\n" +
                "}";

        FunctionScoreQueryBuilder parsed = (FunctionScoreQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, 2, parsed.filterFunctionBuilders().length);
        assertEquals(json, 42, parsed.boost(), 0.0001);
        assertEquals(json, 100, parsed.maxBoost(), 0.00001);
        assertEquals(json, 1, parsed.getMinScore(), 0.0001);
    }
}
