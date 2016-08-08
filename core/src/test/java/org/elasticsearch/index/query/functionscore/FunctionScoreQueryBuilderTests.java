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
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FieldValueFactorFunction;
import org.elasticsearch.common.lucene.search.function.FiltersFunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.WeightFactorFunction;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.RandomQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.WrapperQueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder.FilterFunctionBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.hamcrest.Matcher;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.fieldValueFactorFunction;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.randomFunction;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.weightFactorFunction;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class FunctionScoreQueryBuilderTests extends AbstractQueryTestCase<FunctionScoreQueryBuilder> {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(TestPlugin.class);
    }

    @Override
    protected FunctionScoreQueryBuilder doCreateTestQueryBuilder() {
        FunctionScoreQueryBuilder functionScoreQueryBuilder = createRandomFunctionScoreBuilder();
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

    /**
     * Creates a random function score query using only constructor params. The caller is responsible for randomizing fields set outside of
     * the constructor.
     */
    private static FunctionScoreQueryBuilder createRandomFunctionScoreBuilder() {
        switch (randomIntBetween(0, 3)) {
        case 0:
            FilterFunctionBuilder[] functions = new FilterFunctionBuilder[randomIntBetween(0, 3)];
            for (int i = 0; i < functions.length; i++) {
                functions[i] = new FilterFunctionBuilder(RandomQueryBuilder.createQuery(random()), randomScoreFunction());
            }
            if (randomBoolean()) {
                return new FunctionScoreQueryBuilder(RandomQueryBuilder.createQuery(random()), functions);
            }
            return new FunctionScoreQueryBuilder(functions);
        case 1:
            return new FunctionScoreQueryBuilder(randomScoreFunction());
        case 2:
            return new FunctionScoreQueryBuilder(RandomQueryBuilder.createQuery(random()), randomScoreFunction());
        case 3:
            return new FunctionScoreQueryBuilder(RandomQueryBuilder.createQuery(random()));
        default:
            throw new UnsupportedOperationException();
        }
    }

    private static ScoreFunctionBuilder<?> randomScoreFunction() {
        if (randomBoolean()) {
            return new WeightBuilder().setWeight(randomFloat());
        }
        ScoreFunctionBuilder<?> functionBuilder;
        switch (randomIntBetween(0, 3)) {
        case 0:
            DecayFunctionBuilder<?> decayFunctionBuilder = createRandomDecayFunction();
            if (randomBoolean()) {
                decayFunctionBuilder.setMultiValueMode(randomFrom(MultiValueMode.values()));
            }
            functionBuilder = decayFunctionBuilder;
            break;
        case 1:
            FieldValueFactorFunctionBuilder fieldValueFactorFunctionBuilder = fieldValueFactorFunction(fieldValueFactorCompatibleField());
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
            String script = "1";
            Map<String, Object> params = Collections.emptyMap();
            functionBuilder = new ScriptScoreFunctionBuilder(
                    new Script(script, ScriptService.ScriptType.INLINE, MockScriptEngine.NAME, params));
            break;
        case 3:
            RandomScoreFunctionBuilder randomScoreFunctionBuilder = new RandomScoreFunctionBuilderWithFixedSeed();
            if (randomBoolean()) {
                randomScoreFunctionBuilder.seed(randomLong());
            } else if (randomBoolean()) {
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

    /**
     * A random field compatible with FieldValueFactor.
     */
    private static String fieldValueFactorCompatibleField() {
        return randomFrom(INT_FIELD_NAME, DOUBLE_FIELD_NAME, DATE_FIELD_NAME);
    }

    /**
     * Create a random decay function setting all of its constructor parameters randomly. The caller is responsible for randomizing other
     * fields.
     */
    private static DecayFunctionBuilder<?> createRandomDecayFunction() {
        String field = randomFrom(INT_FIELD_NAME, DOUBLE_FIELD_NAME, DATE_FIELD_NAME, GEO_POINT_FIELD_NAME);
        Object origin;
        Object scale;
        Object offset;
        switch (field) {
        case GEO_POINT_FIELD_NAME:
            origin = new GeoPoint(randomDouble(), randomDouble()).geohash();
            scale = randomFrom(DistanceUnit.values()).toString(randomDouble());
            offset = randomFrom(DistanceUnit.values()).toString(randomDouble());
            break;
        case DATE_FIELD_NAME:
            origin = new DateTime(System.currentTimeMillis() - randomIntBetween(0, 1000000), DateTimeZone.UTC).toString();
            scale = randomPositiveTimeValue();
            offset = randomPositiveTimeValue();
            break;
        default:
            origin = randomBoolean() ? randomInt() : randomFloat();
            scale = randomBoolean() ? between(1, Integer.MAX_VALUE) : randomFloat() + Float.MIN_NORMAL;
            offset = randomBoolean() ? between(1, Integer.MAX_VALUE) : randomFloat() + Float.MIN_NORMAL;
            break;
        }
        offset = randomBoolean() ? null : offset;
        double decay = randomDouble();
        switch (randomIntBetween(0, 2)) {
        case 0:
            return new GaussDecayFunctionBuilder(field, origin, scale, offset, decay);
        case 1:
            return new ExponentialDecayFunctionBuilder(field, origin, scale, offset, decay);
        case 2:
            return new LinearDecayFunctionBuilder(field, origin, scale, offset, decay);
        default:
            throw new UnsupportedOperationException();
        }
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
        expectThrows(IllegalArgumentException.class, () -> new FunctionScoreQueryBuilder((QueryBuilder) null));
        expectThrows(IllegalArgumentException.class, () -> new FunctionScoreQueryBuilder((ScoreFunctionBuilder<?>) null));
        expectThrows(IllegalArgumentException.class, () -> new FunctionScoreQueryBuilder((FilterFunctionBuilder[]) null));
        expectThrows(IllegalArgumentException.class, () -> new FunctionScoreQueryBuilder(null, randomFunction(123)));
        expectThrows(IllegalArgumentException.class, () -> new FunctionScoreQueryBuilder(matchAllQuery(), (ScoreFunctionBuilder<?>) null));
        expectThrows(IllegalArgumentException.class, () -> new FunctionScoreQueryBuilder(matchAllQuery(), (FilterFunctionBuilder[]) null));
        expectThrows(IllegalArgumentException.class, () -> new FunctionScoreQueryBuilder(null, new FilterFunctionBuilder[0]));
        expectThrows(IllegalArgumentException.class,
                () -> new FunctionScoreQueryBuilder(matchAllQuery(), new FilterFunctionBuilder[] { null }));
        expectThrows(IllegalArgumentException.class, () -> new FilterFunctionBuilder((ScoreFunctionBuilder<?>) null));
        expectThrows(IllegalArgumentException.class, () -> new FilterFunctionBuilder(null, randomFunction(123)));
        expectThrows(IllegalArgumentException.class, () -> new FilterFunctionBuilder(matchAllQuery(), null));
        FunctionScoreQueryBuilder builder = new FunctionScoreQueryBuilder(matchAllQuery());
        expectThrows(IllegalArgumentException.class, () -> builder.scoreMode(null));
        expectThrows(IllegalArgumentException.class, () -> builder.boostMode(null));
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

        QueryBuilder queryBuilder = parseQuery(functionScoreQuery);
        /*
         * given that we copy part of the decay functions as bytes, we test that fromXContent and toXContent both work no matter what the
         * initial format was
         */
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
            assertThat(functionScoreQueryBuilder.filterFunctionBuilders()[0].getScoreFunction(),
                    instanceOf(RandomScoreFunctionBuilder.class));
            RandomScoreFunctionBuilder randomScoreFunctionBuilder = (RandomScoreFunctionBuilder) functionScoreQueryBuilder
                    .filterFunctionBuilders()[0].getScoreFunction();
            assertThat(randomScoreFunctionBuilder.getSeed(), equalTo(123456));
            assertThat(randomScoreFunctionBuilder.getWeight(), equalTo(3f));
            assertThat(functionScoreQueryBuilder.filterFunctionBuilders()[1].getScoreFunction(), instanceOf(WeightBuilder.class));
            WeightBuilder weightBuilder = (WeightBuilder) functionScoreQueryBuilder.filterFunctionBuilders()[1].getScoreFunction();
            assertThat(weightBuilder.getWeight(), equalTo(9f));
            assertThat(functionScoreQueryBuilder.filterFunctionBuilders()[2].getScoreFunction(),
                    instanceOf(GaussDecayFunctionBuilder.class));
            GaussDecayFunctionBuilder gaussDecayFunctionBuilder = (GaussDecayFunctionBuilder) functionScoreQueryBuilder
                    .filterFunctionBuilders()[2].getScoreFunction();
            assertThat(gaussDecayFunctionBuilder.getFieldName(), equalTo("field_name"));
            assertThat(functionScoreQueryBuilder.boost(), equalTo(3f));
            assertThat(functionScoreQueryBuilder.scoreMode(), equalTo(FiltersFunctionScoreQuery.ScoreMode.AVG));
            assertThat(functionScoreQueryBuilder.boostMode(), equalTo(CombineFunction.REPLACE));
            assertThat(functionScoreQueryBuilder.maxBoost(), equalTo(10f));

            if (i < XContentType.values().length) {
                queryBuilder = parseQuery(((AbstractQueryBuilder) queryBuilder).buildAsBytes(XContentType.values()[i]));
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

        QueryBuilder queryBuilder = parseQuery(functionScoreQuery);
        /*
         * given that we copy part of the decay functions as bytes, we test that fromXContent and toXContent both work no matter what the
         * initial format was
         */
        for (int i = 0; i <= XContentType.values().length; i++) {
            assertThat(queryBuilder, instanceOf(FunctionScoreQueryBuilder.class));
            FunctionScoreQueryBuilder functionScoreQueryBuilder = (FunctionScoreQueryBuilder) queryBuilder;
            assertThat(functionScoreQueryBuilder.query(), instanceOf(TermQueryBuilder.class));
            TermQueryBuilder termQueryBuilder = (TermQueryBuilder) functionScoreQueryBuilder.query();
            assertThat(termQueryBuilder.fieldName(), equalTo("field1"));
            assertThat(termQueryBuilder.value(), equalTo("value1"));
            assertThat(functionScoreQueryBuilder.filterFunctionBuilders().length, equalTo(1));
            assertThat(functionScoreQueryBuilder.filterFunctionBuilders()[0].getFilter(), instanceOf(MatchAllQueryBuilder.class));
            assertThat(functionScoreQueryBuilder.filterFunctionBuilders()[0].getScoreFunction(),
                    instanceOf(GaussDecayFunctionBuilder.class));
            GaussDecayFunctionBuilder gaussDecayFunctionBuilder = (GaussDecayFunctionBuilder) functionScoreQueryBuilder
                    .filterFunctionBuilders()[0].getScoreFunction();
            assertThat(gaussDecayFunctionBuilder.getFieldName(), equalTo("field_name"));
            assertThat(gaussDecayFunctionBuilder.getWeight(), nullValue());
            assertThat(functionScoreQueryBuilder.boost(), equalTo(3f));
            assertThat(functionScoreQueryBuilder.scoreMode(), equalTo(FiltersFunctionScoreQuery.ScoreMode.AVG));
            assertThat(functionScoreQueryBuilder.boostMode(), equalTo(CombineFunction.REPLACE));
            assertThat(functionScoreQueryBuilder.maxBoost(), equalTo(10f));

            if (i < XContentType.values().length) {
                queryBuilder = parseQuery(((AbstractQueryBuilder) queryBuilder).buildAsBytes(XContentType.values()[i]));
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
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(functionScoreQuery));
        assertThat(e.getMessage(), containsString("use [functions] array if you want to define several functions."));
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
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(functionScoreQuery));
        assertThat(e.getMessage(),
                containsString("failed to parse function_score functions. already found [random_score], now encountering [script_score]."));
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
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(functionScoreQuery));
        assertThat(e.getMessage(), containsString("an entry in functions list is missing a function."));
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
        QueryBuilder query = parseQuery(queryString);
        assertThat(query, instanceOf(FunctionScoreQueryBuilder.class));
        FunctionScoreQueryBuilder functionScoreQueryBuilder = (FunctionScoreQueryBuilder) query;
        assertThat(functionScoreQueryBuilder.filterFunctionBuilders()[0].getScoreFunction(),
                instanceOf(FieldValueFactorFunctionBuilder.class));
        FieldValueFactorFunctionBuilder fieldValueFactorFunctionBuilder = (FieldValueFactorFunctionBuilder) functionScoreQueryBuilder
                .filterFunctionBuilders()[0].getScoreFunction();
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
        expectParsingException(query, "[you can either define [functions] array or a single function, not both. already "
                + "found [functions] array, now encountering [weight].]");
        query = jsonBuilder().startObject().startObject("function_score")
            .field("weight", 2)
            .startArray("functions")
            .startObject().endObject()
            .endArray()
            .endObject().endObject().string();
        expectParsingException(query, "[you can either define [functions] array or a single function, not both. already found "
                + "[weight], now encountering [functions].]");
    }

    public void testMalformedThrowsException() throws IOException {
        String json = "{\n" +
            "    \"function_score\":{\n" +
            "        \"query\":{\n" +
            "            \"term\":{\n" +
            "                \"name.last\":\"banon\"\n" +
            "            }\n" +
            "        },\n" +
            "        \"functions\": [\n" +
            "            {\n" +
            "                {\n" +
            "            }\n" +
            "        ]\n" +
            "    }\n" +
            "}";
        JsonParseException e = expectThrows(JsonParseException.class, () -> parseQuery(json));
        assertThat(e.getMessage(), containsString("Unexpected character ('{"));
    }

    public void testCustomWeightFactorQueryBuilderWithFunctionScore() throws IOException {
        Query parsedQuery = parseQuery(functionScoreQuery(termQuery("name.last", "banon"), weightFactorFunction(1.3f)).buildAsBytes())
                .toQuery(createShardContext());
        assertThat(parsedQuery, instanceOf(FunctionScoreQuery.class));
        FunctionScoreQuery functionScoreQuery = (FunctionScoreQuery) parsedQuery;
        assertThat(((TermQuery) functionScoreQuery.getSubQuery()).getTerm(), equalTo(new Term("name.last", "banon")));
        assertThat((double) ((WeightFactorFunction) functionScoreQuery.getFunction()).getWeight(), closeTo(1.3, 0.001));
    }

    public void testCustomWeightFactorQueryBuilderWithFunctionScoreWithoutQueryGiven() throws IOException {
        Query parsedQuery = parseQuery(functionScoreQuery(weightFactorFunction(1.3f)).buildAsBytes()).toQuery(createShardContext());
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
        expectParsingException(querySource, containsString("[field_value_factor] field 'factor' does not support lists or objects"));
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

        FunctionScoreQueryBuilder parsed = (FunctionScoreQueryBuilder) parseQuery(json, ParseFieldMatcher.EMPTY);
        // this should be equivalent to the same with a match_all query
        String expected =
                "{\n" +
                    "  \"function_score\" : {\n" +
                    "    \"query\" : { \"match_all\" : {} },\n" +
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

        FunctionScoreQueryBuilder expectedParsed = (FunctionScoreQueryBuilder) parseQuery(json, ParseFieldMatcher.EMPTY);
        assertEquals(expectedParsed, parsed);

        assertEquals(json, 2, parsed.filterFunctionBuilders().length);
        assertEquals(json, 42, parsed.boost(), 0.0001);
        assertEquals(json, 100, parsed.maxBoost(), 0.00001);
        assertEquals(json, 1, parsed.getMinScore(), 0.0001);
    }

    @Override
    public void testMustRewrite() throws IOException {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        super.testMustRewrite();
    }

    public void testRewrite() throws IOException {
        FunctionScoreQueryBuilder functionScoreQueryBuilder = new FunctionScoreQueryBuilder(
                new WrapperQueryBuilder(new TermQueryBuilder("foo", "bar").toString()));
        FunctionScoreQueryBuilder rewrite = (FunctionScoreQueryBuilder) functionScoreQueryBuilder.rewrite(createShardContext());
        assertNotSame(functionScoreQueryBuilder, rewrite);
        assertEquals(rewrite.query(), new TermQueryBuilder("foo", "bar"));
    }

    public void testRewriteWithFunction() throws IOException {
        QueryBuilder firstFunction = new WrapperQueryBuilder(new TermQueryBuilder("tq", "1").toString());
        TermQueryBuilder secondFunction = new TermQueryBuilder("tq", "2");
        QueryBuilder queryBuilder = randomBoolean() ? new WrapperQueryBuilder(new TermQueryBuilder("foo", "bar").toString())
                : new TermQueryBuilder("foo", "bar");
        FunctionScoreQueryBuilder functionScoreQueryBuilder = new FunctionScoreQueryBuilder(queryBuilder,
                new FunctionScoreQueryBuilder.FilterFunctionBuilder[] {
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(firstFunction, new RandomScoreFunctionBuilder()),
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(secondFunction, new RandomScoreFunctionBuilder()) });
        FunctionScoreQueryBuilder rewrite = (FunctionScoreQueryBuilder) functionScoreQueryBuilder.rewrite(createShardContext());
        assertNotSame(functionScoreQueryBuilder, rewrite);
        assertEquals(rewrite.query(), new TermQueryBuilder("foo", "bar"));
        assertEquals(rewrite.filterFunctionBuilders()[0].getFilter(), new TermQueryBuilder("tq", "1"));
        assertSame(rewrite.filterFunctionBuilders()[1].getFilter(), secondFunction);
    }

    public void testQueryMalformedArrayNotSupported() throws IOException {
        String json =
            "{\n" +
                "  \"function_score\" : {\n" +
                "    \"not_supported\" : []\n" +
                "  }\n" +
                "}";

        expectParsingException(json, "array [not_supported] is not supported");
    }

    public void testQueryMalformedFieldNotSupported() throws IOException {
        String json =
            "{\n" +
                "  \"function_score\" : {\n" +
                "    \"not_supported\" : \"value\"\n" +
                "  }\n" +
                "}";

        expectParsingException(json, "field [not_supported] is not supported");
    }

    public void testMalformedQueryFunctionFieldNotSupported() throws IOException {
        String json =
            "{\n" +
                "  \"function_score\" : {\n" +
                "    \"functions\" : [ {\n" +
                "      \"not_supported\" : 23.0\n" +
                "    }\n" +
                "  }\n" +
                "}";

        expectParsingException(json, "field [not_supported] is not supported");
    }

    public void testMalformedQuery() throws IOException {
        //verify that an error is thrown rather than setting the query twice (https://github.com/elastic/elasticsearch/issues/16583)
        String json = "{\n" +
                "    \"function_score\":{\n" +
                "        \"query\":{\n" +
                "            \"bool\":{\n" +
                "                \"must\":{\"match\":{\"field\":\"value\"}}" +
                "             },\n" +
                "            \"ignored_field_name\": {\n" +
                "                {\"match\":{\"field\":\"value\"}}\n" +
                "            }\n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "}";
        expectParsingException(json, "[query] is already defined.");
    }

    private void expectParsingException(String json, Matcher<String> messageMatcher) {
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertThat(e.getMessage(), messageMatcher);
    }

    private void expectParsingException(String json, String message) {
        expectParsingException(json, equalTo("failed to parse [function_score] query. " + message));
    }

    /**
     * A hack on top of the normal random score function that fixed toQuery to work properly in this unit testing environment.
     */
    static class RandomScoreFunctionBuilderWithFixedSeed extends RandomScoreFunctionBuilder {
        public static final String NAME = "random_with_fixed_seed";

        public RandomScoreFunctionBuilderWithFixedSeed() {
        }

        /**
         * Read from a stream.
         */
        public RandomScoreFunctionBuilderWithFixedSeed(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        int getCurrentShardId() {
            // We can't use the method that normal queries use during this test so we hack something instead
            return 0;
        }

        @Override
        public String getName() {
            return NAME;
        }

        public static RandomScoreFunctionBuilder fromXContent(QueryParseContext parseContext)
                throws IOException, ParsingException {
            RandomScoreFunctionBuilder builder = RandomScoreFunctionBuilder.fromXContent(parseContext);
            RandomScoreFunctionBuilderWithFixedSeed replacement = new RandomScoreFunctionBuilderWithFixedSeed();
            replacement.seed(builder.getSeed());
            return replacement;
        }
    }

    public static class TestPlugin extends Plugin implements SearchPlugin {
        @Override
        public List<ScoreFunctionSpec<?>> getScoreFunctions() {
            return singletonList(new ScoreFunctionSpec<>(RandomScoreFunctionBuilderWithFixedSeed.NAME,
                    RandomScoreFunctionBuilderWithFixedSeed::new, RandomScoreFunctionBuilderWithFixedSeed::fromXContent));
        }
    }
}
