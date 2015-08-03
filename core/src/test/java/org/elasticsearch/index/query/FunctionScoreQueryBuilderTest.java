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

package org.elasticsearch.index.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FieldValueFactorFunction;
import org.elasticsearch.common.lucene.search.function.FiltersFunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.index.query.functionscore.DecayFunctionBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.elasticsearch.index.query.functionscore.exp.ExponentialDecayFunctionBuilder;
import org.elasticsearch.index.query.functionscore.factor.FactorBuilder;
import org.elasticsearch.index.query.functionscore.fieldvaluefactor.FieldValueFactorFunctionBuilder;
import org.elasticsearch.index.query.functionscore.gauss.GaussDecayFunctionBuilder;
import org.elasticsearch.index.query.functionscore.lin.LinearDecayFunctionBuilder;
import org.elasticsearch.index.query.functionscore.random.RandomScoreFunctionBuilder;
import org.elasticsearch.index.query.functionscore.script.ScriptScoreFunctionBuilder;
import org.elasticsearch.index.query.functionscore.weight.WeightBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.MultiValueMode;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class FunctionScoreQueryBuilderTest extends BaseQueryTestCase<FunctionScoreQueryBuilder> {

    @Override
    protected FunctionScoreQueryBuilder doCreateTestQueryBuilder() {
        // random top level query and filter
        FunctionScoreQueryBuilder query = newFunctionScoreQueryBuilder();

        // random functions
        if (randomBoolean()) {
            query.addAll(randomFilterFunctions());
        }
        // how functions should be combined
        if (randomBoolean()) {
            query.boostMode(randomFrom(CombineFunction.values()));
        }
        // how function scores should be combined with the top level query
        if (randomBoolean()) {
            query.scoreMode(randomFrom(FiltersFunctionScoreQuery.ScoreMode.values()));
        }
        // maximum boost and min score cutoff
        if (randomBoolean()) {
            query.maxBoost(10 * randomFloat());
        }
        if (randomBoolean()) {
            query.minScore(10 * randomFloat());
        }
        return query;
    }

    private FunctionScoreQueryBuilder newFunctionScoreQueryBuilder() {
        return new FunctionScoreQueryBuilder(
                randomBoolean() ? RandomQueryBuilder.createQuery(random()) : null,
                randomBoolean() ? RandomQueryBuilder.createQuery(random()) : null);
    }

    private List<Tuple<QueryBuilder, ScoreFunctionBuilder>> randomFilterFunctions() {
        List<Tuple<QueryBuilder, ScoreFunctionBuilder>> filterFunctions = new ArrayList<>();
        for (int i = 0; i < randomInt(5); i++) {
            filterFunctions.add(randomFilterFunction());
        }
        return filterFunctions;
    }

    private Tuple<QueryBuilder, ScoreFunctionBuilder> randomFilterFunction() {
        QueryBuilder filter = randomBoolean() ? RandomQueryBuilder.createQuery(random()) : null;
        ScoreFunctionBuilder scoreFunctionBuilder = null;
        if (randomBoolean()) {
            switch (randomInt(4)) {
                case 0:
                    scoreFunctionBuilder = randomScriptScoreFunctionBuilder();
                    break;
                case 1:
                    scoreFunctionBuilder = randomWeightBuilder();
                    break;
                case 2:
                    scoreFunctionBuilder = randomRandomScoreFunctionBuilder();
                    break;
                case 3:
                    scoreFunctionBuilder = randomFieldFieldValueFactorFunctionBuilder();
                    break;
                case 4:
                    scoreFunctionBuilder = randomFactorBuilder();
                    break;
            }
        } else {
            scoreFunctionBuilder = randomDecayScoreFunctionBuilder();
        }
        if (randomBoolean() && !(scoreFunctionBuilder instanceof FactorBuilder)) {  // factor builder does not support weight
            scoreFunctionBuilder.setWeight(10 * randomFloat());
        }
        return new Tuple(filter, scoreFunctionBuilder);
    }

    private ScoreFunctionBuilder randomScriptScoreFunctionBuilder() {
        return new ScriptScoreFunctionBuilder(new Script("5 * 2 > 2"));
    }

    private ScoreFunctionBuilder randomWeightBuilder() {
        return new WeightBuilder().setWeight(10 * randomFloat());
    }

    private ScoreFunctionBuilder randomRandomScoreFunctionBuilder() {
        RandomScoreFunctionBuilder functionBuilder = new RandomScoreFunctionBuilder();
        if (randomBoolean()) {
            functionBuilder.seed(randomInt(100));
        }
        return functionBuilder;
    }

    private ScoreFunctionBuilder randomFieldFieldValueFactorFunctionBuilder() {
        FieldValueFactorFunctionBuilder functionBuilder = new FieldValueFactorFunctionBuilder(INT_FIELD_NAME);
        if (randomBoolean()) {
            functionBuilder.factor(10 * randomFloat());
        }
        if (randomBoolean()) {
            functionBuilder.missing(10 * randomDouble());
        }
        if (randomBoolean()) {
            functionBuilder.modifier(randomFrom(FieldValueFactorFunction.Modifier.values()));
        }
        return functionBuilder;
    }

    private ScoreFunctionBuilder randomFactorBuilder() {
        return new FactorBuilder().boostFactor(10 * randomFloat());
    }

    private DecayFunctionBuilder randomDecayScoreFunctionBuilder(String mappedFieldName) {
        // origin, scale, offset, decay
        Object origin = null; // cannot be null for number and geo
        Object scale = null;  // cannot be null for number, geo and date
        Object offset = null;
        double decay = randomBoolean() ? randomDouble() : DecayFunctionBuilder.DEFAULT_DECAY; // must be between 0 and 1
        switch (mappedFieldName) {
            case INT_FIELD_NAME:
            case DOUBLE_FIELD_NAME:
                origin = 10 * randomDouble();
                scale = 5 * randomDouble();
                offset = randomBoolean() ? 5 * randomDouble() : null;
                break;
            case DATE_FIELD_NAME:
                origin = randomBoolean() ? randomDateString() : null;
                scale = randomDateDistance();
                offset = randomBoolean() ? randomDateDistance() : null;
                break;
            case GEOPOINT_FIELD_NAME:
                origin = randomGeoPoint();
                scale = randomGeoDistance();
                offset = randomBoolean() ? randomGeoDistance() : null;
                break;
        }
        // gauss, exp, linear
        DecayFunctionBuilder decayFunctionBuilder;
        switch (randomInt(2)) {
            case 0:
                decayFunctionBuilder = new GaussDecayFunctionBuilder(mappedFieldName, origin, scale);
                break;
            case 1:
                decayFunctionBuilder = new ExponentialDecayFunctionBuilder(mappedFieldName, origin, scale);
                break;
            default:
                decayFunctionBuilder = new LinearDecayFunctionBuilder(mappedFieldName, origin, scale);
                break;
        }
        decayFunctionBuilder.setDecay(decay).setOffset(offset);
        // multi-value mode
        if (randomBoolean()) {
            decayFunctionBuilder.setMultiValueMode(randomFrom(MultiValueMode.values()));
        }
        return decayFunctionBuilder;
    }

    private DecayFunctionBuilder randomDecayScoreFunctionBuilder() {
        // number, geo, date (all fields must be mapped)
        String mappedFieldName = randomFrom(INT_FIELD_NAME, DOUBLE_FIELD_NAME, DATE_FIELD_NAME, GEOPOINT_FIELD_NAME);
        return randomDecayScoreFunctionBuilder(mappedFieldName);
    }
    @Override
    protected void doAssertLuceneQuery(FunctionScoreQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        //NO COMMIT we should do more in depth check here
        assertThat(query, instanceOf(FunctionScoreQuery.class));
    }

    @Test
    public void testValidate() {
        FunctionScoreQueryBuilder functionBuilder = newFunctionScoreQueryBuilder()
                .boostMode((String) null)
                .scoreMode((String) null);
        assertThat(functionBuilder.validate().validationErrors().size(), is(2));

        functionBuilder = newFunctionScoreQueryBuilder()
                .boostMode("not a boost mode")
                .scoreMode("not a score mode");
        assertThat(functionBuilder.validate().validationErrors().size(), is(2));

        functionBuilder = newFunctionScoreQueryBuilder()
                .boostMode(randomFrom(CombineFunction.values()))
                .scoreMode((String) null);
        assertThat(functionBuilder.validate().validationErrors().size(), is(1));

        functionBuilder = newFunctionScoreQueryBuilder()
                .scoreMode(randomFrom(FiltersFunctionScoreQuery.ScoreMode.values()))
                .boostMode((String) null);
        assertThat(functionBuilder.validate().validationErrors().size(), is(1));

        functionBuilder = newFunctionScoreQueryBuilder();
        assertNull(functionBuilder.validate());
    }

    @Test
    public void testValidateTopLevelQueryAndFilter() {
        FunctionScoreQueryBuilder functionBuilder = new FunctionScoreQueryBuilder(
                RandomQueryBuilder.createInvalidQuery(random()),
                RandomQueryBuilder.createInvalidQuery(random()));
        assertThat(functionBuilder.validate().validationErrors().size(), is(2));

        functionBuilder = new FunctionScoreQueryBuilder(
                null,
                RandomQueryBuilder.createInvalidQuery(random()));
        assertThat(functionBuilder.validate().validationErrors().size(), is(1));

        functionBuilder = new FunctionScoreQueryBuilder(
                RandomQueryBuilder.createInvalidQuery(random()),
                null);
        assertThat(functionBuilder.validate().validationErrors().size(), is(1));
    }

    @Test
    public void testValidateFilterFunctions() {
        // score function cannot be null
        FunctionScoreQueryBuilder functionBuilder = newFunctionScoreQueryBuilder().add(null);
        assertThat(functionBuilder.validate().validationErrors().size(), is(1));

        // invalid filter
        functionBuilder = newFunctionScoreQueryBuilder().add(
                RandomQueryBuilder.createInvalidQuery(random()),
                new RandomScoreFunctionBuilder());
        assertThat(functionBuilder.validate().validationErrors().size(), is(1));

        // invalid score function
        functionBuilder = newFunctionScoreQueryBuilder().add(new FieldValueFactorFunctionBuilder(null));
        assertThat(functionBuilder.validate().validationErrors().size(), is(1));

        functionBuilder = newFunctionScoreQueryBuilder().add(new ScriptScoreFunctionBuilder(null));
        assertThat(functionBuilder.validate().validationErrors().size(), is(1));

        // invalid decay score functions
        functionBuilder = newFunctionScoreQueryBuilder().add(randomDecayScoreFunctionBuilder().setFieldName(null));
        assertThat(functionBuilder.validate().validationErrors().size(), is(1));

        functionBuilder = newFunctionScoreQueryBuilder().add(randomDecayScoreFunctionBuilder().setScale(null));
        assertThat(functionBuilder.validate().validationErrors().size(), is(1));

        functionBuilder = newFunctionScoreQueryBuilder().add(randomDecayScoreFunctionBuilder().setDecay(10));
        assertThat(functionBuilder.validate().validationErrors().size(), is(1));
    }

    @Test
    public void testUnmapedFields() throws IOException {
        QueryShardContext context = createShardContext();
        context.setAllowUnmappedFields(true);

        String fieldName = "this field is not mapped";
        FunctionScoreQueryBuilder functionBuilder = newFunctionScoreQueryBuilder().add(
                randomDecayScoreFunctionBuilder().setFieldName(fieldName));
        try {
            functionBuilder.toQuery(context);
            fail("should have failed!");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), Matchers.containsString("unknown field ["+fieldName+"]"));
        }

        functionBuilder = newFunctionScoreQueryBuilder().add(
                new FieldValueFactorFunctionBuilder(fieldName));
        try {
            functionBuilder.toQuery(context);
            fail("should have failed!");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), Matchers.containsString("Unable to find a field mapper for field [" + fieldName + "]"));
        }
    }

    @Test
    public void testNullOrigin() throws IOException {
        QueryShardContext context = createShardContext();
        context.setAllowUnmappedFields(true);

        String mappedFieldName = randomFrom(INT_FIELD_NAME, DOUBLE_FIELD_NAME, GEOPOINT_FIELD_NAME);
        FunctionScoreQueryBuilder functionBuilder = newFunctionScoreQueryBuilder().add(
                randomDecayScoreFunctionBuilder(mappedFieldName).setOrigin(null));
        try {
            functionBuilder.toQuery(context);
            fail("should have failed!");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), Matchers.containsString("both [scale] and [origin] must be set"));
        }

        // a null origin is only allowed for Date fields (default to now)

        //NO COMMIT this fails because indexService is null in our parseContext test
        functionBuilder = newFunctionScoreQueryBuilder().add(
                randomDecayScoreFunctionBuilder(DATE_FIELD_NAME).setOrigin(null));
        assertThat(functionBuilder.toQuery(context), instanceOf(FunctionScoreQuery.class));
    }
}
