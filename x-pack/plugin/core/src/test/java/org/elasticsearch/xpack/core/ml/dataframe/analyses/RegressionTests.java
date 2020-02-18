/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.analyses;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class RegressionTests extends AbstractBWCSerializationTestCase<Regression> {

    private static final BoostedTreeParams BOOSTED_TREE_PARAMS = BoostedTreeParams.builder().build();

    @Override
    protected Regression doParseInstance(XContentParser parser) throws IOException {
        return Regression.fromXContent(parser, false);
    }

    @Override
    protected Regression createTestInstance() {
        return createRandom();
    }

    public static Regression createRandom() {
        String dependentVariableName = randomAlphaOfLength(10);
        BoostedTreeParams boostedTreeParams = BoostedTreeParamsTests.createRandom();
        String predictionFieldName = randomBoolean() ? null : randomAlphaOfLength(10);
        Double trainingPercent = randomBoolean() ? null : randomDoubleBetween(1.0, 100.0, true);
        Long randomizeSeed = randomBoolean() ? null : randomLong();
        return new Regression(dependentVariableName, boostedTreeParams, predictionFieldName, trainingPercent, randomizeSeed);
    }

    public static Regression mutateForVersion(Regression instance, Version version) {
        return new Regression(instance.getDependentVariable(),
            BoostedTreeParamsTests.mutateForVersion(instance.getBoostedTreeParams(), version),
            instance.getPredictionFieldName(),
            instance.getTrainingPercent(),
            instance.getRandomizeSeed());
    }

    @Override
    protected void assertOnBWCObject(Regression bwcSerializedObject, Regression testInstance, Version version) {
        if (version.onOrAfter(Version.V_7_6_0)) {
            super.assertOnBWCObject(bwcSerializedObject, testInstance, version);
            return;
        }

        Regression newBwc = new Regression(bwcSerializedObject.getDependentVariable(),
            bwcSerializedObject.getBoostedTreeParams(),
            bwcSerializedObject.getPredictionFieldName(),
            bwcSerializedObject.getTrainingPercent(),
            42L);
        Regression newInstance = new Regression(testInstance.getDependentVariable(),
            testInstance.getBoostedTreeParams(),
            testInstance.getPredictionFieldName(),
            testInstance.getTrainingPercent(),
            42L);
        super.assertOnBWCObject(newBwc, newInstance, version);
    }

    @Override
    protected Regression mutateInstanceForVersion(Regression instance, Version version) {
        return mutateForVersion(instance, version);
    }

    @Override
    protected Writeable.Reader<Regression> instanceReader() {
        return Regression::new;
    }

    public void testConstructor_GivenTrainingPercentIsLessThanOne() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new Regression("foo", BOOSTED_TREE_PARAMS, "result", 0.999, randomLong()));

        assertThat(e.getMessage(), equalTo("[training_percent] must be a double in [1, 100]"));
    }

    public void testConstructor_GivenTrainingPercentIsGreaterThan100() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new Regression("foo", BOOSTED_TREE_PARAMS, "result", 100.0001, randomLong()));

        assertThat(e.getMessage(), equalTo("[training_percent] must be a double in [1, 100]"));
    }

    public void testGetPredictionFieldName() {
        Regression regression = new Regression("foo", BOOSTED_TREE_PARAMS, "result", 50.0, randomLong());
        assertThat(regression.getPredictionFieldName(), equalTo("result"));

        regression = new Regression("foo", BOOSTED_TREE_PARAMS, null, 50.0, randomLong());
        assertThat(regression.getPredictionFieldName(), equalTo("foo_prediction"));
    }

    public void testGetTrainingPercent() {
        Regression regression = new Regression("foo", BOOSTED_TREE_PARAMS, "result", 50.0, randomLong());
        assertThat(regression.getTrainingPercent(), equalTo(50.0));

        // Boundary condition: training_percent == 1.0
        regression = new Regression("foo", BOOSTED_TREE_PARAMS, "result", 1.0, randomLong());
        assertThat(regression.getTrainingPercent(), equalTo(1.0));

        // Boundary condition: training_percent == 100.0
        regression = new Regression("foo", BOOSTED_TREE_PARAMS, "result", 100.0, randomLong());
        assertThat(regression.getTrainingPercent(), equalTo(100.0));

        // training_percent == null, default applied
        regression = new Regression("foo", BOOSTED_TREE_PARAMS, "result", null, randomLong());
        assertThat(regression.getTrainingPercent(), equalTo(100.0));
    }

    public void testGetParams() {
        assertThat(
            new Regression("foo").getParams(null),
            equalTo(Map.of("dependent_variable", "foo", "prediction_field_name", "foo_prediction")));
    }

    public void testRequiredFieldsIsNonEmpty() {
        assertThat(createTestInstance().getRequiredFields(), is(not(empty())));
    }

    public void testFieldCardinalityLimitsIsEmpty() {
        assertThat(createTestInstance().getFieldCardinalityConstraints(), is(empty()));
    }

    public void testGetExplicitlyMappedFields() {
        assertThat(
            new Regression("foo").getExplicitlyMappedFields(null, "results"),
            hasEntry("results.foo_prediction", Collections.singletonMap("type", "double")));
    }

    public void testGetStateDocId() {
        Regression regression = createRandom();
        assertThat(regression.persistsState(), is(true));
        String randomId = randomAlphaOfLength(10);
        assertThat(regression.getStateDocId(randomId), equalTo(randomId + "_regression_state#1"));
    }

    public void testExtractJobIdFromStateDoc() {
        assertThat(Regression.extractJobIdFromStateDoc("foo_bar-1_regression_state#1"), equalTo("foo_bar-1"));
        assertThat(Regression.extractJobIdFromStateDoc("noop"), is(nullValue()));
    }

    public void testToXContent_GivenVersionBeforeRandomizeSeedWasIntroduced() throws IOException {
        Regression regression = createRandom();
        assertThat(regression.getRandomizeSeed(), is(notNullValue()));

        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            regression.toXContent(builder, new ToXContent.MapParams(Collections.singletonMap("version", "7.5.0")));
            String json = Strings.toString(builder);
            assertThat(json, not(containsString("randomize_seed")));
        }
    }

    public void testToXContent_GivenVersionAfterRandomizeSeedWasIntroduced() throws IOException {
        Regression regression = createRandom();
        assertThat(regression.getRandomizeSeed(), is(notNullValue()));

        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            regression.toXContent(builder, new ToXContent.MapParams(Collections.singletonMap("version", Version.CURRENT.toString())));
            String json = Strings.toString(builder);
            assertThat(json, containsString("randomize_seed"));
        }
    }

    public void testToXContent_GivenVersionIsNull() throws IOException {
        Regression regression = createRandom();
        assertThat(regression.getRandomizeSeed(), is(notNullValue()));

        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            regression.toXContent(builder, new ToXContent.MapParams(Collections.singletonMap("version", null)));
            String json = Strings.toString(builder);
            assertThat(json, containsString("randomize_seed"));
        }
    }

    public void testToXContent_GivenEmptyParams() throws IOException {
        Regression regression = createRandom();
        assertThat(regression.getRandomizeSeed(), is(notNullValue()));

        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            regression.toXContent(builder, ToXContent.EMPTY_PARAMS);
            String json = Strings.toString(builder);
            assertThat(json, containsString("randomize_seed"));
        }
    }
}
