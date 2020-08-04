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
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.instanceOf;
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
        return createRandom(BoostedTreeParamsTests.createRandom());
    }

    private static Regression createRandom(BoostedTreeParams boostedTreeParams) {
        String dependentVariableName = randomAlphaOfLength(10);
        String predictionFieldName = randomBoolean() ? null : randomAlphaOfLength(10);
        Double trainingPercent = randomBoolean() ? null : randomDoubleBetween(1.0, 100.0, true);
        Long randomizeSeed = randomBoolean() ? null : randomLong();
        Regression.LossFunction lossFunction = randomBoolean() ? null : randomFrom(Regression.LossFunction.values());
        Double lossFunctionParameter = randomBoolean() ? null : randomDoubleBetween(0.0, Double.MAX_VALUE, false);
        return new Regression(dependentVariableName, boostedTreeParams, predictionFieldName, trainingPercent, randomizeSeed, lossFunction,
            lossFunctionParameter);
    }

    public static Regression mutateForVersion(Regression instance, Version version) {
        return new Regression(instance.getDependentVariable(),
            BoostedTreeParamsTests.mutateForVersion(instance.getBoostedTreeParams(), version),
            instance.getPredictionFieldName(),
            instance.getTrainingPercent(),
            instance.getRandomizeSeed(),
            instance.getLossFunction(),
            instance.getLossFunctionParameter());
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
            42L,
            bwcSerializedObject.getLossFunction(),
            bwcSerializedObject.getLossFunctionParameter());
        Regression newInstance = new Regression(testInstance.getDependentVariable(),
            testInstance.getBoostedTreeParams(),
            testInstance.getPredictionFieldName(),
            testInstance.getTrainingPercent(),
            42L,
            testInstance.getLossFunction(),
            testInstance.getLossFunctionParameter());
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
            () -> new Regression("foo", BOOSTED_TREE_PARAMS, "result", 0.999, randomLong(), Regression.LossFunction.MSE, null));

        assertThat(e.getMessage(), equalTo("[training_percent] must be a double in [1, 100]"));
    }

    public void testConstructor_GivenTrainingPercentIsGreaterThan100() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new Regression("foo", BOOSTED_TREE_PARAMS, "result", 100.0001, randomLong(), Regression.LossFunction.MSE, null));

        assertThat(e.getMessage(), equalTo("[training_percent] must be a double in [1, 100]"));
    }

    public void testConstructor_GivenLossFunctionParameterIsZero() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new Regression("foo", BOOSTED_TREE_PARAMS, "result", 100.0, randomLong(), Regression.LossFunction.MSE, 0.0));

        assertThat(e.getMessage(), equalTo("[loss_function_parameter] must be a positive double"));
    }

    public void testConstructor_GivenLossFunctionParameterIsNegative() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new Regression("foo", BOOSTED_TREE_PARAMS, "result", 100.0, randomLong(), Regression.LossFunction.MSE, -1.0));

        assertThat(e.getMessage(), equalTo("[loss_function_parameter] must be a positive double"));
    }

    public void testGetPredictionFieldName() {
        Regression regression = new Regression("foo", BOOSTED_TREE_PARAMS, "result", 50.0, randomLong(), Regression.LossFunction.MSE, 1.0);
        assertThat(regression.getPredictionFieldName(), equalTo("result"));

        regression = new Regression("foo", BOOSTED_TREE_PARAMS, null, 50.0, randomLong(), Regression.LossFunction.MSE, null);
        assertThat(regression.getPredictionFieldName(), equalTo("foo_prediction"));
    }

    public void testGetTrainingPercent() {
        Regression regression = new Regression("foo", BOOSTED_TREE_PARAMS, "result", 50.0, randomLong(), Regression.LossFunction.MSE, 1.0);
        assertThat(regression.getTrainingPercent(), equalTo(50.0));

        // Boundary condition: training_percent == 1.0
        regression = new Regression("foo", BOOSTED_TREE_PARAMS, "result", 1.0, randomLong(), Regression.LossFunction.MSE, null);
        assertThat(regression.getTrainingPercent(), equalTo(1.0));

        // Boundary condition: training_percent == 100.0
        regression = new Regression("foo", BOOSTED_TREE_PARAMS, "result", 100.0, randomLong(), Regression.LossFunction.MSE, null);
        assertThat(regression.getTrainingPercent(), equalTo(100.0));

        // training_percent == null, default applied
        regression = new Regression("foo", BOOSTED_TREE_PARAMS, "result", null, randomLong(), Regression.LossFunction.MSE, null);
        assertThat(regression.getTrainingPercent(), equalTo(100.0));
    }

    public void testGetParams_ShouldIncludeBoostedTreeParams() {
        int maxTrees = randomIntBetween(1, 100);
        Regression regression = new Regression("foo",
            BoostedTreeParams.builder().setMaxTrees(maxTrees).build(),
            null,
            100.0,
            0L,
            Regression.LossFunction.MSE,
            null);

        Map<String, Object> params = regression.getParams(null);

        assertThat(params.size(), equalTo(5));
        assertThat(params.get("dependent_variable"), equalTo("foo"));
        assertThat(params.get("prediction_field_name"), equalTo("foo_prediction"));
        assertThat(params.get("max_trees"), equalTo(maxTrees));
        assertThat(params.get("training_percent"), equalTo(100.0));
        assertThat(params.get("loss_function"), equalTo("mse"));
    }

    public void testGetParams_GivenRandomWithoutBoostedTreeParams() {
        Regression regression = createRandom(BoostedTreeParams.builder().build());

        Map<String, Object> params = regression.getParams(null);

        int expectedParamsCount = 4 + (regression.getLossFunctionParameter() == null ? 0 : 1);
        assertThat(params.size(), equalTo(expectedParamsCount));
        assertThat(params.get("dependent_variable"), equalTo(regression.getDependentVariable()));
        assertThat(params.get("prediction_field_name"), equalTo(regression.getPredictionFieldName()));
        assertThat(params.get("training_percent"), equalTo(regression.getTrainingPercent()));
        assertThat(params.get("loss_function"), equalTo(regression.getLossFunction().toString()));
        if (regression.getLossFunctionParameter() == null) {
            assertThat(params.containsKey("loss_function_parameter"), is(false));
        } else {
            assertThat(params.get("loss_function_parameter"), equalTo(regression.getLossFunctionParameter()));
        }
    }

    public void testRequiredFieldsIsNonEmpty() {
        assertThat(createTestInstance().getRequiredFields(), is(not(empty())));
    }

    public void testFieldCardinalityLimitsIsEmpty() {
        assertThat(createTestInstance().getFieldCardinalityConstraints(), is(empty()));
    }

    public void testGetExplicitlyMappedFields() {
        Map<String, Object> explicitlyMappedFields = new Regression("foo").getExplicitlyMappedFields(null, "results");
        assertThat(explicitlyMappedFields, hasEntry("results.foo_prediction", Collections.singletonMap("type", "double")));
        assertThat(explicitlyMappedFields, hasEntry("results.feature_importance", MapUtils.featureImportanceMapping()));
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

    public void testInferenceConfig() {
        Regression regression = createRandom();

        InferenceConfig inferenceConfig = regression.inferenceConfig(null);

        assertThat(inferenceConfig, instanceOf(RegressionConfig.class));

        RegressionConfig regressionConfig = (RegressionConfig) inferenceConfig;

        assertThat(regressionConfig.getResultsField(), equalTo(regression.getPredictionFieldName()));
        Integer expectedNumTopFeatureImportanceValues = regression.getBoostedTreeParams().getNumTopFeatureImportanceValues() == null ?
            0 : regression.getBoostedTreeParams().getNumTopFeatureImportanceValues();
        assertThat(regressionConfig.getNumTopFeatureImportanceValues(), equalTo(expectedNumTopFeatureImportanceValues));
    }
}
