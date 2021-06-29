/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.analyses;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.FrequencyEncodingTests;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.OneHotEncodingTests;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.PreProcessor;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.TargetMeanEncodingTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
        return new NamedXContentRegistry(namedXContent);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(new MlInferenceNamedXContentProvider().getNamedWriteables());
        return new NamedWriteableRegistry(entries);
    }

    public static Regression createRandom() {
        return createRandom(BoostedTreeParamsTests.createRandom());
    }

    private static Regression createRandom(BoostedTreeParams boostedTreeParams) {
        String dependentVariableName = randomAlphaOfLength(10);
        String predictionFieldName = randomBoolean() ? null : randomAlphaOfLength(10);
        Double trainingPercent = randomBoolean() ? null : randomDoubleBetween(0.0, 100.0, false);
        Long randomizeSeed = randomBoolean() ? null : randomLong();
        Regression.LossFunction lossFunction = randomBoolean() ? null : randomFrom(Regression.LossFunction.values());
        Double lossFunctionParameter = randomBoolean() ? null : randomDoubleBetween(0.0, Double.MAX_VALUE, false);
        Boolean earlyStoppingEnabled = randomBoolean() ? null : randomBoolean();
        return new Regression(dependentVariableName, boostedTreeParams, predictionFieldName, trainingPercent, randomizeSeed, lossFunction,
            lossFunctionParameter,
            randomBoolean() ?
                null :
                Stream.generate(() -> randomFrom(FrequencyEncodingTests.createRandom(true),
                    OneHotEncodingTests.createRandom(true),
                    TargetMeanEncodingTests.createRandom(true)))
                    .limit(randomIntBetween(0, 5))
                    .collect(Collectors.toList()),
            earlyStoppingEnabled);
    }

    public static Regression mutateForVersion(Regression instance, Version version) {
        return new Regression(instance.getDependentVariable(),
            BoostedTreeParamsTests.mutateForVersion(instance.getBoostedTreeParams(), version),
            instance.getPredictionFieldName(),
            instance.getTrainingPercent(),
            instance.getRandomizeSeed(),
            instance.getLossFunction(),
            instance.getLossFunctionParameter(),
            version.onOrAfter(Version.V_7_10_0) ? instance.getFeatureProcessors() : Collections.emptyList(),
            instance.getEarlyStoppingEnabled());
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
            bwcSerializedObject.getLossFunctionParameter(),
            bwcSerializedObject.getFeatureProcessors(),
            bwcSerializedObject.getEarlyStoppingEnabled());
        Regression newInstance = new Regression(testInstance.getDependentVariable(),
            testInstance.getBoostedTreeParams(),
            testInstance.getPredictionFieldName(),
            testInstance.getTrainingPercent(),
            42L,
            testInstance.getLossFunction(),
            testInstance.getLossFunctionParameter(),
            testInstance.getFeatureProcessors(),
            testInstance.getEarlyStoppingEnabled());
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

    public void testDeserialization() throws IOException {
        String toDeserialize = "{\n" +
            "      \"dependent_variable\": \"FlightDelayMin\",\n" +
            "      \"feature_processors\": [\n" +
            "        {\n" +
            "          \"one_hot_encoding\": {\n" +
            "            \"field\": \"OriginWeather\",\n" +
            "            \"hot_map\": {\n" +
            "              \"sunny_col\": \"Sunny\",\n" +
            "              \"clear_col\": \"Clear\",\n" +
            "              \"rainy_col\": \"Rain\"\n" +
            "            }\n" +
            "          }\n" +
            "        },\n" +
            "        {\n" +
            "          \"one_hot_encoding\": {\n" +
            "            \"field\": \"DestWeather\",\n" +
            "            \"hot_map\": {\n" +
            "              \"dest_sunny_col\": \"Sunny\",\n" +
            "              \"dest_clear_col\": \"Clear\",\n" +
            "              \"dest_rainy_col\": \"Rain\"\n" +
            "            }\n" +
            "          }\n" +
            "        },\n" +
            "        {\n" +
            "          \"frequency_encoding\": {\n" +
            "            \"field\": \"OriginWeather\",\n" +
            "            \"feature_name\": \"mean\",\n" +
            "            \"frequency_map\": {\n" +
            "              \"Sunny\": 0.8,\n" +
            "              \"Rain\": 0.2\n" +
            "            }\n" +
            "          }\n" +
            "        }\n" +
            "      ]\n" +
            "    }" +
            "";

        try(XContentParser parser = XContentHelper.createParser(xContentRegistry(),
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            new BytesArray(toDeserialize),
            XContentType.JSON)) {
            Regression parsed = Regression.fromXContent(parser, false);
            assertThat(parsed.getDependentVariable(), equalTo("FlightDelayMin"));
            for (PreProcessor preProcessor : parsed.getFeatureProcessors()) {
                assertThat(preProcessor.isCustom(), is(true));
            }
        }
    }

    public void testConstructor_GivenTrainingPercentIsZero() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new Regression("foo", BOOSTED_TREE_PARAMS, "result", 0.0, randomLong(),
                                Regression.LossFunction.MSE, null, null, null));

        assertThat(e.getMessage(), equalTo("[training_percent] must be a positive double in (0, 100]"));
    }

    public void testConstructor_GivenTrainingPercentIsLessThanZero() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new Regression("foo", BOOSTED_TREE_PARAMS, "result", -0.01, randomLong(),
                                Regression.LossFunction.MSE, null, null, null));

        assertThat(e.getMessage(), equalTo("[training_percent] must be a positive double in (0, 100]"));
    }

    public void testConstructor_GivenTrainingPercentIsGreaterThan100() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new Regression("foo", BOOSTED_TREE_PARAMS, "result", 100.0001, randomLong(),
                                Regression.LossFunction.MSE, null, null, null));


        assertThat(e.getMessage(), equalTo("[training_percent] must be a positive double in (0, 100]"));
    }

    public void testConstructor_GivenLossFunctionParameterIsZero() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new Regression("foo", BOOSTED_TREE_PARAMS, "result", 100.0, randomLong(),
                                Regression.LossFunction.MSE, 0.0, null, null));

        assertThat(e.getMessage(), equalTo("[loss_function_parameter] must be a positive double"));
    }

    public void testConstructor_GivenLossFunctionParameterIsNegative() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new Regression("foo", BOOSTED_TREE_PARAMS, "result", 100.0, randomLong(),
                                Regression.LossFunction.MSE, -1.0, null, null));

        assertThat(e.getMessage(), equalTo("[loss_function_parameter] must be a positive double"));
    }

    public void testGetPredictionFieldName() {
        Regression regression = new Regression("foo", BOOSTED_TREE_PARAMS, "result", 50.0, randomLong(),
                                                Regression.LossFunction.MSE, 1.0, null, null);
        assertThat(regression.getPredictionFieldName(), equalTo("result"));

        regression = new Regression("foo", BOOSTED_TREE_PARAMS, null, 50.0, randomLong(),
                                    Regression.LossFunction.MSE, null, null, null);
        assertThat(regression.getPredictionFieldName(), equalTo("foo_prediction"));
    }

    public void testGetTrainingPercent() {
        Regression regression = new Regression("foo", BOOSTED_TREE_PARAMS, "result", 50.0, randomLong(),
                                                Regression.LossFunction.MSE, 1.0, null, null);
        assertThat(regression.getTrainingPercent(), equalTo(50.0));

        // Boundary condition: training_percent == 1.0
        regression = new Regression("foo", BOOSTED_TREE_PARAMS, "result", 1.0, randomLong(),
                                    Regression.LossFunction.MSE, null, null, null);
        assertThat(regression.getTrainingPercent(), equalTo(1.0));

        // Boundary condition: training_percent == 100.0
        regression = new Regression("foo", BOOSTED_TREE_PARAMS, "result", 100.0, randomLong(),
                                    Regression.LossFunction.MSE, null, null, null);
        assertThat(regression.getTrainingPercent(), equalTo(100.0));

        // training_percent == null, default applied
        regression = new Regression("foo", BOOSTED_TREE_PARAMS, "result", null, randomLong(),
                                    Regression.LossFunction.MSE, null, null, null);
        assertThat(regression.getTrainingPercent(), equalTo(100.0));
    }

    public void testGetParams_ShouldIncludeBoostedTreeParams() {
        int maxTrees = randomIntBetween(1, 100);
        Regression regression = new Regression("foo",
            BoostedTreeParams.builder().setMaxTrees(maxTrees).build(),
            null, 100.0, 0L, Regression.LossFunction.MSE, null, null, null);

        Map<String, Object> params = regression.getParams(null);

        assertThat(params.size(), equalTo(6));
        assertThat(params.get("dependent_variable"), equalTo("foo"));
        assertThat(params.get("prediction_field_name"), equalTo("foo_prediction"));
        assertThat(params.get("max_trees"), equalTo(maxTrees));
        assertThat(params.get("training_percent"), equalTo(100.0));
        assertThat(params.get("loss_function"), equalTo("mse"));
        assertThat(params.get("early_stopping_enabled"), equalTo(true));
    }

    public void testGetParams_GivenRandomWithoutBoostedTreeParams() {
        Regression regression = createRandom(BoostedTreeParams.builder().build());

        Map<String, Object> params = regression.getParams(null);

        int expectedParamsCount = 5
            + (regression.getLossFunctionParameter() == null ? 0 : 1)
            + (regression.getFeatureProcessors().isEmpty() ? 0 : 1);
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
        assertThat(params.get("early_stopping_enabled"), equalTo(regression.getEarlyStoppingEnabled()));
    }

    public void testRequiredFieldsIsNonEmpty() {
        assertThat(createTestInstance().getRequiredFields(), is(not(empty())));
    }

    public void testFieldCardinalityLimitsIsEmpty() {
        assertThat(createTestInstance().getFieldCardinalityConstraints(), is(empty()));
    }

    public void testGetResultMappings() {
        Map<String, Object> resultMappings = new Regression("foo").getResultMappings("results", null);
        assertThat(resultMappings, hasEntry("results.foo_prediction", Collections.singletonMap("type", "double")));
        assertThat(resultMappings, hasEntry("results.feature_importance", Regression.FEATURE_IMPORTANCE_MAPPING));
        assertThat(resultMappings, hasEntry("results.is_training", Collections.singletonMap("type", "boolean")));
    }

    public void testGetStateDocId() {
        Regression regression = createRandom();
        assertThat(regression.persistsState(), is(true));
        String randomId = randomAlphaOfLength(10);
        assertThat(regression.getStateDocIdPrefix(randomId), equalTo(randomId + "_regression_state#"));
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
