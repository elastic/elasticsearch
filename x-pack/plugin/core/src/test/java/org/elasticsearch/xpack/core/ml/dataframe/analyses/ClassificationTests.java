/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.analyses;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.BooleanFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.FrequencyEncodingTests;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.OneHotEncodingTests;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.PreProcessor;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.TargetMeanEncodingTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.PredictionFieldType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClassificationTests extends AbstractBWCSerializationTestCase<Classification> {

    private static final BoostedTreeParams BOOSTED_TREE_PARAMS = BoostedTreeParams.builder().build();

    @Override
    protected Classification doParseInstance(XContentParser parser) throws IOException {
        return Classification.fromXContent(parser, false);
    }

    @Override
    protected Classification createTestInstance() {
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

    public static Classification createRandom() {
        String dependentVariableName = randomAlphaOfLength(10);
        BoostedTreeParams boostedTreeParams = BoostedTreeParamsTests.createRandom();
        String predictionFieldName = randomBoolean() ? null : randomAlphaOfLength(10);
        Classification.ClassAssignmentObjective classAssignmentObjective = randomBoolean()
            ? null
            : randomFrom(Classification.ClassAssignmentObjective.values());
        Integer numTopClasses = randomBoolean() ? null : randomIntBetween(-1, 1000);
        Double trainingPercent = randomBoolean() ? null : randomDoubleBetween(0.0, 100.0, false);
        Long randomizeSeed = randomBoolean() ? null : randomLong();
        Boolean earlyStoppingEnabled = randomBoolean() ? null : randomBoolean();
        return new Classification(
            dependentVariableName,
            boostedTreeParams,
            predictionFieldName,
            classAssignmentObjective,
            numTopClasses,
            trainingPercent,
            randomizeSeed,
            randomBoolean()
                ? null
                : Stream.generate(
                    () -> randomFrom(
                        FrequencyEncodingTests.createRandom(true),
                        OneHotEncodingTests.createRandom(true),
                        TargetMeanEncodingTests.createRandom(true)
                    )
                ).limit(randomIntBetween(0, 5)).collect(Collectors.toList()),
            earlyStoppingEnabled
        );
    }

    public static Classification mutateForVersion(Classification instance, Version version) {
        return new Classification(
            instance.getDependentVariable(),
            BoostedTreeParamsTests.mutateForVersion(instance.getBoostedTreeParams(), version),
            instance.getPredictionFieldName(),
            instance.getClassAssignmentObjective(),
            instance.getNumTopClasses(),
            instance.getTrainingPercent(),
            instance.getRandomizeSeed(),
            instance.getFeatureProcessors(),
            instance.getEarlyStoppingEnabled()
        );
    }

    @Override
    protected Writeable.Reader<Classification> instanceReader() {
        return Classification::new;
    }

    public void testDeserialization() throws IOException {
        String toDeserialize = """
            {
                  "dependent_variable": "FlightDelayMin",
                  "feature_processors": [
                    {
                      "one_hot_encoding": {
                        "field": "OriginWeather",
                        "hot_map": {
                          "sunny_col": "Sunny",
                          "clear_col": "Clear",
                          "rainy_col": "Rain"
                        }
                      }
                    },
                    {
                      "one_hot_encoding": {
                        "field": "DestWeather",
                        "hot_map": {
                          "dest_sunny_col": "Sunny",
                          "dest_clear_col": "Clear",
                          "dest_rainy_col": "Rain"
                        }
                      }
                    },
                    {
                      "frequency_encoding": {
                        "field": "OriginWeather",
                        "feature_name": "mean",
                        "frequency_map": {
                          "Sunny": 0.8,
                          "Rain": 0.2
                        }
                      }
                    }
                  ]
                }""";

        try (
            XContentParser parser = XContentHelper.createParser(
                xContentRegistry(),
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                new BytesArray(toDeserialize),
                XContentType.JSON
            )
        ) {
            Classification parsed = Classification.fromXContent(parser, false);
            assertThat(parsed.getDependentVariable(), equalTo("FlightDelayMin"));
            for (PreProcessor preProcessor : parsed.getFeatureProcessors()) {
                assertThat(preProcessor.isCustom(), is(true));
            }
        }
    }

    public void testConstructor_GivenTrainingPercentIsZero() {
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> new Classification("foo", BOOSTED_TREE_PARAMS, "result", null, 3, 0.0, randomLong(), null, null)
        );

        assertThat(e.getMessage(), equalTo("[training_percent] must be a positive double in (0, 100]"));
    }

    public void testConstructor_GivenTrainingPercentIsLessThanZero() {
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> new Classification("foo", BOOSTED_TREE_PARAMS, "result", null, 3, -1.0, randomLong(), null, null)
        );

        assertThat(e.getMessage(), equalTo("[training_percent] must be a positive double in (0, 100]"));
    }

    public void testConstructor_GivenTrainingPercentIsGreaterThan100() {
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> new Classification("foo", BOOSTED_TREE_PARAMS, "result", null, 3, 100.0001, randomLong(), null, null)
        );

        assertThat(e.getMessage(), equalTo("[training_percent] must be a positive double in (0, 100]"));
    }

    public void testConstructor_GivenNumTopClassesIsLessThanMinusOne() {
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> new Classification("foo", BOOSTED_TREE_PARAMS, "result", null, -2, 1.0, randomLong(), null, null)
        );

        assertThat(e.getMessage(), equalTo("[num_top_classes] must be an integer in [0, 1000] or a special value -1"));
    }

    public void testConstructor_GivenNumTopClassesIsGreaterThan1000() {
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> new Classification("foo", BOOSTED_TREE_PARAMS, "result", null, 1001, 1.0, randomLong(), null, null)
        );

        assertThat(e.getMessage(), equalTo("[num_top_classes] must be an integer in [0, 1000] or a special value -1"));
    }

    public void testGetPredictionFieldName() {
        Classification classification = new Classification("foo", BOOSTED_TREE_PARAMS, "result", null, 3, 50.0, randomLong(), null, null);
        assertThat(classification.getPredictionFieldName(), equalTo("result"));

        classification = new Classification("foo", BOOSTED_TREE_PARAMS, null, null, 3, 50.0, randomLong(), null, null);
        assertThat(classification.getPredictionFieldName(), equalTo("foo_prediction"));
    }

    public void testClassAssignmentObjective() {
        Classification classification = new Classification(
            "foo",
            BOOSTED_TREE_PARAMS,
            "result",
            Classification.ClassAssignmentObjective.MAXIMIZE_ACCURACY,
            7,
            1.0,
            randomLong(),
            null,
            null
        );
        assertThat(classification.getClassAssignmentObjective(), equalTo(Classification.ClassAssignmentObjective.MAXIMIZE_ACCURACY));

        classification = new Classification(
            "foo",
            BOOSTED_TREE_PARAMS,
            "result",
            Classification.ClassAssignmentObjective.MAXIMIZE_MINIMUM_RECALL,
            7,
            1.0,
            randomLong(),
            null,
            null
        );
        assertThat(classification.getClassAssignmentObjective(), equalTo(Classification.ClassAssignmentObjective.MAXIMIZE_MINIMUM_RECALL));

        // class_assignment_objective == null, default applied
        classification = new Classification("foo", BOOSTED_TREE_PARAMS, "result", null, 7, 1.0, randomLong(), null, null);
        assertThat(classification.getClassAssignmentObjective(), equalTo(Classification.ClassAssignmentObjective.MAXIMIZE_MINIMUM_RECALL));
    }

    public void testGetNumTopClasses() {
        Classification classification = new Classification("foo", BOOSTED_TREE_PARAMS, "result", null, 7, 1.0, randomLong(), null, null);
        assertThat(classification.getNumTopClasses(), equalTo(7));

        // Special value: num_top_classes == -1
        classification = new Classification("foo", BOOSTED_TREE_PARAMS, "result", null, -1, 1.0, randomLong(), null, null);
        assertThat(classification.getNumTopClasses(), equalTo(-1));

        // Boundary condition: num_top_classes == 0
        classification = new Classification("foo", BOOSTED_TREE_PARAMS, "result", null, 0, 1.0, randomLong(), null, null);
        assertThat(classification.getNumTopClasses(), equalTo(0));

        // Boundary condition: num_top_classes == 1000
        classification = new Classification("foo", BOOSTED_TREE_PARAMS, "result", null, 1000, 1.0, randomLong(), null, null);
        assertThat(classification.getNumTopClasses(), equalTo(1000));

        // num_top_classes == null, default applied
        classification = new Classification("foo", BOOSTED_TREE_PARAMS, "result", null, null, 1.0, randomLong(), null, null);
        assertThat(classification.getNumTopClasses(), equalTo(2));
    }

    public void testGetTrainingPercent() {
        Classification classification = new Classification("foo", BOOSTED_TREE_PARAMS, "result", null, 3, 50.0, randomLong(), null, null);
        assertThat(classification.getTrainingPercent(), equalTo(50.0));

        // Boundary condition: training_percent == 1.0
        classification = new Classification("foo", BOOSTED_TREE_PARAMS, "result", null, 3, 1.0, randomLong(), null, null);
        assertThat(classification.getTrainingPercent(), equalTo(1.0));

        // Boundary condition: training_percent == 100.0
        classification = new Classification("foo", BOOSTED_TREE_PARAMS, "result", null, 3, 100.0, randomLong(), null, null);
        assertThat(classification.getTrainingPercent(), equalTo(100.0));

        // training_percent == null, default applied
        classification = new Classification("foo", BOOSTED_TREE_PARAMS, "result", null, 3, null, randomLong(), null, null);
        assertThat(classification.getTrainingPercent(), equalTo(100.0));
    }

    public void testGetParams() {
        DataFrameAnalysis.FieldInfo fieldInfo = new TestFieldInfo(
            Map.of(
                "foo",
                Set.of(BooleanFieldMapper.CONTENT_TYPE),
                "bar",
                Set.of(NumberFieldMapper.NumberType.LONG.typeName()),
                "baz",
                Set.of(KeywordFieldMapper.CONTENT_TYPE)
            ),
            Map.of("foo", 10L, "bar", 20L, "baz", 30L)
        );
        assertThat(
            new Classification("foo").getParams(fieldInfo),
            equalTo(
                Map.of(
                    "dependent_variable",
                    "foo",
                    "class_assignment_objective",
                    Classification.ClassAssignmentObjective.MAXIMIZE_MINIMUM_RECALL,
                    "num_top_classes",
                    2,
                    "prediction_field_name",
                    "foo_prediction",
                    "prediction_field_type",
                    "bool",
                    "num_classes",
                    10L,
                    "training_percent",
                    100.0,
                    "early_stopping_enabled",
                    true
                )
            )
        );
        assertThat(
            new Classification("bar").getParams(fieldInfo),
            equalTo(
                Map.of(
                    "dependent_variable",
                    "bar",
                    "class_assignment_objective",
                    Classification.ClassAssignmentObjective.MAXIMIZE_MINIMUM_RECALL,
                    "num_top_classes",
                    2,
                    "prediction_field_name",
                    "bar_prediction",
                    "prediction_field_type",
                    "int",
                    "num_classes",
                    20L,
                    "training_percent",
                    100.0,
                    "early_stopping_enabled",
                    true
                )
            )
        );
        assertThat(
            new Classification("baz", BoostedTreeParams.builder().build(), null, null, null, 50.0, null, null, null).getParams(fieldInfo),
            equalTo(
                Map.of(
                    "dependent_variable",
                    "baz",
                    "class_assignment_objective",
                    Classification.ClassAssignmentObjective.MAXIMIZE_MINIMUM_RECALL,
                    "num_top_classes",
                    2,
                    "prediction_field_name",
                    "baz_prediction",
                    "prediction_field_type",
                    "string",
                    "num_classes",
                    30L,
                    "training_percent",
                    50.0,
                    "early_stopping_enabled",
                    true
                )
            )
        );
    }

    public void testRequiredFieldsIsNonEmpty() {
        assertThat(createTestInstance().getRequiredFields(), is(not(empty())));
    }

    public void testFieldCardinalityLimitsIsNonEmpty() {
        Classification classification = createTestInstance();
        List<FieldCardinalityConstraint> constraints = classification.getFieldCardinalityConstraints();

        assertThat(constraints.size(), equalTo(1));
        assertThat(constraints.get(0).getField(), equalTo(classification.getDependentVariable()));
        assertThat(constraints.get(0).getLowerBound(), equalTo(2L));
        assertThat(constraints.get(0).getUpperBound(), equalTo(30L));
    }

    public void testGetResultMappings_DependentVariableMappingIsAbsent() {
        FieldCapabilitiesResponse fieldCapabilitiesResponse = new FieldCapabilitiesResponse(new String[0], Collections.emptyMap());
        expectThrows(
            ElasticsearchStatusException.class,
            () -> new Classification("foo").getResultMappings("results", fieldCapabilitiesResponse)
        );
    }

    public void testGetResultMappings_DependentVariableMappingHasNoTypes() {
        FieldCapabilitiesResponse fieldCapabilitiesResponse = new FieldCapabilitiesResponse(
            new String[0],
            Collections.singletonMap("foo", Collections.emptyMap())
        );
        expectThrows(
            ElasticsearchStatusException.class,
            () -> new Classification("foo").getResultMappings("results", fieldCapabilitiesResponse)
        );
    }

    public void testGetResultMappings_DependentVariableMappingIsPresent() {
        Map<String, Object> expectedTopClassesMapping = new HashMap<>() {
            {
                put("type", "nested");
                put("properties", new HashMap<>() {
                    {
                        put("class_name", singletonMap("type", "dummy"));
                        put("class_probability", singletonMap("type", "double"));
                        put("class_score", singletonMap("type", "double"));
                    }
                });
            }
        };
        FieldCapabilitiesResponse fieldCapabilitiesResponse = new FieldCapabilitiesResponse(
            new String[0],
            Collections.singletonMap("foo", Collections.singletonMap("dummy", createFieldCapabilities("foo", "dummy")))
        );

        Map<String, Object> resultMappings = new Classification("foo").getResultMappings("results", fieldCapabilitiesResponse);

        assertThat(resultMappings, hasEntry("results.foo_prediction", singletonMap("type", "dummy")));
        assertThat(resultMappings, hasEntry("results.prediction_probability", singletonMap("type", "double")));
        assertThat(resultMappings, hasEntry("results.prediction_score", singletonMap("type", "double")));
        assertThat(resultMappings, hasEntry("results.is_training", singletonMap("type", "boolean")));
        assertThat(resultMappings, hasEntry("results.top_classes", expectedTopClassesMapping));
        assertThat(resultMappings, hasEntry("results.feature_importance", Classification.FEATURE_IMPORTANCE_MAPPING));
    }

    public void testToXContent_GivenVersionBeforeRandomizeSeedWasIntroduced() throws IOException {
        Classification classification = createRandom();
        assertThat(classification.getRandomizeSeed(), is(notNullValue()));

        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            classification.toXContent(builder, new ToXContent.MapParams(singletonMap("version", "7.5.0")));
            String json = Strings.toString(builder);
            assertThat(json, not(containsString("randomize_seed")));
        }
    }

    public void testToXContent_GivenVersionAfterRandomizeSeedWasIntroduced() throws IOException {
        Classification classification = createRandom();
        assertThat(classification.getRandomizeSeed(), is(notNullValue()));

        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            classification.toXContent(builder, new ToXContent.MapParams(singletonMap("version", Version.CURRENT.toString())));
            String json = Strings.toString(builder);
            assertThat(json, containsString("randomize_seed"));
        }
    }

    public void testToXContent_GivenVersionIsNull() throws IOException {
        Classification classification = createRandom();
        assertThat(classification.getRandomizeSeed(), is(notNullValue()));

        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            classification.toXContent(builder, new ToXContent.MapParams(singletonMap("version", null)));
            String json = Strings.toString(builder);
            assertThat(json, containsString("randomize_seed"));
        }
    }

    public void testToXContent_GivenEmptyParams() throws IOException {
        Classification classification = createRandom();
        assertThat(classification.getRandomizeSeed(), is(notNullValue()));

        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            classification.toXContent(builder, ToXContent.EMPTY_PARAMS);
            String json = Strings.toString(builder);
            assertThat(json, containsString("randomize_seed"));
        }
    }

    public void testGetStateDocId() {
        Classification classification = createRandom();
        assertThat(classification.persistsState(), is(true));
        String randomId = randomAlphaOfLength(10);
        assertThat(classification.getStateDocIdPrefix(randomId), equalTo(randomId + "_classification_state#"));
    }

    public void testExtractJobIdFromStateDoc() {
        assertThat(Classification.extractJobIdFromStateDoc("foo_bar-1_classification_state#1"), equalTo("foo_bar-1"));
        assertThat(Classification.extractJobIdFromStateDoc("noop"), is(nullValue()));
    }

    public void testGetPredictionFieldType() {
        assertThat(Classification.getPredictionFieldType(Collections.emptySet()), equalTo(PredictionFieldType.STRING));
        assertThat(Classification.getPredictionFieldType(Collections.singleton("keyword")), equalTo(PredictionFieldType.STRING));
        assertThat(Classification.getPredictionFieldType(Collections.singleton("long")), equalTo(PredictionFieldType.NUMBER));
        assertThat(Classification.getPredictionFieldType(Collections.singleton("boolean")), equalTo(PredictionFieldType.BOOLEAN));
    }

    public void testInferenceConfig() {
        Classification classification = createRandom();
        DataFrameAnalysis.FieldInfo fieldInfo = mock(DataFrameAnalysis.FieldInfo.class);
        when(fieldInfo.getTypes(classification.getDependentVariable())).thenReturn(Collections.singleton("keyword"));

        InferenceConfig inferenceConfig = classification.inferenceConfig(fieldInfo);

        assertThat(inferenceConfig, instanceOf(ClassificationConfig.class));

        ClassificationConfig classificationConfig = (ClassificationConfig) inferenceConfig;
        assertThat(classificationConfig.getResultsField(), equalTo(classification.getPredictionFieldName()));
        assertThat(classificationConfig.getNumTopClasses(), equalTo(classification.getNumTopClasses()));
        Integer expectedNumTopFeatureImportanceValues = classification.getBoostedTreeParams().getNumTopFeatureImportanceValues() == null
            ? 0
            : classification.getBoostedTreeParams().getNumTopFeatureImportanceValues();
        assertThat(classificationConfig.getNumTopFeatureImportanceValues(), equalTo(expectedNumTopFeatureImportanceValues));
        assertThat(classificationConfig.getPredictionFieldType(), equalTo(PredictionFieldType.STRING));
    }

    @Override
    protected Classification mutateInstanceForVersion(Classification instance, Version version) {
        return mutateForVersion(instance, version);
    }

    private static class TestFieldInfo implements DataFrameAnalysis.FieldInfo {

        private final Map<String, Set<String>> fieldTypes;
        private final Map<String, Long> fieldCardinalities;

        private TestFieldInfo(Map<String, Set<String>> fieldTypes, Map<String, Long> fieldCardinalities) {
            this.fieldTypes = fieldTypes;
            this.fieldCardinalities = fieldCardinalities;
        }

        @Override
        public Set<String> getTypes(String field) {
            return fieldTypes.get(field);
        }

        @Override
        public Long getCardinality(String field) {
            return fieldCardinalities.get(field);
        }
    }

    private static FieldCapabilities createFieldCapabilities(String field, String type) {
        return new FieldCapabilities(field, type, false, true, true, null, null, null, Collections.emptyMap());
    }
}
