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
import org.elasticsearch.index.mapper.BooleanFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

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

    public static Classification createRandom() {
        String dependentVariableName = randomAlphaOfLength(10);
        BoostedTreeParams boostedTreeParams = BoostedTreeParamsTests.createRandom();
        String predictionFieldName = randomBoolean() ? null : randomAlphaOfLength(10);
        Integer numTopClasses = randomBoolean() ? null : randomIntBetween(0, 1000);
        Double trainingPercent = randomBoolean() ? null : randomDoubleBetween(1.0, 100.0, true);
        Long randomizeSeed = randomBoolean() ? null : randomLong();
        return new Classification(dependentVariableName, boostedTreeParams, predictionFieldName, numTopClasses, trainingPercent,
            randomizeSeed);
    }

    public static Classification mutateForVersion(Classification instance, Version version) {
        return new Classification(instance.getDependentVariable(),
            BoostedTreeParamsTests.mutateForVersion(instance.getBoostedTreeParams(), version),
            instance.getPredictionFieldName(),
            instance.getNumTopClasses(),
            instance.getTrainingPercent(),
            instance.getRandomizeSeed());
    }

    @Override
    protected void assertOnBWCObject(Classification bwcSerializedObject, Classification testInstance, Version version) {
        if (version.onOrAfter(Version.V_7_6_0)) {
            super.assertOnBWCObject(bwcSerializedObject, testInstance, version);
            return;
        }

        Classification newBwc = new Classification(bwcSerializedObject.getDependentVariable(),
            bwcSerializedObject.getBoostedTreeParams(),
            bwcSerializedObject.getPredictionFieldName(),
            bwcSerializedObject.getNumTopClasses(),
            bwcSerializedObject.getTrainingPercent(),
            42L);
        Classification newInstance = new Classification(testInstance.getDependentVariable(),
            testInstance.getBoostedTreeParams(),
            testInstance.getPredictionFieldName(),
            testInstance.getNumTopClasses(),
            testInstance.getTrainingPercent(),
            42L);
        super.assertOnBWCObject(newBwc, newInstance, version);
    }

    @Override
    protected Writeable.Reader<Classification> instanceReader() {
        return Classification::new;
    }

    public void testConstructor_GivenTrainingPercentIsLessThanOne() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new Classification("foo", BOOSTED_TREE_PARAMS, "result", 3, 0.999, randomLong()));

        assertThat(e.getMessage(), equalTo("[training_percent] must be a double in [1, 100]"));
    }

    public void testConstructor_GivenTrainingPercentIsGreaterThan100() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new Classification("foo", BOOSTED_TREE_PARAMS, "result", 3, 100.0001, randomLong()));

        assertThat(e.getMessage(), equalTo("[training_percent] must be a double in [1, 100]"));
    }

    public void testConstructor_GivenNumTopClassesIsLessThanZero() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new Classification("foo", BOOSTED_TREE_PARAMS, "result", -1, 1.0, randomLong()));

        assertThat(e.getMessage(), equalTo("[num_top_classes] must be an integer in [0, 1000]"));
    }

    public void testConstructor_GivenNumTopClassesIsGreaterThan1000() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new Classification("foo", BOOSTED_TREE_PARAMS, "result", 1001, 1.0, randomLong()));

        assertThat(e.getMessage(), equalTo("[num_top_classes] must be an integer in [0, 1000]"));
    }

    public void testGetPredictionFieldName() {
        Classification classification = new Classification("foo", BOOSTED_TREE_PARAMS, "result", 3, 50.0, randomLong());
        assertThat(classification.getPredictionFieldName(), equalTo("result"));

        classification = new Classification("foo", BOOSTED_TREE_PARAMS, null, 3, 50.0, randomLong());
        assertThat(classification.getPredictionFieldName(), equalTo("foo_prediction"));
    }

    public void testGetNumTopClasses() {
        Classification classification = new Classification("foo", BOOSTED_TREE_PARAMS, "result", 7, 1.0, randomLong());
        assertThat(classification.getNumTopClasses(), equalTo(7));

        // Boundary condition: num_top_classes == 0
        classification = new Classification("foo", BOOSTED_TREE_PARAMS, "result", 0, 1.0, randomLong());
        assertThat(classification.getNumTopClasses(), equalTo(0));

        // Boundary condition: num_top_classes == 1000
        classification = new Classification("foo", BOOSTED_TREE_PARAMS, "result", 1000, 1.0, randomLong());
        assertThat(classification.getNumTopClasses(), equalTo(1000));

        // num_top_classes == null, default applied
        classification = new Classification("foo", BOOSTED_TREE_PARAMS, "result", null, 1.0, randomLong());
        assertThat(classification.getNumTopClasses(), equalTo(2));
    }

    public void testGetTrainingPercent() {
        Classification classification = new Classification("foo", BOOSTED_TREE_PARAMS, "result", 3, 50.0, randomLong());
        assertThat(classification.getTrainingPercent(), equalTo(50.0));

        // Boundary condition: training_percent == 1.0
        classification = new Classification("foo", BOOSTED_TREE_PARAMS, "result", 3, 1.0, randomLong());
        assertThat(classification.getTrainingPercent(), equalTo(1.0));

        // Boundary condition: training_percent == 100.0
        classification = new Classification("foo", BOOSTED_TREE_PARAMS, "result", 3, 100.0, randomLong());
        assertThat(classification.getTrainingPercent(), equalTo(100.0));

        // training_percent == null, default applied
        classification = new Classification("foo", BOOSTED_TREE_PARAMS, "result", 3, null, randomLong());
        assertThat(classification.getTrainingPercent(), equalTo(100.0));
    }

    public void testGetParams() {
        Map<String, Set<String>> extractedFields =
            Map.of(
                "foo", Set.of(BooleanFieldMapper.CONTENT_TYPE),
                "bar", Set.of(NumberFieldMapper.NumberType.LONG.typeName()),
                "baz", Set.of(KeywordFieldMapper.CONTENT_TYPE));
        assertThat(
            new Classification("foo").getParams(extractedFields),
            equalTo(
                Map.of(
                    "dependent_variable", "foo",
                    "num_top_classes", 2,
                    "prediction_field_name", "foo_prediction",
                    "prediction_field_type", "bool")));
        assertThat(
            new Classification("bar").getParams(extractedFields),
            equalTo(
                Map.of(
                    "dependent_variable", "bar",
                    "num_top_classes", 2,
                    "prediction_field_name", "bar_prediction",
                    "prediction_field_type", "int")));
        assertThat(
            new Classification("baz").getParams(extractedFields),
            equalTo(
                Map.of(
                    "dependent_variable", "baz",
                    "num_top_classes", 2,
                    "prediction_field_name", "baz_prediction",
                    "prediction_field_type", "string")));
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
        assertThat(constraints.get(0).getUpperBound(), equalTo(2L));
    }

    public void testGetExplicitlyMappedFields() {
        assertThat(new Classification("foo").getExplicitlyMappedFields(null, "results"), is(anEmptyMap()));
        assertThat(new Classification("foo").getExplicitlyMappedFields(Collections.emptyMap(), "results"), is(anEmptyMap()));
        assertThat(
            new Classification("foo").getExplicitlyMappedFields(Collections.singletonMap("foo", "not_a_map"), "results"),
            is(anEmptyMap()));
        assertThat(
            new Classification("foo").getExplicitlyMappedFields(
                Collections.singletonMap("foo", Collections.singletonMap("bar", "baz")),
                "results"),
            allOf(
                hasEntry("results.foo_prediction", Collections.singletonMap("bar", "baz")),
                hasEntry("results.top_classes.class_name", Collections.singletonMap("bar", "baz"))));
        assertThat(
            new Classification("foo").getExplicitlyMappedFields(
                new HashMap<>() {{
                    put("foo", new HashMap<>() {{
                        put("type", "alias");
                        put("path", "bar");
                    }});
                    put("bar", Collections.singletonMap("type", "long"));
                }},
                "results"),
            allOf(
                hasEntry("results.foo_prediction", Collections.singletonMap("type", "long")),
                hasEntry("results.top_classes.class_name", Collections.singletonMap("type", "long"))));
        assertThat(
            new Classification("foo").getExplicitlyMappedFields(
                Collections.singletonMap("foo", new HashMap<>() {{
                    put("type", "alias");
                    put("path", "missing");
                }}),
                "results"),
            is(anEmptyMap()));
    }

    public void testToXContent_GivenVersionBeforeRandomizeSeedWasIntroduced() throws IOException {
        Classification classification = createRandom();
        assertThat(classification.getRandomizeSeed(), is(notNullValue()));

        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            classification.toXContent(builder, new ToXContent.MapParams(Collections.singletonMap("version", "7.5.0")));
            String json = Strings.toString(builder);
            assertThat(json, not(containsString("randomize_seed")));
        }
    }

    public void testToXContent_GivenVersionAfterRandomizeSeedWasIntroduced() throws IOException {
        Classification classification = createRandom();
        assertThat(classification.getRandomizeSeed(), is(notNullValue()));

        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            classification.toXContent(builder, new ToXContent.MapParams(Collections.singletonMap("version", Version.CURRENT.toString())));
            String json = Strings.toString(builder);
            assertThat(json, containsString("randomize_seed"));
        }
    }

    public void testToXContent_GivenVersionIsNull() throws IOException {
        Classification classification = createRandom();
        assertThat(classification.getRandomizeSeed(), is(notNullValue()));

        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            classification.toXContent(builder, new ToXContent.MapParams(Collections.singletonMap("version", null)));
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
        assertThat(classification.getStateDocId(randomId), equalTo(randomId + "_classification_state#1"));
    }

    public void testExtractJobIdFromStateDoc() {
        assertThat(Classification.extractJobIdFromStateDoc("foo_bar-1_classification_state#1"), equalTo("foo_bar-1"));
        assertThat(Classification.extractJobIdFromStateDoc("noop"), is(nullValue()));
    }

    @Override
    protected Classification mutateInstanceForVersion(Classification instance, Version version) {
        return mutateForVersion(instance, version);
    }
}
