/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfigTests.randomClassificationConfig;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class ClassificationConfigUpdateTests extends AbstractBWCSerializationTestCase<ClassificationConfigUpdate> {

    public static ClassificationConfigUpdate randomClassificationConfigUpdate() {
        return new ClassificationConfigUpdate(
            randomBoolean() ? null : randomIntBetween(-1, 10),
            randomBoolean() ? null : randomAlphaOfLength(10),
            randomBoolean() ? null : randomAlphaOfLength(10),
            randomBoolean() ? null : randomIntBetween(0, 10),
            randomBoolean() ? null : randomFrom(PredictionFieldType.values())
        );
    }

    public void testFromMap() {
        ClassificationConfigUpdate expected = ClassificationConfigUpdate.EMPTY_PARAMS;
        assertThat(ClassificationConfigUpdate.fromMap(Collections.emptyMap()), equalTo(expected));

        expected = new ClassificationConfigUpdate(3, "foo", "bar", 2, PredictionFieldType.NUMBER);
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ClassificationConfig.NUM_TOP_CLASSES.getPreferredName(), 3);
        configMap.put(ClassificationConfig.RESULTS_FIELD.getPreferredName(), "foo");
        configMap.put(ClassificationConfig.TOP_CLASSES_RESULTS_FIELD.getPreferredName(), "bar");
        configMap.put(ClassificationConfig.NUM_TOP_FEATURE_IMPORTANCE_VALUES.getPreferredName(), 2);
        configMap.put(ClassificationConfig.PREDICTION_FIELD_TYPE.getPreferredName(), PredictionFieldType.NUMBER.toString());
        assertThat(ClassificationConfigUpdate.fromMap(configMap), equalTo(expected));
    }

    public void testFromMapWithUnknownField() {
        ElasticsearchException ex = expectThrows(
            ElasticsearchException.class,
            () -> ClassificationConfigUpdate.fromMap(Collections.singletonMap("some_key", 1))
        );
        assertThat(ex.getMessage(), equalTo("Unrecognized fields [some_key]."));
    }

    public void testApply() {
        ClassificationConfig originalConfig = randomClassificationConfig();

        assertThat(originalConfig, equalTo(originalConfig.apply(ClassificationConfigUpdate.EMPTY_PARAMS)));

        assertThat(
            new ClassificationConfig.Builder(originalConfig).setNumTopClasses(5).build(),
            equalTo(originalConfig.apply(new ClassificationConfigUpdate.Builder().setNumTopClasses(5).build()))
        );
        assertThat(
            new ClassificationConfig.Builder().setNumTopClasses(5)
                .setNumTopFeatureImportanceValues(1)
                .setPredictionFieldType(PredictionFieldType.BOOLEAN)
                .setResultsField("foo")
                .setTopClassesResultsField("bar")
                .build(),
            equalTo(
                originalConfig.apply(
                    new ClassificationConfigUpdate.Builder().setNumTopClasses(5)
                        .setNumTopFeatureImportanceValues(1)
                        .setPredictionFieldType(PredictionFieldType.BOOLEAN)
                        .setResultsField("foo")
                        .setTopClassesResultsField("bar")
                        .build()
                )
            )
        );
    }

    public void testDuplicateFieldNamesThrow() {
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> new ClassificationConfigUpdate(5, "foo", "foo", 1, PredictionFieldType.BOOLEAN)
        );

        assertEquals("Invalid inference config. More than one field is configured as [foo]", e.getMessage());
    }

    public void testDuplicateWithResultsField() {
        ClassificationConfigUpdate update = randomClassificationConfigUpdate();
        String newFieldName = update.getResultsField() + "_value";

        InferenceConfigUpdate updateWithField = update.newBuilder().setResultsField(newFieldName).build();

        assertNotSame(updateWithField, update);
        assertEquals(newFieldName, updateWithField.getResultsField());
        // other fields are the same
        assertThat(updateWithField, instanceOf(ClassificationConfigUpdate.class));
        ClassificationConfigUpdate classUpdate = (ClassificationConfigUpdate) updateWithField;
        assertEquals(update.getTopClassesResultsField(), classUpdate.getTopClassesResultsField());
        assertEquals(update.getNumTopClasses(), classUpdate.getNumTopClasses());
        assertEquals(update.getPredictionFieldType(), classUpdate.getPredictionFieldType());
        assertEquals(update.getNumTopFeatureImportanceValues(), classUpdate.getNumTopFeatureImportanceValues());
    }

    @Override
    protected ClassificationConfigUpdate createTestInstance() {
        return randomClassificationConfigUpdate();
    }

    @Override
    protected ClassificationConfigUpdate mutateInstance(ClassificationConfigUpdate instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<ClassificationConfigUpdate> instanceReader() {
        return ClassificationConfigUpdate::new;
    }

    @Override
    protected ClassificationConfigUpdate doParseInstance(XContentParser parser) throws IOException {
        return ClassificationConfigUpdate.fromXContentStrict(parser);
    }

    @Override
    protected ClassificationConfigUpdate mutateInstanceForVersion(ClassificationConfigUpdate instance, TransportVersion version) {
        return instance;
    }
}
