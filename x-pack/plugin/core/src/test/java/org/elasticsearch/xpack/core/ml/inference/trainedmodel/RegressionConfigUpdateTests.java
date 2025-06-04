/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfigTests.randomRegressionConfig;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class RegressionConfigUpdateTests extends AbstractBWCSerializationTestCase<RegressionConfigUpdate> {

    public static RegressionConfigUpdate randomRegressionConfigUpdate() {
        return new RegressionConfigUpdate(
            randomBoolean() ? null : randomAlphaOfLength(10),
            randomBoolean() ? null : randomIntBetween(0, 10)
        );
    }

    public void testFromMap() {
        RegressionConfigUpdate expected = new RegressionConfigUpdate("foo", 3);
        Map<String, Object> config = new HashMap<>() {
            {
                put(RegressionConfig.RESULTS_FIELD.getPreferredName(), "foo");
                put(RegressionConfig.NUM_TOP_FEATURE_IMPORTANCE_VALUES.getPreferredName(), 3);
            }
        };
        assertThat(RegressionConfigUpdate.fromMap(config), equalTo(expected));
    }

    public void testFromMapWithUnknownField() {
        ElasticsearchException ex = expectThrows(
            ElasticsearchException.class,
            () -> RegressionConfigUpdate.fromMap(Collections.singletonMap("some_key", 1))
        );
        assertThat(ex.getMessage(), equalTo("Unrecognized fields [some_key]."));
    }

    public void testApply() {
        RegressionConfig originalConfig = randomRegressionConfig();

        assertThat(originalConfig, equalTo(originalConfig.apply(RegressionConfigUpdate.EMPTY_PARAMS)));

        assertThat(
            new RegressionConfig.Builder(originalConfig).setNumTopFeatureImportanceValues(5).build(),
            equalTo(originalConfig.apply(new RegressionConfigUpdate.Builder().setNumTopFeatureImportanceValues(5).build()))
        );
        assertThat(
            new RegressionConfig.Builder().setNumTopFeatureImportanceValues(1).setResultsField("foo").build(),
            equalTo(
                originalConfig.apply(
                    new RegressionConfigUpdate.Builder().setNumTopFeatureImportanceValues(1).setResultsField("foo").build()
                )
            )
        );
    }

    public void testInvalidResultFieldNotUnique() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> new RegressionConfigUpdate("warning", 0));
        assertEquals("Invalid inference config. More than one field is configured as [warning]", e.getMessage());
    }

    public void testNewBuilder() {
        RegressionConfigUpdate update = randomRegressionConfigUpdate();
        String newFieldName = update.getResultsField() + "_value";

        InferenceConfigUpdate updateWithField = update.newBuilder().setResultsField(newFieldName).build();

        assertNotSame(updateWithField, update);
        assertEquals(newFieldName, updateWithField.getResultsField());
        // other fields are the same
        assertThat(updateWithField, instanceOf(RegressionConfigUpdate.class));
        assertEquals(
            update.getNumTopFeatureImportanceValues(),
            ((RegressionConfigUpdate) updateWithField).getNumTopFeatureImportanceValues()
        );
    }

    @Override
    protected RegressionConfigUpdate createTestInstance() {
        return randomRegressionConfigUpdate();
    }

    @Override
    protected RegressionConfigUpdate mutateInstance(RegressionConfigUpdate instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<RegressionConfigUpdate> instanceReader() {
        return RegressionConfigUpdate::new;
    }

    @Override
    protected RegressionConfigUpdate doParseInstance(XContentParser parser) throws IOException {
        return RegressionConfigUpdate.fromXContentStrict(parser);
    }

    @Override
    protected RegressionConfigUpdate mutateInstanceForVersion(RegressionConfigUpdate instance, TransportVersion version) {
        return instance;
    }
}
