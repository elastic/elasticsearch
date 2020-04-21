/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfigTests.randomRegressionConfig;
import static org.hamcrest.Matchers.equalTo;

public class RegressionConfigUpdateTests extends AbstractBWCSerializationTestCase<RegressionConfigUpdate> {

    public static RegressionConfigUpdate randomRegressionConfigUpdate() {
        return new RegressionConfigUpdate(randomBoolean() ? null : randomAlphaOfLength(10),
            randomBoolean() ? null : randomIntBetween(0, 10));
    }

    public void testFromMap() {
        RegressionConfigUpdate expected = new RegressionConfigUpdate("foo", 3);
        Map<String, Object> config = new HashMap<>(){{
            put(RegressionConfig.RESULTS_FIELD.getPreferredName(), "foo");
            put(RegressionConfig.NUM_TOP_FEATURE_IMPORTANCE_VALUES.getPreferredName(), 3);
        }};
        assertThat(RegressionConfigUpdate.fromMap(config), equalTo(expected));
    }

    public void testFromMapWithUnknownField() {
        ElasticsearchException ex = expectThrows(ElasticsearchException.class,
            () -> RegressionConfigUpdate.fromMap(Collections.singletonMap("some_key", 1)));
        assertThat(ex.getMessage(), equalTo("Unrecognized fields [some_key]."));
    }

    public void testApply() {
        RegressionConfig originalConfig = randomRegressionConfig();

        assertThat(originalConfig, equalTo(RegressionConfigUpdate.EMPTY_PARAMS.apply(originalConfig)));

        assertThat(new RegressionConfig.Builder(originalConfig).setNumTopFeatureImportanceValues(5).build(),
            equalTo(new RegressionConfigUpdate.Builder().setNumTopFeatureImportanceValues(5).build().apply(originalConfig)));
        assertThat(new RegressionConfig.Builder()
                .setNumTopFeatureImportanceValues(1)
                .setResultsField("foo")
                .build(),
            equalTo(new RegressionConfigUpdate.Builder()
                .setNumTopFeatureImportanceValues(1)
                .setResultsField("foo")
                .build()
                .apply(originalConfig)
            ));
    }

    @Override
    protected RegressionConfigUpdate createTestInstance() {
        return randomRegressionConfigUpdate();
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
    protected RegressionConfigUpdate mutateInstanceForVersion(RegressionConfigUpdate instance, Version version) {
        return instance;
    }
}
