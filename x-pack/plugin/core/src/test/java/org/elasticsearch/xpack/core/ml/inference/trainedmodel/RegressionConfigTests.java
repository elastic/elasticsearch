/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class RegressionConfigTests extends AbstractSerializingTestCase<RegressionConfig> {

    public static RegressionConfig randomRegressionConfig() {
        return new RegressionConfig(randomBoolean() ? null : randomAlphaOfLength(10));
    }

    public void testFromMap() {
        RegressionConfig expected = new RegressionConfig("foo");
        Map<String, Object> config = new HashMap<>(){{
            put(RegressionConfig.RESULTS_FIELD.getPreferredName(), "foo");
        }};
        assertThat(RegressionConfig.fromMap(config), equalTo(expected));
    }

    public void testFromMapWithUnknownField() {
        ElasticsearchException ex = expectThrows(ElasticsearchException.class,
            () -> RegressionConfig.fromMap(Collections.singletonMap("some_key", 1)));
        assertThat(ex.getMessage(), equalTo("Unrecognized fields [some_key]."));
    }

    @Override
    protected RegressionConfig createTestInstance() {
        return randomRegressionConfig();
    }

    @Override
    protected Writeable.Reader<RegressionConfig> instanceReader() {
        return RegressionConfig::new;
    }

    @Override
    protected RegressionConfig doParseInstance(XContentParser parser) throws IOException {
        return RegressionConfig.fromXContent(parser);
    }
}
