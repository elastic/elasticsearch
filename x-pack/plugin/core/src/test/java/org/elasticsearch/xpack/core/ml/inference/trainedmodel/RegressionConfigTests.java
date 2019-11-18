/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class RegressionConfigTests extends AbstractWireSerializingTestCase<RegressionConfig> {

    public static RegressionConfig randomRegressionConfig() {
        return new RegressionConfig();
    }

    public void testFromMap() {
        RegressionConfig expected = new RegressionConfig();
        assertThat(RegressionConfig.fromMap(Collections.emptyMap()), equalTo(expected));
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

}
