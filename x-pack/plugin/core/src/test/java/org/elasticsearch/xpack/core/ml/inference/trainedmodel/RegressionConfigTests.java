/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class RegressionConfigTests extends AbstractWireSerializingTestCase<RegressionConfig> {

    public static RegressionConfig randomRegressionConfig() {
        return new RegressionConfig();
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
