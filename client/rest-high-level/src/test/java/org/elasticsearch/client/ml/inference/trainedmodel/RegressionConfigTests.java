/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml.inference.trainedmodel;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class RegressionConfigTests extends AbstractXContentTestCase<RegressionConfig> {

    public static RegressionConfig randomRegressionConfig() {
        return new RegressionConfig(
            randomBoolean() ? null : randomAlphaOfLength(10),
            randomBoolean() ? null : randomIntBetween(0, 10));
    }


    @Override
    protected RegressionConfig createTestInstance() {
        return randomRegressionConfig();
    }

    @Override
    protected RegressionConfig doParseInstance(XContentParser parser) throws IOException {
        return RegressionConfig.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
