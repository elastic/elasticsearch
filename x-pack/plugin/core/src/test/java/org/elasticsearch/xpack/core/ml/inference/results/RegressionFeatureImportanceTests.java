/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class RegressionFeatureImportanceTests extends AbstractSerializingTestCase<RegressionFeatureImportance> {

    @Override
    protected RegressionFeatureImportance doParseInstance(XContentParser parser) throws IOException {
        return RegressionFeatureImportance.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<RegressionFeatureImportance> instanceReader() {
        return RegressionFeatureImportance::new;
    }

    @Override
    protected RegressionFeatureImportance createTestInstance() {
        return createRandomInstance();
    }

    public static RegressionFeatureImportance createRandomInstance() {
        return new RegressionFeatureImportance(randomAlphaOfLength(10), randomDoubleBetween(-10.0, 10.0, false));
    }
}
