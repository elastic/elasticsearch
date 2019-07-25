/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.analyses;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

public class RegressionTests extends AbstractSerializingTestCase<Regression> {

    @Override
    protected Regression doParseInstance(XContentParser parser) throws IOException {
        return Regression.fromXContent(parser, false);
    }

    @Override
    protected Regression createTestInstance() {
        return createRandom();
    }

    public static Regression createRandom() {
        Double lambda = randomBoolean() ? null : randomDoubleBetween(0.0, Double.MAX_VALUE, true);
        Double gamma = randomBoolean() ? null : randomDoubleBetween(0.0, Double.MAX_VALUE, true);
        Double eta = randomBoolean() ? null : randomDoubleBetween(0.001, 1.0, true);
        Integer maximumNumberTrees = randomBoolean() ? null : randomIntBetween(1, 2000);
        Double featureBagFraction = randomBoolean() ? null : randomDoubleBetween(0.0, 1.0, false);
        String predictionFieldName = randomBoolean() ? null : randomAlphaOfLength(10);
        return new Regression(randomAlphaOfLength(10), lambda, gamma, eta, maximumNumberTrees, featureBagFraction,
            predictionFieldName);
    }

    @Override
    protected Writeable.Reader<Regression> instanceReader() {
        return Regression::new;
    }
}
