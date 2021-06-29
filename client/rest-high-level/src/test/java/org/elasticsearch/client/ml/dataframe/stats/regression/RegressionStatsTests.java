/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.dataframe.stats.regression;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.time.Instant;

public class RegressionStatsTests extends AbstractXContentTestCase<RegressionStats> {

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected RegressionStats doParseInstance(XContentParser parser) throws IOException {
        return RegressionStats.PARSER.apply(parser, null);
    }


    @Override
    protected RegressionStats createTestInstance() {
        return createRandom();
    }

    public static RegressionStats createRandom() {
        return new RegressionStats(
            Instant.now(),
            randomBoolean() ? null : randomIntBetween(1, Integer.MAX_VALUE),
            HyperparametersTests.createRandom(),
            TimingStatsTests.createRandom(),
            ValidationLossTests.createRandom()
        );
    }
}
