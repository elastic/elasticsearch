/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.dataframe.stats.outlierdetection;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.time.Instant;

public class OutlierDetectionStatsTests extends AbstractXContentTestCase<OutlierDetectionStats> {

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected OutlierDetectionStats doParseInstance(XContentParser parser) throws IOException {
        return OutlierDetectionStats.PARSER.apply(parser, null);
    }

    @Override
    protected OutlierDetectionStats createTestInstance() {
        return createRandom();
    }

    public static OutlierDetectionStats createRandom() {
        return new OutlierDetectionStats(
            Instant.now(),
            ParametersTests.createRandom(),
            TimingStatsTests.createRandom()
        );
    }
}
