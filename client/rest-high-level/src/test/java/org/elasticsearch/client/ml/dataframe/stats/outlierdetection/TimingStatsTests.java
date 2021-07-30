/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.dataframe.stats.outlierdetection;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class TimingStatsTests extends AbstractXContentTestCase<TimingStats> {

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }


    @Override
    protected TimingStats doParseInstance(XContentParser parser) throws IOException {
        return TimingStats.PARSER.apply(parser, null);
    }

    @Override
    protected TimingStats createTestInstance() {
        return createRandom();
    }

    public static TimingStats createRandom() {
        return new TimingStats(randomBoolean() ? null : TimeValue.timeValueMillis(randomNonNegativeLong()));
    }
}
