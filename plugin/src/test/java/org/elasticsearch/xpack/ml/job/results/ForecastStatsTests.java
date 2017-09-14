/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.results;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

public class ForecastStatsTests extends AbstractSerializingTestCase<ForecastStats> {

    @Override
    protected ForecastStats parseInstance(XContentParser parser) {
        return ForecastStats.PARSER.apply(parser, null);
    }

    @Override
    protected ForecastStats createTestInstance() {
        return createTestInstance("ForecastStatsTest", randomNonNegativeLong());
    }

    public ForecastStats createTestInstance(String jobId, long forecastId) {
        ForecastStats forecastStats = new ForecastStats(jobId, forecastId);

        if (randomBoolean()) {
            forecastStats.setRecordCount(randomLong());
        }

        return forecastStats;
    }

    @Override
    protected Reader<ForecastStats> instanceReader() {
        return ForecastStats::new;
    }

    @Override
    protected ForecastStats doParseInstance(XContentParser parser) throws IOException {
        return ForecastStats.PARSER.apply(parser, null);
    }

}
