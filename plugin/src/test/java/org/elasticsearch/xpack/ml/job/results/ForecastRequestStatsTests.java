/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.results;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.ml.job.results.ForecastRequestStats.ForecastRequestStatus;

import java.io.IOException;
import java.time.Instant;

public class ForecastRequestStatsTests extends AbstractSerializingTestCase<ForecastRequestStats> {

    @Override
    protected ForecastRequestStats parseInstance(XContentParser parser) {
        return ForecastRequestStats.PARSER.apply(parser, null);
    }

    @Override
    protected ForecastRequestStats createTestInstance() {
        return createTestInstance("ForecastRequestStatsTest", randomNonNegativeLong());
    }

    public ForecastRequestStats createTestInstance(String jobId, long forecastId) {
        ForecastRequestStats forecastRequestStats = new ForecastRequestStats(jobId, forecastId);

        if (randomBoolean()) {
            forecastRequestStats.setRecordCount(randomLong());
        }
        if (randomBoolean()) {
            forecastRequestStats.setMessage(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            forecastRequestStats.setStartTimeStamp(Instant.ofEpochMilli(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            forecastRequestStats.setEndTimeStamp(Instant.ofEpochMilli(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            forecastRequestStats.setProgress(randomDouble());
        }
        if (randomBoolean()) {
            forecastRequestStats.setProcessingTime(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            forecastRequestStats.setMemoryUsage(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            forecastRequestStats.setStatus(randomFrom(ForecastRequestStatus.values()));
        }

        return forecastRequestStats;
    }

    @Override
    protected Reader<ForecastRequestStats> instanceReader() {
        return ForecastRequestStats::new;
    }

    @Override
    protected ForecastRequestStats doParseInstance(XContentParser parser) throws IOException {
        return ForecastRequestStats.PARSER.apply(parser, null);
    }

}
