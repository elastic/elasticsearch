/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.job.process;

import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class TimingStatsTests extends AbstractXContentTestCase<TimingStats> {

    private static final String JOB_ID = "my-job-id";

    public static TimingStats createTestInstance(String jobId) {
        return new TimingStats(
            jobId,
            randomLong(),
            randomDouble(),
            randomBoolean() ? null : randomDouble(),
            randomBoolean() ? null : randomDouble(),
            randomBoolean() ? null : randomDouble(),
            randomBoolean() ? null : randomDouble(),
            randomBoolean() ? null : randomDouble());
    }

    @Override
    public TimingStats createTestInstance() {
        return createTestInstance(randomAlphaOfLength(10));
    }

    @Override
    protected TimingStats doParseInstance(XContentParser parser) {
        return TimingStats.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    public void testConstructor() {
        TimingStats stats = new TimingStats(JOB_ID, 7, 8.61, 1.0, 2.0, 1.23, 7.89, 4.56);

        assertThat(stats.getJobId(), equalTo(JOB_ID));
        assertThat(stats.getBucketCount(), equalTo(7L));
        assertThat(stats.getTotalBucketProcessingTimeMs(), equalTo(8.61));
        assertThat(stats.getMinBucketProcessingTimeMs(), equalTo(1.0));
        assertThat(stats.getMaxBucketProcessingTimeMs(), equalTo(2.0));
        assertThat(stats.getAvgBucketProcessingTimeMs(), equalTo(1.23));
        assertThat(stats.getExponentialAvgBucketProcessingTimeMs(), equalTo(7.89));
        assertThat(stats.getExponentialAvgBucketProcessingTimePerHourMs(), equalTo(4.56));
    }

    public void testConstructor_NullValues() {
        TimingStats stats = new TimingStats(JOB_ID, 7, 8.61, null, null, null, null, null);

        assertThat(stats.getJobId(), equalTo(JOB_ID));
        assertThat(stats.getBucketCount(), equalTo(7L));
        assertThat(stats.getTotalBucketProcessingTimeMs(), equalTo(8.61));
        assertThat(stats.getMinBucketProcessingTimeMs(), nullValue());
        assertThat(stats.getMaxBucketProcessingTimeMs(), nullValue());
        assertThat(stats.getAvgBucketProcessingTimeMs(), nullValue());
        assertThat(stats.getExponentialAvgBucketProcessingTimeMs(), nullValue());
        assertThat(stats.getExponentialAvgBucketProcessingTimePerHourMs(), nullValue());
    }

    public void testParse_OptionalFieldsAbsent() throws IOException {
        String json = "{\"job_id\": \"my-job-id\"}";
        try (XContentParser parser =
                 XContentFactory.xContent(XContentType.JSON).createParser(
                     xContentRegistry(), DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json)) {
            TimingStats stats = TimingStats.PARSER.apply(parser, null);
            assertThat(stats.getJobId(), equalTo(JOB_ID));
            assertThat(stats.getBucketCount(), equalTo(0L));
            assertThat(stats.getTotalBucketProcessingTimeMs(), equalTo(0.0));
            assertThat(stats.getMinBucketProcessingTimeMs(), nullValue());
            assertThat(stats.getMaxBucketProcessingTimeMs(), nullValue());
            assertThat(stats.getAvgBucketProcessingTimeMs(), nullValue());
            assertThat(stats.getExponentialAvgBucketProcessingTimeMs(), nullValue());
            assertThat(stats.getExponentialAvgBucketProcessingTimePerHourMs(), nullValue());
        }
    }
}
