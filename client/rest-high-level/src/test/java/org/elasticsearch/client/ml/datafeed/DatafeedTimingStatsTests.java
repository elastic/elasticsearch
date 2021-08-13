/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.datafeed;

import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class DatafeedTimingStatsTests extends AbstractXContentTestCase<DatafeedTimingStats> {

    private static final String JOB_ID = "my-job-id";

    public static DatafeedTimingStats createRandomInstance() {
        return new DatafeedTimingStats(
            randomAlphaOfLength(10),
            randomLong(),
            randomLong(),
            randomDouble(),
            randomBoolean() ? null : randomDouble(),
            randomBoolean() ? null : randomDouble());
    }

    @Override
    protected DatafeedTimingStats createTestInstance() {
        return createRandomInstance();
    }

    @Override
    protected DatafeedTimingStats doParseInstance(XContentParser parser) throws IOException {
        return DatafeedTimingStats.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    public void testParse_OptionalFieldsAbsent() throws IOException {
        String json = "{\"job_id\": \"my-job-id\"}";
        try (XContentParser parser =
                 XContentFactory.xContent(XContentType.JSON).createParser(
                     xContentRegistry(), DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json)) {
            DatafeedTimingStats stats = DatafeedTimingStats.PARSER.apply(parser, null);
            assertThat(stats.getJobId(), equalTo(JOB_ID));
            assertThat(stats.getSearchCount(), equalTo(0L));
            assertThat(stats.getBucketCount(), equalTo(0L));
            assertThat(stats.getTotalSearchTimeMs(), equalTo(0.0));
            assertThat(stats.getAvgSearchTimePerBucketMs(), nullValue());
            assertThat(stats.getExponentialAvgSearchTimePerHourMs(), nullValue());
        }
    }

    public void testConstructorAndGetters() {
        DatafeedTimingStats stats = new DatafeedTimingStats(JOB_ID, 5, 10, 123.456, 78.9, 98.7);
        assertThat(stats.getJobId(), equalTo(JOB_ID));
        assertThat(stats.getSearchCount(), equalTo(5L));
        assertThat(stats.getBucketCount(), equalTo(10L));
        assertThat(stats.getTotalSearchTimeMs(), equalTo(123.456));
        assertThat(stats.getAvgSearchTimePerBucketMs(), equalTo(78.9));
        assertThat(stats.getExponentialAvgSearchTimePerHourMs(), equalTo(98.7));
    }
}
