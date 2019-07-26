/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.datafeed;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class DatafeedTimingStatsTests extends AbstractSerializingTestCase<DatafeedTimingStats> {

    private static final String JOB_ID = "my-job-id";

    public static DatafeedTimingStats createRandom() {
        return new DatafeedTimingStats(randomAlphaOfLength(10), randomLong(), randomLong(), randomDouble());
    }

    @Override
    protected DatafeedTimingStats createTestInstance(){
        return createRandom();
    }

    @Override
    protected Writeable.Reader<DatafeedTimingStats> instanceReader() {
        return DatafeedTimingStats::new;
    }

    @Override
    protected DatafeedTimingStats doParseInstance(XContentParser parser) {
        return DatafeedTimingStats.PARSER.apply(parser, null);
    }

    @Override
    protected DatafeedTimingStats mutateInstance(DatafeedTimingStats instance) throws IOException {
        String jobId = instance.getJobId();
        long searchCount = instance.getSearchCount();
        long bucketCount = instance.getBucketCount();
        double totalSearchTimeMs = instance.getTotalSearchTimeMs();
        return new DatafeedTimingStats(
            jobId + randomAlphaOfLength(5),
            searchCount + 2,
            bucketCount + 1,
            totalSearchTimeMs + randomDoubleBetween(1.0, 100.0, true));
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
        }
    }

    public void testEquals() {
        DatafeedTimingStats stats1 = new DatafeedTimingStats(JOB_ID, 5, 10, 100.0);
        DatafeedTimingStats stats2 = new DatafeedTimingStats(JOB_ID, 5, 10, 100.0);
        DatafeedTimingStats stats3 = new DatafeedTimingStats(JOB_ID, 5, 10, 200.0);

        assertTrue(stats1.equals(stats1));
        assertTrue(stats1.equals(stats2));
        assertFalse(stats2.equals(stats3));
    }

    public void testHashCode() {
        DatafeedTimingStats stats1 = new DatafeedTimingStats(JOB_ID, 5, 10, 100.0);
        DatafeedTimingStats stats2 = new DatafeedTimingStats(JOB_ID, 5, 10, 100.0);
        DatafeedTimingStats stats3 = new DatafeedTimingStats(JOB_ID, 5, 10, 200.0);

        assertEquals(stats1.hashCode(), stats1.hashCode());
        assertEquals(stats1.hashCode(), stats2.hashCode());
        assertNotEquals(stats2.hashCode(), stats3.hashCode());
    }

    public void testConstructorsAndGetters() {
        DatafeedTimingStats stats = new DatafeedTimingStats(JOB_ID, 5, 10, 123.456);
        assertThat(stats.getJobId(), equalTo(JOB_ID));
        assertThat(stats.getSearchCount(), equalTo(5L));
        assertThat(stats.getBucketCount(), equalTo(10L));
        assertThat(stats.getTotalSearchTimeMs(), equalTo(123.456));
        assertThat(stats.getAvgSearchTimePerBucketMs(), closeTo(12.3456, 1e-9));

        stats = new DatafeedTimingStats(JOB_ID);
        assertThat(stats.getJobId(), equalTo(JOB_ID));
        assertThat(stats.getSearchCount(), equalTo(0L));
        assertThat(stats.getBucketCount(), equalTo(0L));
        assertThat(stats.getTotalSearchTimeMs(), equalTo(0.0));
        assertThat(stats.getAvgSearchTimePerBucketMs(), nullValue());
    }

    public void testCopyConstructor() {
        DatafeedTimingStats stats1 = new DatafeedTimingStats(JOB_ID, 5, 10, 123.456);
        DatafeedTimingStats stats2 = new DatafeedTimingStats(stats1);

        assertThat(stats2.getJobId(), equalTo(JOB_ID));
        assertThat(stats2.getSearchCount(), equalTo(5L));
        assertThat(stats2.getBucketCount(), equalTo(10L));
        assertThat(stats2.getTotalSearchTimeMs(), equalTo(123.456));
        assertThat(stats2.getAvgSearchTimePerBucketMs(), closeTo(12.3456, 1e-9));
    }

    public void testIncrementTotalSearchTimeMs() {
        DatafeedTimingStats stats = new DatafeedTimingStats(JOB_ID, 5, 10, 100.0);
        stats.incrementTotalSearchTimeMs(200.0);
        assertThat(stats.getJobId(), equalTo(JOB_ID));
        assertThat(stats.getSearchCount(), equalTo(6L));
        assertThat(stats.getBucketCount(), equalTo(10L));
        assertThat(stats.getTotalSearchTimeMs(), equalTo(300.0));
        assertThat(stats.getAvgSearchTimePerBucketMs(), equalTo(30.0));
    }

    public void testIncrementBucketCount() {
        DatafeedTimingStats stats = new DatafeedTimingStats(JOB_ID, 5, 10, 100.0);
        stats.incrementBucketCount(10);
        assertThat(stats.getJobId(), equalTo(JOB_ID));
        assertThat(stats.getSearchCount(), equalTo(5L));
        assertThat(stats.getBucketCount(), equalTo(20L));
        assertThat(stats.getTotalSearchTimeMs(), equalTo(100.0));
        assertThat(stats.getAvgSearchTimePerBucketMs(), equalTo(5.0));
    }

    public void testAvgSearchTimePerBucketIsCalculatedProperlyAfterUpdates() {
        DatafeedTimingStats stats = new DatafeedTimingStats(JOB_ID, 5, 10, 100.0);
        assertThat(stats.getBucketCount(), equalTo(10L));
        assertThat(stats.getTotalSearchTimeMs(), equalTo(100.0));
        assertThat(stats.getAvgSearchTimePerBucketMs(), equalTo(10.0));

        stats.incrementBucketCount(10);
        assertThat(stats.getBucketCount(), equalTo(20L));
        assertThat(stats.getTotalSearchTimeMs(), equalTo(100.0));
        assertThat(stats.getAvgSearchTimePerBucketMs(), equalTo(5.0));

        stats.incrementTotalSearchTimeMs(200.0);
        assertThat(stats.getBucketCount(), equalTo(20L));
        assertThat(stats.getTotalSearchTimeMs(), equalTo(300.0));
        assertThat(stats.getAvgSearchTimePerBucketMs(), equalTo(15.0));

        stats.incrementBucketCount(5);
        assertThat(stats.getBucketCount(), equalTo(25L));
        assertThat(stats.getTotalSearchTimeMs(), equalTo(300.0));
        assertThat(stats.getAvgSearchTimePerBucketMs(), equalTo(12.0));
    }

    public void testDocumentId() {
        assertThat(DatafeedTimingStats.documentId("my-job-id"), equalTo("my-job-id_datafeed_timing_stats"));
    }
}
