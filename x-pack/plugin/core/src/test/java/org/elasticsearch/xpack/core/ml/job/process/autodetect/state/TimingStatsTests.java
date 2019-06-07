/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.job.process.autodetect.state;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class TimingStatsTests extends AbstractSerializingTestCase<TimingStats> {

    private static final String JOB_ID = "my-job-id";

    public static TimingStats createTestInstance(String jobId) {
        return new TimingStats(
            jobId,
            randomLong(),
            randomBoolean() ? null : randomDouble(),
            randomBoolean() ? null : randomDouble(),
            randomBoolean() ? null : randomDouble());
    }

    @Override
    public TimingStats createTestInstance() {
        return createTestInstance(randomAlphaOfLength(10));
    }

    @Override
    protected Writeable.Reader<TimingStats> instanceReader() {
        return TimingStats::new;
    }

    @Override
    protected TimingStats doParseInstance(XContentParser parser) {
        return TimingStats.PARSER.apply(parser, null);
    }

    public void testEquals() {
        TimingStats stats1 = new TimingStats(JOB_ID, 7, 1.0, 2.0, 1.23);
        TimingStats stats2 = new TimingStats(JOB_ID, 7, 1.0, 2.0, 1.23);
        TimingStats stats3 = new TimingStats(JOB_ID, 7, 1.0, 3.0, 1.23);

        assertTrue(stats1.equals(stats1));
        assertTrue(stats1.equals(stats2));
        assertFalse(stats2.equals(stats3));
    }

    public void testHashCode() {
        TimingStats stats1 = new TimingStats(JOB_ID, 7, 1.0, 2.0, 1.23);
        TimingStats stats2 = new TimingStats(JOB_ID, 7, 1.0, 2.0, 1.23);
        TimingStats stats3 = new TimingStats(JOB_ID, 7, 1.0, 3.0, 1.23);

        assertEquals(stats1.hashCode(), stats1.hashCode());
        assertEquals(stats1.hashCode(), stats2.hashCode());
        assertNotEquals(stats2.hashCode(), stats3.hashCode());
    }

    public void testDefaultConstructor() {
        TimingStats stats = new TimingStats(JOB_ID);

        assertThat(stats.getJobId(), equalTo(JOB_ID));
        assertThat(stats.getBucketCount(), equalTo(0L));
        assertThat(stats.getMinBucketProcessingTimeMs(), nullValue());
        assertThat(stats.getMaxBucketProcessingTimeMs(), nullValue());
        assertThat(stats.getAvgBucketProcessingTimeMs(), nullValue());
    }

    public void testConstructor() {
        TimingStats stats = new TimingStats(JOB_ID, 7, 1.0, 2.0, 1.23);

        assertThat(stats.getJobId(), equalTo(JOB_ID));
        assertThat(stats.getBucketCount(), equalTo(7L));
        assertThat(stats.getMinBucketProcessingTimeMs(), equalTo(1.0));
        assertThat(stats.getMaxBucketProcessingTimeMs(), equalTo(2.0));
        assertThat(stats.getAvgBucketProcessingTimeMs(), equalTo(1.23));
    }

    public void testCopyConstructor() {
        TimingStats stats1 = new TimingStats(JOB_ID, 7, 1.0, 2.0, 1.23);
        TimingStats stats2 = new TimingStats(stats1);

        assertThat(stats2.getJobId(), equalTo(JOB_ID));
        assertThat(stats2.getBucketCount(), equalTo(7L));
        assertThat(stats2.getMinBucketProcessingTimeMs(), equalTo(1.0));
        assertThat(stats2.getMaxBucketProcessingTimeMs(), equalTo(2.0));
        assertThat(stats2.getAvgBucketProcessingTimeMs(), equalTo(1.23));
        assertEquals(stats1, stats2);
        assertEquals(stats1.hashCode(), stats2.hashCode());
    }

    public void testUpdateStats() {
        TimingStats stats = new TimingStats(JOB_ID);

        stats.updateStats(3);
        assertThat(stats, equalTo(new TimingStats(JOB_ID, 1, 3.0, 3.0, 3.0)));

        stats.updateStats(2);
        assertThat(stats, equalTo(new TimingStats(JOB_ID, 2, 2.0, 3.0, 2.5)));

        stats.updateStats(4);
        assertThat(stats, equalTo(new TimingStats(JOB_ID, 3, 2.0, 4.0, 3.0)));

        stats.updateStats(1);
        assertThat(stats, equalTo(new TimingStats(JOB_ID, 4, 1.0, 4.0, 2.5)));

        stats.updateStats(5);
        assertThat(stats, equalTo(new TimingStats(JOB_ID, 5, 1.0, 5.0, 3.0)));
    }

    public void testDocumentId() {
        assertThat(TimingStats.documentId("my-job-id"), equalTo("my-job-id_timing_stats"));
    }

    public void testTimingStatsDifferSignificantly() {
        assertThat(
            TimingStats.differSignificantly(
                new TimingStats(JOB_ID, 10, 10.0, 10.0, 1.0), new TimingStats(JOB_ID, 10, 10.0, 10.0, 1.0)),
            is(false));
        assertThat(
            TimingStats.differSignificantly(
                new TimingStats(JOB_ID, 10, 10.0, 10.0, 1.0), new TimingStats(JOB_ID, 10, 10.0, 11.0, 1.0)),
            is(false));
        assertThat(
            TimingStats.differSignificantly(
                new TimingStats(JOB_ID, 10, 10.0, 10.0, 1.0), new TimingStats(JOB_ID, 10, 10.0, 12.0, 1.0)),
            is(true));
    }

    public void testValuesDifferSignificantly() {
        assertThat(TimingStats.differSignificantly((Double) null, (Double) null), is(false));
        assertThat(TimingStats.differSignificantly(1.0, null), is(true));
        assertThat(TimingStats.differSignificantly(null, 1.0), is(true));
        assertThat(TimingStats.differSignificantly(0.9, 1.0), is(false));
        assertThat(TimingStats.differSignificantly(1.0, 0.9), is(false));
        assertThat(TimingStats.differSignificantly(0.9, 1.000001), is(true));
        assertThat(TimingStats.differSignificantly(1.0, 0.899999), is(true));
        assertThat(TimingStats.differSignificantly(0.0, 1.0), is(true));
        assertThat(TimingStats.differSignificantly(1.0, 0.0), is(true));
    }
}
