/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.job.process.autodetect.state;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.hamcrest.CustomTypeSafeMatcher;
import org.hamcrest.Matcher;

import static org.hamcrest.Matchers.closeTo;
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
        TimingStats stats1 = new TimingStats(JOB_ID, 7, 1.0, 2.0, 1.23, 7.89);
        TimingStats stats2 = new TimingStats(JOB_ID, 7, 1.0, 2.0, 1.23, 7.89);
        TimingStats stats3 = new TimingStats(JOB_ID, 7, 1.0, 3.0, 1.23, 7.89);

        assertTrue(stats1.equals(stats1));
        assertTrue(stats1.equals(stats2));
        assertFalse(stats2.equals(stats3));
    }

    public void testHashCode() {
        TimingStats stats1 = new TimingStats(JOB_ID, 7, 1.0, 2.0, 1.23, 7.89);
        TimingStats stats2 = new TimingStats(JOB_ID, 7, 1.0, 2.0, 1.23, 7.89);
        TimingStats stats3 = new TimingStats(JOB_ID, 7, 1.0, 3.0, 1.23, 7.89);

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
        assertThat(stats.getExponentialAvgBucketProcessingTimeMs(), nullValue());
    }

    public void testConstructor() {
        TimingStats stats = new TimingStats(JOB_ID, 7, 1.0, 2.0, 1.23, 7.89);

        assertThat(stats.getJobId(), equalTo(JOB_ID));
        assertThat(stats.getBucketCount(), equalTo(7L));
        assertThat(stats.getMinBucketProcessingTimeMs(), equalTo(1.0));
        assertThat(stats.getMaxBucketProcessingTimeMs(), equalTo(2.0));
        assertThat(stats.getAvgBucketProcessingTimeMs(), equalTo(1.23));
        assertThat(stats.getExponentialAvgBucketProcessingTimeMs(), equalTo(7.89));
    }

    public void testCopyConstructor() {
        TimingStats stats1 = new TimingStats(JOB_ID, 7, 1.0, 2.0, 1.23, 7.89);
        TimingStats stats2 = new TimingStats(stats1);

        assertThat(stats2.getJobId(), equalTo(JOB_ID));
        assertThat(stats2.getBucketCount(), equalTo(7L));
        assertThat(stats2.getMinBucketProcessingTimeMs(), equalTo(1.0));
        assertThat(stats2.getMaxBucketProcessingTimeMs(), equalTo(2.0));
        assertThat(stats2.getAvgBucketProcessingTimeMs(), equalTo(1.23));
        assertThat(stats2.getExponentialAvgBucketProcessingTimeMs(), equalTo(7.89));
        assertEquals(stats1, stats2);
        assertEquals(stats1.hashCode(), stats2.hashCode());
    }

    public void testUpdateStats() {
        TimingStats stats = new TimingStats(JOB_ID);

        stats.updateStats(3);
        assertThat(stats, areCloseTo(new TimingStats(JOB_ID, 1, 3.0, 3.0, 3.0, 3.0), 1e-9));

        stats.updateStats(2);
        assertThat(stats, areCloseTo(new TimingStats(JOB_ID, 2, 2.0, 3.0, 2.5, 2.99), 1e-9));

        stats.updateStats(4);
        assertThat(stats, areCloseTo(new TimingStats(JOB_ID, 3, 2.0, 4.0, 3.0, 3.0001), 1e-9));

        stats.updateStats(1);
        assertThat(stats, areCloseTo(new TimingStats(JOB_ID, 4, 1.0, 4.0, 2.5, 2.980099), 1e-9));

        stats.updateStats(5);
        assertThat(stats, areCloseTo(new TimingStats(JOB_ID, 5, 1.0, 5.0, 3.0, 3.00029801), 1e-9));
    }

    public void testDocumentId() {
        assertThat(TimingStats.documentId("my-job-id"), equalTo("my-job-id_timing_stats"));
    }

    public void testTimingStatsDifferSignificantly() {
        assertThat(
            TimingStats.differSignificantly(
                new TimingStats(JOB_ID, 10, 10.0, 10.0, 1.0, 10.0), new TimingStats(JOB_ID, 10, 10.0, 10.0, 1.0, 10.0)),
            is(false));
        assertThat(
            TimingStats.differSignificantly(
                new TimingStats(JOB_ID, 10, 10.0, 10.0, 1.0, 10.0), new TimingStats(JOB_ID, 10, 10.0, 11.0, 1.0, 10.0)),
            is(false));
        assertThat(
            TimingStats.differSignificantly(
                new TimingStats(JOB_ID, 10, 10.0, 10.0, 1.0, 10.0), new TimingStats(JOB_ID, 10, 10.0, 12.0, 1.0, 10.0)),
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

    /**
     * Creates a matcher of {@link TimingStats}s that matches when an examined stats are equal
     * to the specified <code>operand</code>, within a range of +/- <code>error</code>.
     *
     * @param operand
     *     the expected value of matching stats
     * @param error
     *     the delta (+/-) within which matches will be allowed
     */
    private static Matcher<TimingStats> areCloseTo(TimingStats operand, double error) {
        return new CustomTypeSafeMatcher<>("TimingStats close to " + operand) {
            @Override
            protected boolean matchesSafely(TimingStats item) {
                return equalTo(operand.getJobId()).matches(item.getJobId())
                    && equalTo(operand.getBucketCount()).matches(item.getBucketCount())
                    && closeTo(operand.getMinBucketProcessingTimeMs(), error).matches(item.getMinBucketProcessingTimeMs())
                    && closeTo(operand.getMaxBucketProcessingTimeMs(), error).matches(item.getMaxBucketProcessingTimeMs())
                    && closeTo(operand.getAvgBucketProcessingTimeMs(), error).matches(item.getAvgBucketProcessingTimeMs())
                    && closeTo(operand.getExponentialAvgBucketProcessingTimeMs(), error)
                        .matches(item.getExponentialAvgBucketProcessingTimeMs());
            }
        };
    }
}
