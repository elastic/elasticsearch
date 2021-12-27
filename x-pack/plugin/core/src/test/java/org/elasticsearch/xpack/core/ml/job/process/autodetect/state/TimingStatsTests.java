/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.process.autodetect.state;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ExponentialAverageCalculationContext;
import org.elasticsearch.xpack.core.ml.utils.ExponentialAverageCalculationContextTests;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;
import org.hamcrest.CustomTypeSafeMatcher;
import org.hamcrest.Matcher;

import java.time.Instant;
import java.util.Collections;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
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
            randomBoolean() ? null : randomDouble(),
            ExponentialAverageCalculationContextTests.createRandom()
        );
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

    @Override
    protected ToXContent.Params getToXContentParams() {
        return new ToXContent.MapParams(Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true"));
    }

    public void testDefaultConstructor() {
        TimingStats stats = new TimingStats(JOB_ID);

        assertThat(stats.getJobId(), equalTo(JOB_ID));
        assertThat(stats.getBucketCount(), equalTo(0L));
        assertThat(stats.getTotalBucketProcessingTimeMs(), equalTo(0.0));
        assertThat(stats.getMinBucketProcessingTimeMs(), nullValue());
        assertThat(stats.getMaxBucketProcessingTimeMs(), nullValue());
        assertThat(stats.getAvgBucketProcessingTimeMs(), nullValue());
        assertThat(stats.getExponentialAvgBucketProcessingTimeMs(), nullValue());
        assertThat(stats.getExponentialAvgCalculationContext(), equalTo(new ExponentialAverageCalculationContext()));
    }

    public void testConstructor() {
        ExponentialAverageCalculationContext context = new ExponentialAverageCalculationContext(
            78.9,
            Instant.ofEpochMilli(123456789),
            987.0
        );
        TimingStats stats = new TimingStats(JOB_ID, 7, 1.0, 2.0, 1.23, 7.89, context);

        assertThat(stats.getJobId(), equalTo(JOB_ID));
        assertThat(stats.getBucketCount(), equalTo(7L));
        assertThat(stats.getTotalBucketProcessingTimeMs(), equalTo(8.61));
        assertThat(stats.getMinBucketProcessingTimeMs(), equalTo(1.0));
        assertThat(stats.getMaxBucketProcessingTimeMs(), equalTo(2.0));
        assertThat(stats.getAvgBucketProcessingTimeMs(), equalTo(1.23));
        assertThat(stats.getExponentialAvgBucketProcessingTimeMs(), equalTo(7.89));
        assertThat(stats.getExponentialAvgCalculationContext(), equalTo(context));
    }

    public void testCopyConstructor() {
        ExponentialAverageCalculationContext context = new ExponentialAverageCalculationContext(
            78.9,
            Instant.ofEpochMilli(123456789),
            987.0
        );
        TimingStats stats1 = new TimingStats(JOB_ID, 7, 1.0, 2.0, 1.23, 7.89, context);
        TimingStats stats2 = new TimingStats(stats1);

        assertThat(stats2.getJobId(), equalTo(JOB_ID));
        assertThat(stats2.getBucketCount(), equalTo(7L));
        assertThat(stats2.getTotalBucketProcessingTimeMs(), equalTo(8.61));
        assertThat(stats2.getMinBucketProcessingTimeMs(), equalTo(1.0));
        assertThat(stats2.getMaxBucketProcessingTimeMs(), equalTo(2.0));
        assertThat(stats2.getAvgBucketProcessingTimeMs(), equalTo(1.23));
        assertThat(stats2.getExponentialAvgBucketProcessingTimeMs(), equalTo(7.89));
        assertThat(stats2.getExponentialAvgCalculationContext(), equalTo(context));
    }

    public void testUpdateStats() {
        TimingStats stats = new TimingStats(JOB_ID);

        stats.updateStats(3);
        assertThat(stats, areCloseTo(new TimingStats(JOB_ID, 1, 3.0, 3.0, 3.0, 3.0, createContext(3.0)), 1e-9));

        stats.updateStats(2);
        assertThat(stats, areCloseTo(new TimingStats(JOB_ID, 2, 2.0, 3.0, 2.5, 2.99, createContext(5.0)), 1e-9));

        stats.updateStats(4);
        assertThat(stats, areCloseTo(new TimingStats(JOB_ID, 3, 2.0, 4.0, 3.0, 3.0001, createContext(9.0)), 1e-9));

        stats.updateStats(1);
        assertThat(stats, areCloseTo(new TimingStats(JOB_ID, 4, 1.0, 4.0, 2.5, 2.980099, createContext(10.0)), 1e-9));

        stats.updateStats(5);
        assertThat(stats, areCloseTo(new TimingStats(JOB_ID, 5, 1.0, 5.0, 3.0, 3.00029801, createContext(15.0)), 1e-9));
    }

    private static ExponentialAverageCalculationContext createContext(double incrementalSearchTimeMs) {
        return new ExponentialAverageCalculationContext(incrementalSearchTimeMs, null, null);
    }

    public void testTotalBucketProcessingTimeIsCalculatedProperlyAfterUpdates() {
        TimingStats stats = new TimingStats(JOB_ID);
        assertThat(stats.getTotalBucketProcessingTimeMs(), equalTo(0.0));

        stats.updateStats(3);
        assertThat(stats.getTotalBucketProcessingTimeMs(), equalTo(3.0));

        stats.updateStats(2);
        assertThat(stats.getTotalBucketProcessingTimeMs(), equalTo(5.0));

        stats.updateStats(4);
        assertThat(stats.getTotalBucketProcessingTimeMs(), equalTo(9.0));

        stats.updateStats(1);
        assertThat(stats.getTotalBucketProcessingTimeMs(), equalTo(10.0));

        stats.updateStats(5);
        assertThat(stats.getTotalBucketProcessingTimeMs(), equalTo(15.0));
    }

    public void testDocumentId() {
        assertThat(TimingStats.documentId("my-job-id"), equalTo("my-job-id_timing_stats"));
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
                    && closeTo(operand.getTotalBucketProcessingTimeMs(), error).matches(item.getTotalBucketProcessingTimeMs())
                    && closeTo(operand.getMinBucketProcessingTimeMs(), error).matches(item.getMinBucketProcessingTimeMs())
                    && closeTo(operand.getMaxBucketProcessingTimeMs(), error).matches(item.getMaxBucketProcessingTimeMs())
                    && closeTo(operand.getAvgBucketProcessingTimeMs(), error).matches(item.getAvgBucketProcessingTimeMs())
                    && closeTo(operand.getExponentialAvgBucketProcessingTimeMs(), error).matches(
                        item.getExponentialAvgBucketProcessingTimeMs()
                    )
                    && closeTo(operand.getExponentialAvgBucketProcessingTimePerHourMs(), error).matches(
                        item.getExponentialAvgBucketProcessingTimePerHourMs()
                    );
            }
        };
    }
}
