/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.datafeed;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.utils.ExponentialAverageCalculationContext;
import org.elasticsearch.xpack.core.ml.utils.ExponentialAverageCalculationContextTests;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class DatafeedTimingStatsTests extends AbstractXContentSerializingTestCase<DatafeedTimingStats> {

    private static final String JOB_ID = "my-job-id";

    public static DatafeedTimingStats createRandom() {
        return new DatafeedTimingStats(
            randomAlphaOfLength(10),
            randomLong(),
            randomLong(),
            randomDouble(),
            ExponentialAverageCalculationContextTests.createRandom()
        );
    }

    @Override
    protected DatafeedTimingStats createTestInstance() {
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
    protected ToXContent.Params getToXContentParams() {
        return new ToXContent.MapParams(Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true"));
    }

    @Override
    protected DatafeedTimingStats mutateInstance(DatafeedTimingStats instance) {
        String jobId = instance.getJobId();
        long searchCount = instance.getSearchCount();
        long bucketCount = instance.getBucketCount();
        double totalSearchTimeMs = instance.getTotalSearchTimeMs();
        double incrementalSearchTimeMs = instance.getExponentialAvgCalculationContext().getIncrementalMetricValueMs();
        return new DatafeedTimingStats(
            jobId + randomAlphaOfLength(5),
            searchCount + 2,
            bucketCount + 1,
            totalSearchTimeMs + randomDoubleBetween(1.0, 100.0, true),
            new ExponentialAverageCalculationContext(incrementalSearchTimeMs + randomDoubleBetween(1.0, 100.0, true), null, null)
        );
    }

    public void testParse_OptionalFieldsAbsent() throws IOException {
        String json = "{\"job_id\": \"my-job-id\"}";
        try (
            XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry()), json)
        ) {
            DatafeedTimingStats stats = DatafeedTimingStats.PARSER.apply(parser, null);
            assertThat(stats.getJobId(), equalTo(JOB_ID));
            assertThat(stats.getSearchCount(), equalTo(0L));
            assertThat(stats.getBucketCount(), equalTo(0L));
            assertThat(stats.getTotalSearchTimeMs(), equalTo(0.0));
            assertThat(stats.getAvgSearchTimePerBucketMs(), nullValue());
            assertThat(stats.getExponentialAvgCalculationContext(), equalTo(new ExponentialAverageCalculationContext()));
        }
    }

    public void testConstructor() {
        ExponentialAverageCalculationContext context = new ExponentialAverageCalculationContext(
            78.9,
            Instant.ofEpochMilli(123456789),
            987.0
        );
        DatafeedTimingStats stats = new DatafeedTimingStats(JOB_ID, 5, 10, 123.456, context);
        assertThat(stats.getJobId(), equalTo(JOB_ID));
        assertThat(stats.getSearchCount(), equalTo(5L));
        assertThat(stats.getBucketCount(), equalTo(10L));
        assertThat(stats.getTotalSearchTimeMs(), equalTo(123.456));
        assertThat(stats.getAvgSearchTimePerBucketMs(), closeTo(12.3456, 1e-9));
        assertThat(stats.getExponentialAvgCalculationContext(), equalTo(context));
    }

    public void testDefaultConstructor() {
        DatafeedTimingStats stats = new DatafeedTimingStats(JOB_ID);
        assertThat(stats.getJobId(), equalTo(JOB_ID));
        assertThat(stats.getSearchCount(), equalTo(0L));
        assertThat(stats.getBucketCount(), equalTo(0L));
        assertThat(stats.getTotalSearchTimeMs(), equalTo(0.0));
        assertThat(stats.getAvgSearchTimePerBucketMs(), nullValue());
        assertThat(stats.getExponentialAvgCalculationContext(), equalTo(new ExponentialAverageCalculationContext()));
    }

    public void testCopyConstructor() {
        ExponentialAverageCalculationContext context = new ExponentialAverageCalculationContext(
            78.9,
            Instant.ofEpochMilli(123456789),
            987.0
        );
        DatafeedTimingStats stats1 = new DatafeedTimingStats(JOB_ID, 5, 10, 123.456, context);
        DatafeedTimingStats stats2 = new DatafeedTimingStats(stats1);

        assertThat(stats2.getJobId(), equalTo(JOB_ID));
        assertThat(stats2.getSearchCount(), equalTo(5L));
        assertThat(stats2.getBucketCount(), equalTo(10L));
        assertThat(stats2.getTotalSearchTimeMs(), equalTo(123.456));
        assertThat(stats2.getAvgSearchTimePerBucketMs(), closeTo(12.3456, 1e-9));
        assertThat(stats2.getExponentialAvgCalculationContext(), equalTo(context));
    }

    public void testIncrementTotalSearchTimeMs() {
        DatafeedTimingStats stats = new DatafeedTimingStats(
            JOB_ID,
            5,
            10,
            100.0,
            new ExponentialAverageCalculationContext(50.0, null, null)
        );
        stats.incrementSearchTimeMs(200.0);
        assertThat(stats.getJobId(), equalTo(JOB_ID));
        assertThat(stats.getSearchCount(), equalTo(6L));
        assertThat(stats.getBucketCount(), equalTo(10L));
        assertThat(stats.getTotalSearchTimeMs(), equalTo(300.0));
        assertThat(stats.getAvgSearchTimePerBucketMs(), equalTo(30.0));
        assertThat(stats.getExponentialAvgCalculationContext(), equalTo(new ExponentialAverageCalculationContext(250.0, null, null)));
    }

    public void testIncrementBucketCount() {
        DatafeedTimingStats stats = new DatafeedTimingStats(
            JOB_ID,
            5,
            10,
            100.0,
            new ExponentialAverageCalculationContext(50.0, null, null)
        );
        stats.incrementBucketCount(10);
        assertThat(stats.getJobId(), equalTo(JOB_ID));
        assertThat(stats.getSearchCount(), equalTo(5L));
        assertThat(stats.getBucketCount(), equalTo(20L));
        assertThat(stats.getTotalSearchTimeMs(), equalTo(100.0));
        assertThat(stats.getAvgSearchTimePerBucketMs(), equalTo(5.0));
        assertThat(stats.getExponentialAvgCalculationContext(), equalTo(new ExponentialAverageCalculationContext(50.0, null, null)));
    }

    public void testAvgSearchTimePerBucketIsCalculatedProperlyAfterUpdates() {
        DatafeedTimingStats stats = new DatafeedTimingStats(JOB_ID);
        assertThat(stats.getBucketCount(), equalTo(0L));
        assertThat(stats.getTotalSearchTimeMs(), equalTo(0.0));
        assertThat(stats.getAvgSearchTimePerBucketMs(), nullValue());

        stats.incrementSearchTimeMs(100.0);
        stats.incrementBucketCount(10);
        assertThat(stats.getBucketCount(), equalTo(10L));
        assertThat(stats.getTotalSearchTimeMs(), equalTo(100.0));
        assertThat(stats.getAvgSearchTimePerBucketMs(), equalTo(10.0));

        stats.incrementBucketCount(10);
        assertThat(stats.getBucketCount(), equalTo(20L));
        assertThat(stats.getTotalSearchTimeMs(), equalTo(100.0));
        assertThat(stats.getAvgSearchTimePerBucketMs(), equalTo(5.0));

        stats.incrementSearchTimeMs(200.0);
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
