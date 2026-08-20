/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits.metering;

import org.elasticsearch.common.ExponentiallyWeightedMovingAverage;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.DoubleGauge;
import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.DoubleWithAttributes;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit.TimestampFieldValueRange;

import java.util.Iterator;
import java.util.Map;
import java.util.OptionalDouble;

/// Collects metrics for BCC uploads in stateless deployments.
public class BccUploadMetricsCollector {

    public static final String BCC_TOTAL_SIZE_HISTOGRAM_METRIC = "es.bcc.total_size_in_megabytes.histogram";
    public static final String BCC_NUMBER_COMMITS_HISTOGRAM_METRIC = "es.bcc.number_of_commits.histogram";
    public static final String BCC_ELAPSED_TIME_BEFORE_FREEZE_HISTOGRAM_METRIC = "es.bcc.elapsed_time_before_freeze.histogram";
    public static final String BCC_TIMESTAMP_RANGE_HISTOGRAM_METRIC = "es.bcc.timestamp_range.histogram";
    public static final String BCC_MISSING_TIMESTAMP_METRIC = "es.bcc.missing_timestamp.total";
    public static final String BCC_AVERAGE_COMMIT_UPLOAD_THROUGHPUT_METRIC = "es.bcc.average_upload_throughput.current";
    public static final String BCC_SIZE_ATTRIBUTE_KEY = "es_bcc_size";

    private final LongHistogram bccSizeInMegabytesHistogram;
    private final LongHistogram bccNumberCommitsHistogram;
    private final LongHistogram bccAgeHistogram;
    private final DoubleHistogram bccTimestampRangeHistogram;
    private final LongCounter bccMissingTimestampCounter;
    @SuppressWarnings("unused")
    private final DoubleGauge averageCommitUploadThroughputGauge;
    /// `alpha` determines how much older data points influence the average, a value of 1 means that the mean is equal
    /// to the latest data point.
    /// We use a slight recency biased value.
    private final ExponentiallyWeightedMovingAverage commitUploadThroughputMiBSec;

    public BccUploadMetricsCollector(final TelemetryProvider telemetryProvider, final ByteSizeValue initialThroughput) {
        this.commitUploadThroughputMiBSec = new ExponentiallyWeightedMovingAverage(0.6, initialThroughput.getBytes());
        final var meterRegistry = telemetryProvider.getMeterRegistry();
        this.bccSizeInMegabytesHistogram = meterRegistry.registerLongHistogram(
            BCC_TOTAL_SIZE_HISTOGRAM_METRIC,
            "Histogram for total size in megabytes of batched compound commits",
            "megabytes"
        );
        this.bccNumberCommitsHistogram = meterRegistry.registerLongHistogram(
            BCC_NUMBER_COMMITS_HISTOGRAM_METRIC,
            "Histogram for number of commits per batched compound commit",
            "unit"
        );
        this.bccAgeHistogram = meterRegistry.registerLongHistogram(
            BCC_ELAPSED_TIME_BEFORE_FREEZE_HISTOGRAM_METRIC,
            "Histogram for elapsed time in milliseconds of batched compound commits before freezing",
            "ms"
        );
        this.bccTimestampRangeHistogram = meterRegistry.registerDoubleHistogram(
            BCC_TIMESTAMP_RANGE_HISTOGRAM_METRIC,
            "Span of the max minus min @timestamp range of uploaded batched compound commits, in minutes, "
                + "broken down by the ["
                + BCC_SIZE_ATTRIBUTE_KEY
                + "] size bucket",
            "minutes"
        );
        this.bccMissingTimestampCounter = meterRegistry.registerLongCounter(
            BCC_MISSING_TIMESTAMP_METRIC,
            "Number of uploaded batched compound commits where none of the compound commits have a @timestamp range",
            "count"
        );
        this.averageCommitUploadThroughputGauge = meterRegistry.registerDoubleGauge(
            BCC_AVERAGE_COMMIT_UPLOAD_THROUGHPUT_METRIC,
            "moving average of batch compound commit upload throughput",
            "MiB/s",
            () -> new DoubleWithAttributes(commitUploadThroughputMiBSec.getAverage())
        );
    }

    /// Records all BCC upload metrics: size, number of commits, age before freeze, and timestamp range.
    public void recordBccUpload(long totalSizeInBytes, int numCommits, long ageMillis, Iterator<TimestampFieldValueRange> timestampRanges) {
        bccSizeInMegabytesHistogram.record(ByteSizeUnit.BYTES.toMB(totalSizeInBytes));
        bccNumberCommitsHistogram.record(numCommits);
        bccAgeHistogram.record(ageMillis);
        bccTimestampSpanMinutes(timestampRanges).ifPresentOrElse(
            spanMinutes -> bccTimestampRangeHistogram.record(spanMinutes, Map.of(BCC_SIZE_ATTRIBUTE_KEY, bccSizeBucket(totalSizeInBytes))),
            bccMissingTimestampCounter::increment
        );
    }

    /// Updates the moving average with a new upload throughput sample.
    public void recordUploadThroughput(final double throughputMiBPerSec) {
        commitUploadThroughputMiBSec.addValue(throughputMiBPerSec);
    }

    public double getAverageUploadThroughputMiBSec() {
        return commitUploadThroughputMiBSec.getAverage();
    }

    public static String bccSizeBucket(final long totalSizeBytes) {
        assert totalSizeBytes > 0 : "was " + totalSizeBytes;
        if (totalSizeBytes <= ByteSizeUnit.MB.toBytes(16)) {
            return "<=16MiB";
        } else if (totalSizeBytes <= ByteSizeUnit.MB.toBytes(64)) {
            return "<=64MiB";
        } else if (totalSizeBytes <= ByteSizeUnit.MB.toBytes(256)) {
            return "<=256MiB";
        } else {
            return ">256MiB";
        }
    }

    /// Calculates the span, in minutes, between the minimum and maximum timestamps
    /// present in a collection of [TimestampFieldValueRange] objects.
    /// If the collection contains no valid timestamp range entries, an empty `OptionalDouble` is returned.
    ///
    /// @param ranges an `Iterator` of `TimestampFieldValueRange` objects, where each range
    ///               specifies a minimum and maximum timestamp in milliseconds.
    ///               Null entries in the iterator are skipped.
    /// @return an `OptionalDouble` containing the timestamp span in minutes if valid ranges are
    ///         found, or an empty `OptionalDouble` if none are present.
    public static OptionalDouble bccTimestampSpanMinutes(final Iterator<TimestampFieldValueRange> ranges) {
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        boolean any = false;
        while (ranges.hasNext()) {
            final TimestampFieldValueRange range = ranges.next();
            if (range == null) {
                continue;
            }
            any = true;
            min = Math.min(min, range.minMillis());
            max = Math.max(max, range.maxMillis());
        }
        if (any == false) {
            return OptionalDouble.empty();
        }
        // Subtract in double space so that for astronomically large spans we lose precision rather than overflow a Long.
        return OptionalDouble.of(((double) max - (double) min) / 60_000d);
    }
}
