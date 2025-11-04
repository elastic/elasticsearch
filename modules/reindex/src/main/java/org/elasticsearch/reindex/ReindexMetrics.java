/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

public class ReindexMetrics {

    public static final String REINDEX_TIME_HISTOGRAM = "es.reindex.duration.histogram";
    // metrics for remote reindex should be a subset of the all metrics
    public static final String REINDEX_TIME_HISTOGRAM_REMOTE = "es.reindex.duration.histogram.remote";
    public static final String REINDEX_SUCCESS_HISTOGRAM = "es.reindex.completion.success";
    public static final String REINDEX_SUCCESS_HISTOGRAM_REMOTE = "es.reindex.completion.success.remote";
    public static final String REINDEX_FAILURE_HISTOGRAM = "es.reindex.completion.failure";
    public static final String REINDEX_FAILURE_HISTOGRAM_REMOTE = "es.reindex.completion.failure.remote";

    private final LongHistogram reindexTimeSecsHistogram;
    private final LongHistogram reindexTimeSecsHistogramRemote;
    private final LongHistogram reindexSuccessHistogram;
    private final LongHistogram reindexSuccessHistogramRemote;
    private final LongHistogram reindexFailureHistogram;
    private final LongHistogram reindexFailureHistogramRemote;

    public ReindexMetrics(MeterRegistry meterRegistry) {
        this.reindexTimeSecsHistogram = meterRegistry.registerLongHistogram(REINDEX_TIME_HISTOGRAM, "Time to reindex by search", "seconds");
        this.reindexTimeSecsHistogramRemote = meterRegistry.registerLongHistogram(
            REINDEX_TIME_HISTOGRAM_REMOTE,
            "Time to reindex by search from remote cluster",
            "seconds"
        );

        this.reindexSuccessHistogram = meterRegistry.registerLongHistogram(
            REINDEX_SUCCESS_HISTOGRAM,
            "Number of successful reindex",
            "unit"
        );
        this.reindexSuccessHistogramRemote = meterRegistry.registerLongHistogram(
            REINDEX_SUCCESS_HISTOGRAM_REMOTE,
            "Number of successful reindex from remote cluster",
            "unit"
        );

        this.reindexFailureHistogram = meterRegistry.registerLongHistogram(REINDEX_FAILURE_HISTOGRAM, "Number of failed reindex", "unit");
        this.reindexFailureHistogramRemote = meterRegistry.registerLongHistogram(
            REINDEX_FAILURE_HISTOGRAM_REMOTE,
            "Number of failed reindex from remote cluster",
            "unit"
        );
    }

    public long recordTookTime(long tookTime, boolean remote) {
        reindexTimeSecsHistogram.record(tookTime);
        if (remote) {
            reindexTimeSecsHistogramRemote.record(tookTime);
        }
        return tookTime;
    }

    public void recordSuccess(boolean remote) {
        reindexSuccessHistogram.record(1L);
        if (remote) {
            reindexSuccessHistogramRemote.record(1L);
        }
    }

    public void recordFailure(boolean remote) {
        reindexFailureHistogram.record(1L);
        if (remote) {
            reindexFailureHistogramRemote.record(1L);
        }
    }
}
