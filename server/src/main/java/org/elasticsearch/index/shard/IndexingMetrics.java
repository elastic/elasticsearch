/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.telemetry.metric.MetricAttributes;

import java.util.Map;

/**
 * Node-wide APM instruments for bulk execution on primary shards. Only primaries record, and the latency histogram additionally skips a
 * primary that is a relocation target. Every bulk item that completes on the primary is counted, including deletes and items that fail.
 * Not counted, by construction of the hook in {@link org.elasticsearch.action.bulk.BulkPrimaryExecutionContext}: CCR follower replication,
 * which bypasses the shard bulk action and is not client ingest; whole-request indexing pressure rejections; and items aborted by request
 * filters before they reach the shard.
 */
public class IndexingMetrics {

    public static final String INDEXING_DURATION_HISTOGRAM = "es.indexing.shards.duration.histogram";
    public static final String INDEXING_OPERATIONS_TOTAL = "es.indexing.shards.operations.total";
    public static final String INDEXING_FAILURE_TOTAL = "es.indexing.shards.failure.total";

    private final DoubleHistogram bulkDurationInSeconds;
    private final LongCounter operations;
    private final LongCounter failures;

    public IndexingMetrics(MeterRegistry meterRegistry) {
        // seconds rather than milliseconds: the default bucket ladder spans 2^-8 to 2^17, so in seconds it resolves ~4 ms at the low end
        // and reaches ~36 h at the top, whereas in milliseconds everything above ~2.2 min collapses into the overflow bucket
        bulkDurationInSeconds = meterRegistry.registerDoubleHistogram(
            INDEXING_DURATION_HISTOGRAM,
            "Time to execute a bulk request on a primary shard in seconds",
            "s"
        );
        operations = meterRegistry.registerLongCounter(
            INDEXING_OPERATIONS_TOTAL,
            "Number of bulk items (index, create, update, delete) completed on a primary, successful or not",
            "unit"
        );
        failures = meterRegistry.registerLongCounter(INDEXING_FAILURE_TOTAL, "Number of bulk items that failed on a primary", "unit");
    }

    public void onBulk(IndexMode indexMode, long tookInNanos) {
        bulkDurationInSeconds.record(tookInNanos / 1_000_000_000.0, Map.of(MetricAttributes.ES_INDEX_MODE, indexMode.getName()));
    }

    /** Counts the bulk items, successful or not, that completed within one shard bulk request. */
    public void onBulkItemsCompleted(IndexMode indexMode, int count) {
        operations.incrementBy(count, Map.of(MetricAttributes.ES_INDEX_MODE, indexMode.getName()));
    }

    /** Counts one bulk item that failed with its final error. */
    public void onBulkItemFailed(IndexMode indexMode, Exception failure) {
        failures.incrementBy(
            1,
            Map.of(MetricAttributes.ES_INDEX_MODE, indexMode.getName(), MetricAttributes.ERROR_TYPE, MetricAttributes.errorType(failure))
        );
    }
}
