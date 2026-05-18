/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.index.MergePolicy;
import org.elasticsearch.index.merge.OnGoingMerge;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.concurrent.atomic.AtomicLong;

public class MergeMetrics {

    public static final String MERGE_SEGMENTS_SIZE = "es.merge.segments.size";
    public static final String MERGE_DOCS_TOTAL = "es.merge.docs.total";
    public static final String MERGE_SEGMENTS_QUEUED_USAGE = "es.merge.segments.queued.usage";
    public static final String MERGE_SEGMENTS_RUNNING_USAGE = "es.merge.segments.running.usage";
    public static final String MERGE_SEGMENTS_MERGED_SIZE = "es.merge.segments.merged.size";
    public static final String MERGE_QUEUED_ESTIMATED_MEMORY_SIZE = "es.merge.segments.memory.size";
    public static final String MERGE_TIME_IN_SECONDS = "es.merge.time";
    public static MergeMetrics NOOP = new MergeMetrics(TelemetryProvider.NOOP.getMeterRegistry());

    private final LongCounter mergeSizeInBytes;
    private final LongCounter mergeMergedSegmentSizeInBytes;
    private final LongCounter mergeNumDocs;
    private final LongHistogram mergeTimeInSeconds;

    private final AtomicLong runningMergeSizeInBytes = new AtomicLong();
    private final AtomicLong queuedMergeSizeInBytes = new AtomicLong();
    private final AtomicLong queuedEstimatedMergeMemoryInBytes = new AtomicLong();

    public MergeMetrics(MeterRegistry meterRegistry) {
        mergeSizeInBytes = meterRegistry.registerLongCounter(MERGE_SEGMENTS_SIZE, "Total size of segments merged", "bytes");
        meterRegistry.registerLongGauge(
            MERGE_SEGMENTS_QUEUED_USAGE,
            "Total usage of segments queued to be merged",
            "bytes",
            () -> new LongWithAttributes(queuedMergeSizeInBytes.get())
        );
        meterRegistry.registerLongGauge(
            MERGE_SEGMENTS_RUNNING_USAGE,
            "Total usage of segments currently being merged",
            "bytes",
            () -> new LongWithAttributes(runningMergeSizeInBytes.get())
        );
        mergeMergedSegmentSizeInBytes = meterRegistry.registerLongCounter(
            MERGE_SEGMENTS_MERGED_SIZE,
            "Total size of the new merged segments",
            "bytes"
        );
        mergeNumDocs = meterRegistry.registerLongCounter(MERGE_DOCS_TOTAL, "Total number of documents merged", "documents");
        mergeTimeInSeconds = meterRegistry.registerLongHistogram(MERGE_TIME_IN_SECONDS, "Merge time in seconds", "seconds");
        meterRegistry.registerLongGauge(
            MERGE_QUEUED_ESTIMATED_MEMORY_SIZE,
            "Estimated memory usage for queued merges",
            "bytes",
            () -> new LongWithAttributes(queuedEstimatedMergeMemoryInBytes.get())
        );
    }

    public void incrementQueuedMergeBytes(OnGoingMerge currentMerge, long estimatedMemorySize) {
        queuedMergeSizeInBytes.getAndAdd(currentMerge.getTotalBytesSize());
        queuedEstimatedMergeMemoryInBytes.getAndAdd(estimatedMemorySize);
    }

    public void moveQueuedMergeBytesToRunning(OnGoingMerge currentMerge, long estimatedMemorySize) {
        long totalSize = currentMerge.getTotalBytesSize();
        queuedMergeSizeInBytes.getAndAdd(-totalSize);
        runningMergeSizeInBytes.getAndAdd(totalSize);
        queuedEstimatedMergeMemoryInBytes.getAndAdd(-estimatedMemorySize);
    }

    public void decrementRunningMergeBytes(OnGoingMerge currentMerge) {
        runningMergeSizeInBytes.getAndAdd(-currentMerge.getTotalBytesSize());
    }

    public void markMergeMetrics(MergePolicy.OneMerge currentMerge, long mergedSegmentSize, long tookMillis) {
        mergeSizeInBytes.incrementBy(currentMerge.totalBytesSize());
        mergeMergedSegmentSizeInBytes.incrementBy(mergedSegmentSize);
        mergeNumDocs.incrementBy(currentMerge.totalNumDocs());
        mergeTimeInSeconds.record(tookMillis / 1000);
    }

    public long getQueuedMergeSizeInBytes() {
        return queuedMergeSizeInBytes.get();
    }

    public long getRunningMergeSizeInBytes() {
        return runningMergeSizeInBytes.get();
    }
}
