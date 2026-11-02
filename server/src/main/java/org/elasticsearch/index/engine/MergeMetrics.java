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
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.merge.OnGoingMerge;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.telemetry.metric.MetricAttributes;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class MergeMetrics {

    public static final String MERGE_SEGMENTS_SIZE = "es.merge.segments.size";
    public static final String MERGE_DOCS_TOTAL = "es.merge.docs.total";
    public static final String MERGE_SEGMENTS_QUEUED_USAGE = "es.merge.segments.queued.usage";
    public static final String MERGE_SEGMENTS_RUNNING_USAGE = "es.merge.segments.running.usage";
    public static final String MERGE_SEGMENTS_MERGED_SIZE = "es.merge.segments.merged.size";
    public static final String MERGE_QUEUED_ESTIMATED_MEMORY_SIZE = "es.merge.segments.memory.size";
    public static final String MERGE_TIME_IN_SECONDS = "es.merge.time";
    public static final String MERGE_FAILURE_TOTAL = "es.merge.failure.total";
    public static final String MERGE_FAILURE_BLOBSTORE_TRANSIENT_TOTAL = "es.merge.failure.blobstore_transient.total";
    public static final String MERGE_ABORTED_TOTAL = "es.merge.aborted.total";
    public static MergeMetrics NOOP = new MergeMetrics(TelemetryProvider.NOOP.getMeterRegistry());

    /**
     * Packages of the S3, GCS and Azure SDKs and of the Elasticsearch clients that wrap them. An exception of a class in one of these
     * packages, or thrown from a class in one of them (e.g. a Lucene {@code AlreadyClosedException} from a closed client), means the
     * merge failed on the blob store and not on the merge itself.
     */
    static final List<String> BLOB_STORE_CLIENT_PACKAGES = List.of(
        "software.amazon.awssdk.",
        "com.google.cloud.",
        "com.azure.",
        "org.elasticsearch.repositories.s3.",
        "org.elasticsearch.repositories.gcs.",
        "org.elasticsearch.repositories.azure."
    );

    private final LongCounter mergeSizeInBytes;
    private final LongCounter mergeMergedSegmentSizeInBytes;
    private final LongCounter mergeNumDocs;
    private final LongHistogram mergeTimeInSeconds;
    private final LongCounter mergeFailures;
    private final LongCounter mergeBlobStoreTransientFailures;
    private final LongCounter mergeAborts;

    private final AtomicLong runningMergeSizeInBytes = new AtomicLong();
    private final AtomicLong queuedMergeSizeInBytes = new AtomicLong();
    private final AtomicLong queuedEstimatedMergeMemoryInBytes = new AtomicLong();

    public MergeMetrics(MeterRegistry meterRegistry) {
        mergeSizeInBytes = meterRegistry.registerLongCounter(MERGE_SEGMENTS_SIZE, "Total size of segments merged", "bytes");
        meterRegistry.registerLongAsyncGauge(
            MERGE_SEGMENTS_QUEUED_USAGE,
            "Total usage of segments queued to be merged",
            "bytes",
            queuedMergeSizeInBytes::get
        );
        meterRegistry.registerLongAsyncGauge(
            MERGE_SEGMENTS_RUNNING_USAGE,
            "Total usage of segments currently being merged",
            "bytes",
            runningMergeSizeInBytes::get
        );
        mergeMergedSegmentSizeInBytes = meterRegistry.registerLongCounter(
            MERGE_SEGMENTS_MERGED_SIZE,
            "Total size of the new merged segments",
            "bytes"
        );
        mergeNumDocs = meterRegistry.registerLongCounter(MERGE_DOCS_TOTAL, "Total number of documents merged", "documents");
        mergeTimeInSeconds = meterRegistry.registerLongHistogram(MERGE_TIME_IN_SECONDS, "Merge time in seconds", "seconds");
        mergeFailures = meterRegistry.registerLongCounter(
            MERGE_FAILURE_TOTAL,
            "Number of merges that failed with an exception after they started",
            "unit"
        );
        mergeBlobStoreTransientFailures = meterRegistry.registerLongCounter(
            MERGE_FAILURE_BLOBSTORE_TRANSIENT_TOTAL,
            "Number of merges that failed because the blob store was unreachable or closed",
            "unit"
        );
        mergeAborts = meterRegistry.registerLongCounter(
            MERGE_ABORTED_TOTAL,
            "Number of merges that were aborted after they started",
            "unit"
        );
        meterRegistry.registerLongAsyncGauge(
            MERGE_QUEUED_ESTIMATED_MEMORY_SIZE,
            "Estimated memory usage for queued merges",
            "bytes",
            queuedEstimatedMergeMemoryInBytes::get
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

    public void onFailure(IndexMode indexMode, Throwable error) {
        Map<String, Object> attributes = Map.of(
            MetricAttributes.ES_INDEX_MODE,
            indexMode.getName(),
            MetricAttributes.ERROR_TYPE,
            MetricAttributes.errorType(error)
        );
        mergeFailures.incrementBy(1, attributes);
        if (ExceptionsHelper.unwrapCausesAndSuppressed(error, MergeMetrics::isFromBlobStoreClient).isPresent()) {
            mergeBlobStoreTransientFailures.incrementBy(1, attributes);
        }
    }

    private static boolean isFromBlobStoreClient(Throwable t) {
        StackTraceElement[] stackTrace = t.getStackTrace();
        String thrownFrom = stackTrace.length > 0 ? stackTrace[0].getClassName() : "";
        String exceptionClass = t.getClass().getName();
        for (String pkg : BLOB_STORE_CLIENT_PACKAGES) {
            if (exceptionClass.startsWith(pkg) || thrownFrom.startsWith(pkg)) {
                return true;
            }
        }
        return false;
    }

    public void onAborted(IndexMode indexMode) {
        mergeAborts.incrementBy(1, Map.of(MetricAttributes.ES_INDEX_MODE, indexMode.getName()));
    }

    public long getQueuedMergeSizeInBytes() {
        return queuedMergeSizeInBytes.get();
    }

    public long getRunningMergeSizeInBytes() {
        return runningMergeSizeInBytes.get();
    }
}
