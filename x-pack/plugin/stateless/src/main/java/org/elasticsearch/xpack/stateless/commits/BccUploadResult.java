/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.util.List;

import static org.elasticsearch.core.Strings.format;

/**
 * Outcome of a {@link BatchedCompoundCommitUploadTask}, including per-attempt upload phase timings.
 */
public record BccUploadResult(BatchedCompoundCommit batchedCompoundCommit, long totalUploadTimeMs, List<BccUploadAttemptTiming> attempts) {

    public int uploadAttempts() {
        return attempts.size();
    }

    public long totalGenerationQueueWaitMs() {
        return attempts.stream().mapToLong(BccUploadAttemptTiming::generationQueueWaitMs).sum();
    }

    public long totalObjectStoreQueueWaitMs() {
        return attempts.stream().mapToLong(BccUploadAttemptTiming::objectStoreQueueWaitMs).sum();
    }

    /**
     * Upload throughput in mebibytes per second for the last (successful) attempt.
     */
    public double uploadThroughputMiBPerSec() {
        if (attempts.isEmpty()) {
            return 0.0;
        }
        return attempts.get(attempts.size() - 1).uploadThroughputMiBPerSec();
    }

    /**
     * Timings for one upload attempt (one {@link BatchedCompoundCommitUploadTask#tryAction} that reached the object store).
     *
     * @param attempt                  1-based attempt number within the upload task
     * @param generationQueueWaitMs    time waiting for earlier BCC uploads on the same shard before calling the object store
     * @param objectStoreQueueWaitMs   time waiting in the object-store upload task runner before IO starts
     * @param uploadIoMs               time spent in the object-store upload IO
     * @param bccSizeBytes             total VBCC size uploaded
     */
    public record BccUploadAttemptTiming(
        int attempt,
        long generationQueueWaitMs,
        long objectStoreQueueWaitMs,
        long uploadIoMs,
        long bccSizeBytes
    ) {

        /**
         * Object-store upload throughput in mebibytes per second, derived from {@link #bccSizeBytes} and {@link #uploadIoMs}.
         */
        public double uploadThroughputMiBPerSec() {
            if (uploadIoMs <= 0 || bccSizeBytes <= 0) {
                return 0.0;
            }
            final double mib = ByteSizeUnit.BYTES.toMB(bccSizeBytes);
            return mib / (uploadIoMs / 1000.0);
        }

        public String toLogString() {
            return format(
                "attempt=%d generationQueue=%dms objectStoreQueue=%dms uploadIo=%dms size=%s throughput=%.2fMiB/s",
                attempt,
                generationQueueWaitMs,
                objectStoreQueueWaitMs,
                uploadIoMs,
                ByteSizeValue.ofBytes(bccSizeBytes),
                uploadThroughputMiBPerSec()
            );
        }
    }

    public String attemptsToLogString() {
        return attempts.stream().map(BccUploadAttemptTiming::toLogString).reduce((a, b) -> a + "; " + b).orElse("none");
    }
}
