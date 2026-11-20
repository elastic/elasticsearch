/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import static org.elasticsearch.core.Strings.format;

/**
 * Outcome of a {@link BatchedCompoundCommitUploadTask}, with the timing of the attempt that uploaded the commit.
 *
 * @param batchedCompoundCommit the uploaded commit
 * @param totalUploadTimeMs     the time from the creation of the upload task until the commit was uploaded, including the failed
 *                              attempts, the delays between them and, on the first attempt, the cache warming
 * @param timing                the timing of the successful attempt
 */
public record BccUploadResult(BatchedCompoundCommit batchedCompoundCommit, long totalUploadTimeMs, BccUploadAttemptTiming timing) {

    /**
     * Timing of the upload attempt that reached the object store and succeeded.
     *
     * @param attempt                1-based attempt number within the upload task, which is the number of attempts of the upload
     * @param generationQueueWaitMs  time waiting for earlier BCC uploads on the same shard before calling the object store
     * @param objectStoreQueueWaitMs time waiting in the object-store upload task runner before IO starts
     * @param uploadIoMs             time spent in the object-store upload IO
     * @param bccSizeBytes           total VBCC size uploaded
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
}
