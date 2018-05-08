/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.translog.Translog;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

/**
 * A policy that controls how many soft-deleted documents should be retained for peer-recovery and querying changes purpose.
 */
final class SoftDeletesPolicy {
    private final LongSupplier globalCheckpointSupplier;
    private int retentionNumLocks = 0;
    private long checkpointOfSafeCommit;
    private volatile long minRequiredSeqNoForRecovery;
    private volatile long retentionOperations;

    SoftDeletesPolicy(LongSupplier globalCheckpointSupplier, long retentionOperations) {
        this.globalCheckpointSupplier = globalCheckpointSupplier;
        this.retentionOperations = retentionOperations;
        this.checkpointOfSafeCommit = SequenceNumbers.NO_OPS_PERFORMED;
        this.minRequiredSeqNoForRecovery = checkpointOfSafeCommit + 1;
    }

    /**
     * Updates the number of soft-deleted prior to the global checkpoint to be retained
     * See {@link org.elasticsearch.index.IndexSettings#INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING}
     */
    void setRetentionOperations(long retentionOperations) {
        this.retentionOperations = retentionOperations;
    }

    /**
     * Sets the local checkpoint of the current safe commit.
     * All operations whose seqno are greater than this checkpoint will be retained until the new checkpoint is advanced.
     */
    synchronized void setCheckpointOfSafeCommit(long newCheckpoint) {
        if (newCheckpoint < this.checkpointOfSafeCommit) {
            throw new IllegalArgumentException("Local checkpoint can't go backwards; " +
                "new checkpoint [" + newCheckpoint + "]," + "current checkpoint [" + checkpointOfSafeCommit + "]");
        }
        this.checkpointOfSafeCommit = newCheckpoint;
        updateMinRequiredSeqNoForRecovery();
    }

    private void updateMinRequiredSeqNoForRecovery() {
        assert Thread.holdsLock(this) : Thread.currentThread().getName();
        if (retentionNumLocks == 0) {
            // Need to keep all operations after the local checkpoint of the safe commit for recovery purpose.
            this.minRequiredSeqNoForRecovery = checkpointOfSafeCommit + 1;
        }
    }

    /**
     * Acquires a lock on soft-deleted documents to prevent them from cleaning up in merge processes. This is necessary to
     * make sure that all operations after the local checkpoint of the safe commit are retained until the lock is released.
     * This is a analogy to the translog's retention lock; see {@link Translog#acquireRetentionLock()}
     */
    synchronized Releasable acquireRetentionLock() {
        retentionNumLocks++;
        assert retentionNumLocks > 0 : "Invalid number of retention locks [" + retentionNumLocks + "]";
        final AtomicBoolean released = new AtomicBoolean();
        return () -> {
            if (released.compareAndSet(false, true)) {
                releaseRetentionLock();
            }
        };
    }

    private synchronized void releaseRetentionLock() {
        retentionNumLocks--;
        assert retentionNumLocks >= 0 : "Invalid number of retention locks [" + retentionNumLocks + "]";
        updateMinRequiredSeqNoForRecovery();
    }

    /**
     * Returns a soft-deletes retention query that will be used in {@link org.apache.lucene.index.SoftDeletesRetentionMergePolicy}
     * Documents including tombstones are soft-deleted and matched this query will be retained and won't cleaned up by merges.
     */
    Query retentionQuery() {
        return LongPoint.newRangeQuery(SeqNoFieldMapper.NAME, getMinSeqNoToRetain(), Long.MAX_VALUE);
    }

    // Package-level for testing
    long getMinSeqNoToRetain() {
        final long minSeqNoForQueryingChanges = globalCheckpointSupplier.getAsLong() + 1 - retentionOperations;
        return Math.min(minRequiredSeqNoForRecovery, minSeqNoForQueryingChanges);
    }
}
