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
import org.elasticsearch.index.seqno.RetentionLease;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.translog.Translog;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * A policy that controls how many soft-deleted documents should be retained for peer-recovery and querying history changes purpose.
 */
final class SoftDeletesPolicy {
    private final LongSupplier globalCheckpointSupplier;
    private long localCheckpointOfSafeCommit;
    // This lock count is used to prevent `minRetainedSeqNo` from advancing.
    private int retentionLockCount;
    // The extra number of operations before the global checkpoint are retained
    private long retentionOperations;
    // The min seq_no value that is retained - ops after this seq# should exist in the Lucene index.
    private long minRetainedSeqNo;
    // provides the retention leases used to calculate the minimum sequence number to retain
    private final Supplier<RetentionLeases> retentionLeasesSupplier;

    SoftDeletesPolicy(
            final LongSupplier globalCheckpointSupplier,
            final long minRetainedSeqNo,
            final long retentionOperations,
            final Supplier<RetentionLeases> retentionLeasesSupplier) {
        this.globalCheckpointSupplier = globalCheckpointSupplier;
        this.retentionOperations = retentionOperations;
        this.minRetainedSeqNo = minRetainedSeqNo;
        this.retentionLeasesSupplier = Objects.requireNonNull(retentionLeasesSupplier);
        this.localCheckpointOfSafeCommit = SequenceNumbers.NO_OPS_PERFORMED;
        this.retentionLockCount = 0;
    }

    /**
     * Updates the number of soft-deleted documents prior to the global checkpoint to be retained
     * See {@link org.elasticsearch.index.IndexSettings#INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING}
     */
    synchronized void setRetentionOperations(long retentionOperations) {
        this.retentionOperations = retentionOperations;
    }

    /**
     * Sets the local checkpoint of the current safe commit
     */
    synchronized void setLocalCheckpointOfSafeCommit(long newCheckpoint) {
        if (newCheckpoint < this.localCheckpointOfSafeCommit) {
            throw new IllegalArgumentException("Local checkpoint can't go backwards; " +
                "new checkpoint [" + newCheckpoint + "]," + "current checkpoint [" + localCheckpointOfSafeCommit + "]");
        }
        this.localCheckpointOfSafeCommit = newCheckpoint;
    }

    /**
     * Acquires a lock on soft-deleted documents to prevent them from cleaning up in merge processes. This is necessary to
     * make sure that all operations that are being retained will be retained until the lock is released.
     * This is a analogy to the translog's retention lock; see {@link Translog#acquireRetentionLock()}
     */
    synchronized Releasable acquireRetentionLock() {
        assert retentionLockCount >= 0 : "Invalid number of retention locks [" + retentionLockCount + "]";
        retentionLockCount++;
        final AtomicBoolean released = new AtomicBoolean();
        return () -> {
            if (released.compareAndSet(false, true)) {
                releaseRetentionLock();
            }
        };
    }

    private synchronized void releaseRetentionLock() {
        assert retentionLockCount > 0 : "Invalid number of retention locks [" + retentionLockCount + "]";
        retentionLockCount--;
    }

    /**
     * Returns the min seqno that is retained in the Lucene index.
     * Operations whose seq# is least this value should exist in the Lucene index.
     */
    synchronized long getMinRetainedSeqNo() {
        /*
         * When an engine is flushed, we need to provide it the latest collection of retention leases even when the soft deletes policy is
         * locked for peer recovery.
         */
        final RetentionLeases retentionLeases = retentionLeasesSupplier.get();
        // do not advance if the retention lock is held
        if (retentionLockCount == 0) {
            /*
             * This policy retains operations for two purposes: peer-recovery and querying changes history.
             *  - Peer-recovery is driven by the local checkpoint of the safe commit. In peer-recovery, the primary transfers a safe commit,
             *    then sends operations after the local checkpoint of that commit. This requires keeping all ops after
             *    localCheckpointOfSafeCommit.
             *  - Changes APIs are driven by a combination of the global checkpoint, retention operations, and retention leases. Here we
             *    prefer using the global checkpoint instead of the maximum sequence number because only operations up to the global
             *    checkpoint are exposed in the changes APIs.
             */

            // calculate the minimum sequence number to retain based on retention leases
            final long minimumRetainingSequenceNumber = retentionLeases
                    .leases()
                    .stream()
                    .mapToLong(RetentionLease::retainingSequenceNumber)
                    .min()
                    .orElse(Long.MAX_VALUE);
            /*
             * The minimum sequence number to retain is the minimum of the minimum based on retention leases, and the number of operations
             * below the global checkpoint to retain (index.soft_deletes.retention.operations). The additional increments on the global
             * checkpoint and the local checkpoint of the safe commit are due to the fact that we want to retain all operations above
             * those checkpoints.
             */
            final long minSeqNoForQueryingChanges =
                    Math.min(1 + globalCheckpointSupplier.getAsLong() - retentionOperations, minimumRetainingSequenceNumber);
            final long minSeqNoToRetain = Math.min(minSeqNoForQueryingChanges, 1 + localCheckpointOfSafeCommit);

            /*
             * We take the maximum as minSeqNoToRetain can go backward as the retention operations value can be changed in settings, or from
             * the addition of leases with a retaining sequence number lower than previous retaining sequence numbers.
             */
            minRetainedSeqNo = Math.max(minRetainedSeqNo, minSeqNoToRetain);
        }
        return minRetainedSeqNo;
    }

    /**
     * Returns a soft-deletes retention query that will be used in {@link org.apache.lucene.index.SoftDeletesRetentionMergePolicy}
     * Documents including tombstones are soft-deleted and matched this query will be retained and won't cleaned up by merges.
     */
    Query getRetentionQuery() {
        return LongPoint.newRangeQuery(SeqNoFieldMapper.NAME, getMinRetainedSeqNo(), Long.MAX_VALUE);
    }

}
