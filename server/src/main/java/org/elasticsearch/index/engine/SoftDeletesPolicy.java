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

    SoftDeletesPolicy(LongSupplier globalCheckpointSupplier, long minRetainedSeqNo, long retentionOperations) {
        this.globalCheckpointSupplier = globalCheckpointSupplier;
        this.retentionOperations = retentionOperations;
        this.minRetainedSeqNo = minRetainedSeqNo;
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
        // Do not advance if the retention lock is held
        if (retentionLockCount == 0) {
            // This policy retains operations for two purposes: peer-recovery and querying changes history.
            // - Peer-recovery is driven by the local checkpoint of the safe commit. In peer-recovery, the primary transfers a safe commit,
            // then sends ops after the local checkpoint of that commit. This requires keeping all ops after localCheckpointOfSafeCommit;
            // - Changes APIs are driven the combination of the global checkpoint and retention ops. Here we prefer using the global
            // checkpoint instead of max_seqno because only operations up to the global checkpoint are exposed in the the changes APIs.
            final long minSeqNoForQueryingChanges = globalCheckpointSupplier.getAsLong() - retentionOperations;
            final long minSeqNoToRetain = Math.min(minSeqNoForQueryingChanges, localCheckpointOfSafeCommit) + 1;
            // This can go backward as the retentionOperations value can be changed in settings.
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
