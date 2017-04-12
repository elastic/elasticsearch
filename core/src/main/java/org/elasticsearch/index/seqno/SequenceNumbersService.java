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

package org.elasticsearch.index.seqno;

import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;

import java.util.Set;

/**
 * Encapsulates the local and global checkpoints into a single service for use as a shard component.
 */
public class SequenceNumbersService extends AbstractIndexShardComponent {

    /**
     * Represents an unassigned sequence number (e.g., can be used on primary operations before they are executed).
     */
    public static final long UNASSIGNED_SEQ_NO = -2L;

    /**
     * Represents no operations have been performed on the shard.
     */
    public static final long NO_OPS_PERFORMED = -1L;

    private final LocalCheckpointTracker localCheckpointTracker;
    private final GlobalCheckpointTracker globalCheckpointTracker;

    /**
     * Initialize the sequence number service. The {@code maxSeqNo} should be set to the last sequence number assigned by this shard, or
     * {@link SequenceNumbersService#NO_OPS_PERFORMED}, {@code localCheckpoint} should be set to the last known local checkpoint for this
     * shard, or {@link SequenceNumbersService#NO_OPS_PERFORMED}, and {@code globalCheckpoint} should be set to the last known global
     * checkpoint for this shard, or {@link SequenceNumbersService#UNASSIGNED_SEQ_NO}.
     *
     * @param shardId          the shard this service is providing tracking local checkpoints for
     * @param indexSettings    the index settings
     * @param maxSeqNo         the last sequence number assigned by this shard, or {@link SequenceNumbersService#NO_OPS_PERFORMED}
     * @param localCheckpoint  the last known local checkpoint for this shard, or {@link SequenceNumbersService#NO_OPS_PERFORMED}
     * @param globalCheckpoint the last known global checkpoint for this shard, or {@link SequenceNumbersService#UNASSIGNED_SEQ_NO}
     */
    public SequenceNumbersService(
        final ShardId shardId,
        final IndexSettings indexSettings,
        final long maxSeqNo,
        final long localCheckpoint,
        final long globalCheckpoint) {
        super(shardId, indexSettings);
        localCheckpointTracker = new LocalCheckpointTracker(indexSettings, maxSeqNo, localCheckpoint);
        globalCheckpointTracker = new GlobalCheckpointTracker(shardId, indexSettings, globalCheckpoint);
    }

    /**
     * Issue the next sequence number. Note that you must call {@link #markSeqNoAsCompleted(long)} after the operation for which the
     * issued sequence number completes (whether or not the operation completes successfully).
     *
     * @return the next assigned sequence number
     */
    public long generateSeqNo() {
        return localCheckpointTracker.generateSeqNo();
    }

    /**
     * The maximum sequence number issued so far. See {@link LocalCheckpointTracker#getMaxSeqNo()} for additional details.
     *
     * @return the maximum sequence number
     */
    public long getMaxSeqNo() {
        return localCheckpointTracker.getMaxSeqNo();
    }

    /**
     * Waits for all operations up to the provided sequence number to complete.
     *
     * @param seqNo the sequence number that the checkpoint must advance to before this method returns
     * @throws InterruptedException if the thread was interrupted while blocking on the condition
     */
    public void waitForOpsToComplete(final long seqNo) throws InterruptedException {
        localCheckpointTracker.waitForOpsToComplete(seqNo);
    }

    /**
     * Marks the processing of the provided sequence number as completed as updates the checkpoint if possible.
     * See {@link LocalCheckpointTracker#markSeqNoAsCompleted(long)} for additional details.
     *
     * @param seqNo the sequence number to mark as completed
     */
    public void markSeqNoAsCompleted(final long seqNo) {
        localCheckpointTracker.markSeqNoAsCompleted(seqNo);
    }

    /**
     * The current sequence number stats.
     *
     * @return stats encapsulating the maximum sequence number, the local checkpoint and the global checkpoint
     */
    public SeqNoStats stats() {
        return localCheckpointTracker.getStats(getGlobalCheckpoint());
    }

    /**
     * Notifies the service to update the local checkpoint for the shard with the provided allocation ID. See
     * {@link GlobalCheckpointTracker#updateLocalCheckpoint(String, long)} for details.
     *
     * @param allocationId the allocation ID of the shard to update the local checkpoint for
     * @param checkpoint   the local checkpoint for the shard
     */
    public void updateLocalCheckpointForShard(final String allocationId, final long checkpoint) {
        globalCheckpointTracker.updateLocalCheckpoint(allocationId, checkpoint);
    }

    /**
     * Marks the shard with the provided allocation ID as in-sync with the primary shard. See
     * {@link GlobalCheckpointTracker#markAllocationIdAsInSync(String)} for additional details.
     *
     * @param allocationId the allocation ID of the shard to mark as in-sync
     */
    public void markAllocationIdAsInSync(final String allocationId) {
        globalCheckpointTracker.markAllocationIdAsInSync(allocationId);
    }

    /**
     * Returns the local checkpoint for the shard.
     *
     * @return the local checkpoint
     */
    public long getLocalCheckpoint() {
        return localCheckpointTracker.getCheckpoint();
    }

    /**
     * Returns the global checkpoint for the shard.
     *
     * @return the global checkpoint
     */
    public long getGlobalCheckpoint() {
        return globalCheckpointTracker.getCheckpoint();
    }

    /**
     * Scans through the currently known local checkpoint and updates the global checkpoint accordingly.
     *
     * @return {@code true} if the checkpoint has been updated or if it can not be updated since one of the local checkpoints of one of the
     * active allocations is not known.
     */
    public boolean updateGlobalCheckpointOnPrimary() {
        return globalCheckpointTracker.updateCheckpointOnPrimary();
    }

    /**
     * Updates the global checkpoint on a replica shard after it has been updated by the primary.
     *
     * @param checkpoint the global checkpoint
     */
    public void updateGlobalCheckpointOnReplica(final long checkpoint) {
        globalCheckpointTracker.updateCheckpointOnReplica(checkpoint);
    }

    /**
     * Notifies the service of the current allocation IDs in the cluster state. See
     * {@link GlobalCheckpointTracker#updateAllocationIdsFromMaster(Set, Set)} for details.
     *
     * @param activeAllocationIds       the allocation IDs of the currently active shard copies
     * @param initializingAllocationIds the allocation IDs of the currently initializing shard copies
     */
    public void updateAllocationIdsFromMaster(final Set<String> activeAllocationIds, final Set<String> initializingAllocationIds) {
        globalCheckpointTracker.updateAllocationIdsFromMaster(activeAllocationIds, initializingAllocationIds);
    }

}
