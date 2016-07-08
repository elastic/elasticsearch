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
 * a very light weight implementation. will be replaced with proper machinery later
 */
public class SequenceNumbersService extends AbstractIndexShardComponent {

    public static final long UNASSIGNED_SEQ_NO = -2L;

    /**
     * Represents no operations have been performed on the shard.
     */
    public static final long NO_OPS_PERFORMED = -1L;

    final LocalCheckpointTracker localCheckpointTracker;
    final GlobalCheckpointTracker globalCheckpointTracker;

    final Runnable onGlobalpointUpdate;

    /**
     * Initialize the sequence number service. The {@code maxSeqNo}
     * should be set to the last sequence number assigned by this
     * shard, or {@link SequenceNumbersService#NO_OPS_PERFORMED},
     * {@code localCheckpoint} should be set to the last known local
     * checkpoint for this shard, or
     * {@link SequenceNumbersService#NO_OPS_PERFORMED}, and
     * {@code globalCheckpoint} should be set to the last known global
     * checkpoint for this shard, or
     * {@link SequenceNumbersService#UNASSIGNED_SEQ_NO}.
     *  @param shardId          the shard this service is providing tracking
     *                         local checkpoints for
     * @param indexSettings    the index settings
     * @param maxSeqNo         the last sequence number assigned by this
 *                         shard, or
 *                         {@link SequenceNumbersService#NO_OPS_PERFORMED}
     * @param localCheckpoint  the last known local checkpoint for this shard,
*                         or {@link SequenceNumbersService#NO_OPS_PERFORMED}
     * @param globalCheckpoint the last known global checkpoint for this shard,
*                         or {@link SequenceNumbersService#UNASSIGNED_SEQ_NO}
     * @param onGlobalCheckpointUpdate a call back to call when the global check point is updated.
     *                                 Note that this may be called at other times as well
     *
     */
    public SequenceNumbersService(
        final ShardId shardId,
        final IndexSettings indexSettings,
        final long maxSeqNo,
        final long localCheckpoint,
        final long globalCheckpoint, Runnable onGlobalCheckpointUpdate) {
        super(shardId, indexSettings);
        this.onGlobalpointUpdate = onGlobalCheckpointUpdate;
        localCheckpointTracker = new LocalCheckpointTracker(indexSettings, maxSeqNo, localCheckpoint);
        globalCheckpointTracker = new GlobalCheckpointTracker(indexSettings, globalCheckpoint, logger);
    }

    /**
     * generates a new sequence number.
     * Note: you must call {@link #markSeqNoAsCompleted(long)} after the operation for which this seq# was generated
     * was completed (whether successfully or with a failure)
     */
    public long generateSeqNo() {
        return localCheckpointTracker.generateSeqNo();
    }

    /**
     * marks the given seqNo as completed. See {@link LocalCheckpointTracker#markSeqNoAsCompleted(long)}
     * more details
     */
    public void markSeqNoAsCompleted(long seqNo) {
        localCheckpointTracker.markSeqNoAsCompleted(seqNo);
    }

    /**
     * Gets sequence number related stats
     */
    public SeqNoStats stats() {
        return new SeqNoStats(localCheckpointTracker.getMaxSeqNo(), localCheckpointTracker.getCheckpoint(),
            globalCheckpointTracker.getCheckpoint());
    }

    /**
     * notifies the service of a local checkpoint.
     * see {@link GlobalCheckpointTracker#updateLocalCheckpoint(String, long)} for details.
     */
    public void updateLocalCheckpointForShard(String allocationId, long checkpoint) {
        globalCheckpointTracker.updateLocalCheckpoint(allocationId, checkpoint);
    }

    /**
     * marks the allocationId as "in sync" with the primary shard.
     * see {@link GlobalCheckpointTracker#markAllocationIdAsInSync(String, long)} for details.
     *
     * @param allocationId    allocationId of the recovering shard
     * @param localCheckpoint the local checkpoint of the shard in question
     */
    public void markAllocationIdAsInSync(String allocationId, long localCheckpoint) {
        globalCheckpointTracker.markAllocationIdAsInSync(allocationId, localCheckpoint);
    }

    public long getLocalCheckpoint() {
        return localCheckpointTracker.getCheckpoint();
    }

    public long getGlobalCheckpoint() {
        return globalCheckpointTracker.getCheckpoint();
    }

    /**
     * updates the global checkpoint on a replica shard (after it has been updated by the primary).
     */
    public void updateGlobalCheckpointOnReplica(long checkpoint) {
        globalCheckpointTracker.updateCheckpointOnReplica(checkpoint);
        onGlobalpointUpdate.run();
    }

    /**
     * Notifies the service of the current allocation ids in the cluster state.
     * see {@link GlobalCheckpointTracker#updateAllocationIdsFromMaster(Set, Set)} for details.
     *
     * @param activeAllocationIds       the allocation ids of the currently active shard copies
     * @param initializingAllocationIds the allocation ids of the currently initializing shard copies
     */
    public void updateAllocationIdsFromMaster(Set<String> activeAllocationIds, Set<String> initializingAllocationIds) {
        globalCheckpointTracker.updateAllocationIdsFromMaster(activeAllocationIds, initializingAllocationIds);
    }

    /**
     * Scans through the currently known local checkpoint and updates the global checkpoint accordingly.
     *
     * @return true if the checkpoint has been updated or if it can not be updated since one of the local checkpoints
     * of one of the active allocations is not known.
     */
    public boolean updateGlobalCheckpointOnPrimary() {
        boolean maybeUpdate = globalCheckpointTracker.updateCheckpointOnPrimary();
        if (maybeUpdate) {
            onGlobalpointUpdate.run();
        }
        return maybeUpdate;
    }

    /** waits for all operations up to and including the given seq# to complete **/
    public void waitForOpsToComplete(long upToSeqNo) throws InterruptedException {
        localCheckpointTracker.waitForOpsToComplete(upToSeqNo);
    }
}
