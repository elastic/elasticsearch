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

    public final static long UNASSIGNED_SEQ_NO = -2L;
    public final static long NO_OPS_PERFORMED = -1L;
    final LocalCheckpointService localCheckpointService;
    final GlobalCheckpointService globalCheckpointService;

    public SequenceNumbersService(ShardId shardId, IndexSettings indexSettings) {
        super(shardId, indexSettings);
        localCheckpointService = new LocalCheckpointService(shardId, indexSettings);
        globalCheckpointService = new GlobalCheckpointService(shardId, indexSettings);
    }

    /**
     * generates a new sequence number.
     * Note: you must call {@link #markSeqNoAsCompleted(long)} after the operation for which this seq# was generated
     * was completed (whether successfully or with a failure)
     */
    public long generateSeqNo() {
        return localCheckpointService.generateSeqNo();
    }

    /**
     * marks the given seqNo as completed. See {@link LocalCheckpointService#markSeqNoAsCompleted(long)}
     * more details
     */
    public void markSeqNoAsCompleted(long seqNo) {
        localCheckpointService.markSeqNoAsCompleted(seqNo);
    }

    /**
     * Gets sequence number related stats
     */
    public SeqNoStats stats() {
        return new SeqNoStats(localCheckpointService.getMaxSeqNo(), localCheckpointService.getCheckpoint(),
            globalCheckpointService.getCheckpoint());
    }

    /**
     * notifies the service of a local checkpoint.
     * see {@link GlobalCheckpointService#updateLocalCheckpoint(String, long)} for details.
     */
    public void updateLocalCheckpointForShard(String allocationId, long checkpoint) {
        globalCheckpointService.updateLocalCheckpoint(allocationId, checkpoint);
    }

    /**
     * marks the allocationId as "in sync" with the primary shard.
     * see {@link GlobalCheckpointService#markAllocationIdAsInSync(String, long)} for details.
     *
     * @param allocationId    allocationId of the recovering shard
     * @param localCheckpoint the local checkpoint of the shard in question
     */
    public void markAllocationIdAsInSync(String allocationId, long localCheckpoint) {
        globalCheckpointService.markAllocationIdAsInSync(allocationId, localCheckpoint);
    }

    public long getLocalCheckpoint() {
        return localCheckpointService.getCheckpoint();
    }

    public long getGlobalCheckpoint() {
        return globalCheckpointService.getCheckpoint();
    }

    /**
     * updates the global checkpoint on a replica shard (after it has been updated by the primary).
     */
    public void updateGlobalCheckpointOnReplica(long checkpoint) {
        globalCheckpointService.updateCheckpointOnReplica(checkpoint);
    }

    /**
     * Notifies the service of the current allocation ids in the cluster state.
     * see {@link GlobalCheckpointService#updateAllocationIdsFromMaster(Set, Set)} for details.
     *
     * @param activeAllocationIds       the allocation ids of the currently active shard copies
     * @param initializingAllocationIds the allocation ids of the currently initializing shard copies
     */
    public void updateAllocationIdsFromMaster(Set<String> activeAllocationIds, Set<String> initializingAllocationIds) {
        globalCheckpointService.updateAllocationIdsFromMaster(activeAllocationIds, initializingAllocationIds);
    }

    /**
     * Scans through the currently known local checkpoint and updates the global check point accordingly.
     *
     * @return true if the checkpoint has been updated or if it can not be update since one of the local checkpoint
     * of one of the active allocations is not known.
     */
    public boolean updateGlobalCheckpointOnPrimary() {
        return globalCheckpointService.updateCheckpointOnPrimary();
    }
}
