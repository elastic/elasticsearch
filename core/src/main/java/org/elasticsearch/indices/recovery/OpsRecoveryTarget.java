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

package org.elasticsearch.indices.recovery;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.TranslogRecoveryPerformer;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.util.List;

/**
 * Represents a recovery where the current node is the target node of the recovery.
 * To track recoveries in a central place, instances of
 * this class are created through {@link RecoveriesCollection}.
 */
public class OpsRecoveryTarget extends RecoveryTarget implements OpsRecoveryTargetHandler {

    /**
     * Creates a new recovery target object that represents a recovery to the provided shard.
     *
     * @param indexShard                        local shard where we want to recover to
     * @param sourceNode                        source node of the recovery where we recover from
     * @param listener                          called when recovery is completed/failed
     */
    public OpsRecoveryTarget(final IndexShard indexShard,
                             final DiscoveryNode sourceNode,
                             final PeerRecoveryTargetService.RecoveryListener listener) {
        super(indexShard, sourceNode, listener);
        assert indexShard.commitStats() != null : "engine should be open";
    }

    @Override
    public OpsRecoveryTarget retryCopy() {
        return new OpsRecoveryTarget(indexShard(), sourceNode(), listener());
    }

    @Override
    public String startRecoveryActionName() {
        return PeerRecoverySourceService.Actions.START_OPS_RECOVERY;
    }

    @Override
    public StartRecoveryRequest createStartRecoveryRequest(Logger logger, DiscoveryNode localNode)
        throws IOException {
            final SeqNoStats seqNoStats = indexShard().seqNoStats();
            // Recovery will start at the first op after the local check point
            final long startSeqNo = seqNoStats.getLocalCheckpoint() + 1;
        return new StartOpsRecoveryRequest(shardId(), sourceNode(), localNode, recoveryId(),
            startSeqNo);
    }

    /**
     * The finalize request refreshes the engine now that new segments are available, enables garbage collection of tombstone files, and
     * updates the global checkpoint.
     *
     * @param globalCheckpoint the global checkpoint on the recovery source
     */
    public void finalizeRecovery(final long globalCheckpoint) {
        indexShard().updateGlobalCheckpointOnReplica(globalCheckpoint);
        final IndexShard indexShard = indexShard();
        indexShard.finalizeRecovery();
    }

    /***
     * @return the allocation id of the target shard.
     */
    public String getTargetAllocationId() {
        return indexShard().routingEntry().allocationId().getId();
    }

    /**
     * Index a set of translog operations on the target
     * @param operations operations to index
     * @param totalTranslogOps current number of total operations expected to be indexed
     */
    public void indexTranslogOperations(List<Translog.Operation> operations, int totalTranslogOps) throws TranslogRecoveryPerformer
            .BatchOperationException {
        final RecoveryState.Translog translog = state().getTranslog();
        translog.totalOperations(totalTranslogOps);
        assert indexShard().recoveryState() == state();
        indexShard().performBatchRecovery(operations);
    }
}
