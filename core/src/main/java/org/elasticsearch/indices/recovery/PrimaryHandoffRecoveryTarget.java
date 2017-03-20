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
import org.elasticsearch.common.util.Callback;
import org.elasticsearch.index.shard.IndexShard;

import java.io.IOException;

/**
 * Represents a recovery where the current node is the target node of the recovery.
 * To track recoveries in a central place, instances of
 * this class are created through {@link RecoveriesCollection}.
 */
public class PrimaryHandoffRecoveryTarget extends RecoveryTarget
    implements PrimaryHandoffRecoveryTargetHandler {

    private final Callback<Long> ensureClusterStateVersionCallback;

    /**
     * Creates a new recovery target object that represents a recovery to the provided shard.
     *
     * @param indexShard                        local shard where we want to recover to
     * @param sourceNode                        source node of the recovery where we recover from
     * @param listener                          called when recovery is completed/failed
     * @param ensureClusterStateVersionCallback callback to ensure that the current node is at least
     *                                          on a cluster state with the provided version;
     *                                          necessary for primary relocation so that new primary
     *                                          knows about all other ongoing replica recoveries
     *                                          when replicating documents (see
     *                                          {@link FileRecoverySourceHandler})
     */
    public PrimaryHandoffRecoveryTarget(final IndexShard indexShard,
                                        final DiscoveryNode sourceNode,
                                        final PeerRecoveryTargetService.RecoveryListener listener,
                                        final Callback<Long> ensureClusterStateVersionCallback) {
        super(indexShard, sourceNode, listener);
        this.ensureClusterStateVersionCallback = ensureClusterStateVersionCallback;
        // make sure the store is not released until we are done.
    }

    @Override
    public PrimaryHandoffRecoveryTarget retryCopy() {
        return new PrimaryHandoffRecoveryTarget(indexShard(), sourceNode(), listener(),
            ensureClusterStateVersionCallback);
    }

    @Override
    protected void onResetRecovery() {
        RecoveryState.Stage stage = indexShard().recoveryState().getStage();
        if (indexShard().recoveryState().getPrimary() && (stage == RecoveryState.Stage.FINALIZE ||
            stage == RecoveryState.Stage.DONE)) {
            // once primary relocation has moved past the finalization step, the relocation source
            // can be moved to RELOCATED state and start indexing as primary into the target
            // shard (see TransportReplicationAction). Resetting the target shard in this state
            // could mean that indexing is halted until the recovery retry attempt is completed
            // and could also destroy existing documents indexed and acknowledged before the reset.
            assert stage != RecoveryState.Stage.DONE :
                "recovery should not have completed when it's being reset";
            throw new IllegalStateException(
                "cannot reset recovery as previous attempt made it past finalization step");
        }
    }

    @Override
    public String startRecoveryActionName() {
        return PeerRecoverySourceService.Actions.START_PRIMARY_HANDOFF;
    }

    @Override
    public StartRecoveryRequest createStartRecoveryRequest(Logger logger, DiscoveryNode localNode)
        throws IOException {
        return new StartPrimaryHandoffRequest(shardId(), sourceNode(), localNode, recoveryId());
    }

    /**
     * Blockingly waits for cluster state with at least clusterStateVersion to be available
     */
    public void ensureClusterStateVersion(long clusterStateVersion) {
        ensureClusterStateVersionCallback.handle(clusterStateVersion);
    }
}
