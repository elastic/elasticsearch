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

package org.elasticsearch.cluster.service;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNodes;

/**
 * A simple immutable container class that comprises a cluster state and cluster state status. Used by {@link ClusterService}
 * to provide a snapshot view on which cluster state is currently being applied / already applied.
 */
public final class ClusterServiceState {
    private final ClusterState clusterState;
    private final ClusterStateStatus clusterStateStatus;
    private final ClusterState localClusterState;

    public ClusterServiceState(ClusterState clusterState, ClusterStateStatus clusterStateStatus, ClusterBlock noMasterBlock) {
        this.clusterState = clusterState;
        this.clusterStateStatus = clusterStateStatus;
        if (noMasterBlock != null) {
            assert noMasterBlock.id() == ClusterService.NO_MASTER_BLOCK_ID : "block must have NO_MASTER block id";
            // There is no master currently, so add the appropriate blocks to the returned
            // cluster state and set the masterNodeId to null.
            ClusterBlocks clusterBlocks =
                ClusterBlocks.builder().blocks(clusterState.blocks())
                    .removeGlobalBlock(ClusterService.NO_MASTER_BLOCK_ID) // remove block if it already exists before adding new one
                    .addGlobalBlock(noMasterBlock)
                    .build();

            DiscoveryNodes discoveryNodes =
                new DiscoveryNodes.Builder(clusterState.nodes())
                    .masterNodeId(null)
                    .build();

            localClusterState = ClusterState.builder(clusterState)
                                        .blocks(clusterBlocks)
                                        .nodes(discoveryNodes)
                                        .build();
        } else {
            localClusterState = clusterState;
        }
    }

    public ClusterState getClusterState() {
        return clusterState;
    }

    public ClusterStateStatus getClusterStateStatus() {
        return clusterStateStatus;
    }

    /**
     * Returns the local view of the cluster state, which is based on the authoritative cluster state as
     * obtained from {@link #getClusterState()} with added overlays on the cluster state based on the local
     * state of the {@link ClusterService} at the time of this instance's creation.  In particular, the overlays
     * that can be added are NO_MASTER blocks on the cluster state and setting the masterNodeId to null, in
     * the case of the ClusterService having no master.
     */
    public ClusterState getLocalClusterState() {
        return localClusterState;
    }

    /**
     * Returns {@code true} if the cluster service's state as represented by this instance has no master node,
     * returns {@code false} otherwise.
     */
    public boolean hasNoMaster() {
        return localClusterState.blocks().hasGlobalBlock(ClusterService.NO_MASTER_BLOCK_ID);
    }

    @Override
    public String toString() {
        return "version [" + clusterState.version() + "], status [" + clusterStateStatus + "]";
    }
}
