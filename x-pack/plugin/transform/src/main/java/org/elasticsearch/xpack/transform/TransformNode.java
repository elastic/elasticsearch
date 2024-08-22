/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * Stateful representation of this node, relevant to the {@link org.elasticsearch.xpack.transform.transforms.TransformTask}.
 * For stateless functions, see {@link org.elasticsearch.xpack.transform.transforms.TransformNodes}.
 */
public class TransformNode {
    private final Supplier<Optional<ClusterState>> clusterState;

    public TransformNode(Supplier<Optional<ClusterState>> clusterState) {
        this.clusterState = clusterState;
    }

    /**
     * @return an optional containing true if this node is reported as shutting down in the cluster state metadata, false if it is not
     * reported as shutting down, or empty if the cluster state is missing or the local node has not been set yet.
     */
    public Optional<Boolean> isShuttingDown() {
        return clusterState.get().map(state -> {
            var localId = state.nodes().getLocalNodeId();
            if (localId != null) {
                return state.metadata().nodeShutdowns().contains(localId);
            } else {
                return null; // empty
            }
        });
    }

    /**
     * @return the node id stored in the cluster state, or "null" if the cluster state is missing or the local node has not been set yet.
     * This should behave exactly as {@link String#valueOf(Object)}.
     */
    public String nodeId() {
        return clusterState.get().map(ClusterState::nodes).map(DiscoveryNodes::getLocalNodeId).orElse("null");
    }
}
