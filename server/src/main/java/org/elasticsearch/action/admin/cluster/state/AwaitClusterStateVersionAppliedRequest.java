/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.state;

import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.core.TimeValue;

import java.util.Objects;

public class AwaitClusterStateVersionAppliedRequest extends BaseNodesRequest {
    private final long clusterStateVersion;
    private final TimeValue nodeTimeout;

    /// Creates a new instance of the request.
    /// @param clusterStateVersion a version that will be awaited on the provided set of nodes
    /// @param nodeTimeout a timeout for the cluster state observer awaiting application of the cluster state version on every node.
    ///                  Use [TimeValue.MINUS_ONE] as a "no timeout" value.
    /// @param nodesIds nodes identified by node id to use when checking if a cluster state version is applied
    public AwaitClusterStateVersionAppliedRequest(long clusterStateVersion, TimeValue nodeTimeout, String... nodesIds) {
        super(nodesIds);
        this.clusterStateVersion = clusterStateVersion;
        this.nodeTimeout = Objects.requireNonNull(nodeTimeout);
    }

    /// Creates a new instance of the request.
    /// @param clusterStateVersion a version that will be awaited on the provided set of nodes
    /// @param nodeTimeout a timeout for the cluster state observer awaiting application of the cluster state version on every node.
    ///                  Use [TimeValue.MINUS_ONE] as a "no timeout" value.
    /// @param concreteNodes nodes to use when checking if a cluster state version is applied
    public AwaitClusterStateVersionAppliedRequest(long clusterStateVersion, TimeValue nodeTimeout, DiscoveryNode... concreteNodes) {
        super(concreteNodes);
        this.clusterStateVersion = clusterStateVersion;
        this.nodeTimeout = Objects.requireNonNull(nodeTimeout);
    }

    public static AwaitClusterStateVersionAppliedRequest onAllNodes(long clusterStateVersion, TimeValue nodeTimeout) {
        /// Null nodes means all known nodes in the cluster,
        /// see [org.elasticsearch.action.support.nodes.TransportNodesAction#resolveRequest].
        return new AwaitClusterStateVersionAppliedRequest(clusterStateVersion, nodeTimeout, (DiscoveryNode[]) null);
    }

    public long clusterStateVersion() {
        return clusterStateVersion;
    }

    public TimeValue nodeTimeout() {
        return nodeTimeout;
    }
}
