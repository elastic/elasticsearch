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

public class EnsureClusterStateVersionAppliedRequest extends BaseNodesRequest {
    private final long clusterStateVersion;

    public EnsureClusterStateVersionAppliedRequest(long clusterStateVersion, String... nodesIds) {
        super(nodesIds);
        this.clusterStateVersion = clusterStateVersion;
    }

    public EnsureClusterStateVersionAppliedRequest(long clusterStateVersion, DiscoveryNode... concreteNodes) {
        super(concreteNodes);
        this.clusterStateVersion = clusterStateVersion;
    }

    public static EnsureClusterStateVersionAppliedRequest onAllNodes(long clusterStateVersion) {
        /// Null nodes means all known nodes in the cluster,
        /// see [org.elasticsearch.action.support.nodes.TransportNodesAction#resolveRequest].
        return new EnsureClusterStateVersionAppliedRequest(clusterStateVersion, (DiscoveryNode[]) null);
    }

    public long clusterStateVersion() {
        return clusterStateVersion;
    }
}
