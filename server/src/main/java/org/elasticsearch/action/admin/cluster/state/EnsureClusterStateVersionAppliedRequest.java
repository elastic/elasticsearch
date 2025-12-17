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

public class EnsureClusterStateVersionAppliedRequest extends BaseNodesRequest {
    private final long clusterStateVersion;
    private final TimeValue nodeTimeout;

    public EnsureClusterStateVersionAppliedRequest(long clusterStateVersion, TimeValue nodeTimeout, String... nodesIds) {
        super(nodesIds);
        this.clusterStateVersion = clusterStateVersion;
        this.nodeTimeout = nodeTimeout;
    }

    public EnsureClusterStateVersionAppliedRequest(long clusterStateVersion, TimeValue nodeTimeout, DiscoveryNode... concreteNodes) {
        super(concreteNodes);
        this.clusterStateVersion = clusterStateVersion;
        this.nodeTimeout = nodeTimeout;
    }

    public static EnsureClusterStateVersionAppliedRequest onAllNodes(long clusterStateVersion, TimeValue nodeTimeout) {
        /// Null nodes means all known nodes in the cluster,
        /// see [org.elasticsearch.action.support.nodes.TransportNodesAction#resolveRequest].
        return new EnsureClusterStateVersionAppliedRequest(clusterStateVersion, nodeTimeout, (DiscoveryNode[]) null);
    }

    public long clusterStateVersion() {
        return clusterStateVersion;
    }

    public TimeValue nodeTimeout() {
        return nodeTimeout;
    }
}
