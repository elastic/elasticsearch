/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;

import java.io.IOException;

public class ClusterStateReusingStreamInput extends FilterStreamInput {

    private final DiscoveryNodes discoveryNodes;

    public ClusterStateReusingStreamInput(StreamInput delegate, ClusterState clusterState) {
        super(delegate);
        this.discoveryNodes = clusterState.nodes();
    }

    public DiscoveryNode readDiscoveryNode() throws IOException {
        final DiscoveryNode fromStream = new DiscoveryNode(delegate);
        final DiscoveryNode fromClusterState = discoveryNodes.get(fromStream.getId());
        return fromStream.equals(fromClusterState) && fromStream.getRoles().equals(fromClusterState.getRoles())
            ? fromClusterState
            : fromStream;
    }
}
