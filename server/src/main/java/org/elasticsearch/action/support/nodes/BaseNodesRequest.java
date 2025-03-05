/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.nodes;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public abstract class BaseNodesRequest extends ActionRequest {

    /**
     * Sequence of node specifications that describe the nodes that this request should target. See {@link DiscoveryNodes#resolveNodes} for
     * a full description of the options. If set, {@link #concreteNodes} is {@code null} and ignored.
     **/
    private final String[] nodesIds;

    /**
     * The exact nodes that this request should target. If set, {@link #nodesIds} is {@code null} and ignored.
     **/
    private final DiscoveryNode[] concreteNodes;

    @Nullable // if no timeout
    private TimeValue timeout;

    protected BaseNodesRequest(String[] nodesIds) {
        this.nodesIds = nodesIds;
        this.concreteNodes = null;
    }

    protected BaseNodesRequest(DiscoveryNode... concreteNodes) {
        this.nodesIds = null;
        this.concreteNodes = concreteNodes;
    }

    public final String[] nodesIds() {
        return nodesIds;
    }

    @Nullable
    public TimeValue timeout() {
        return this.timeout;
    }

    public final void setTimeout(@Nullable TimeValue timeout) {
        this.timeout = timeout;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        // `BaseNodesRequest` is rather heavyweight, especially all those `DiscoveryNodes` objects in larger clusters, and there is no need
        // to send it out over the wire. Use a dedicated transport request just for the bits you need.
        TransportAction.localOnly();
    }

    /**
     * @return the nodes to which this request should fan out.
     */
    DiscoveryNode[] resolveNodes(ClusterState clusterState) {
        assert nodesIds == null || concreteNodes == null;
        return Objects.requireNonNullElseGet(
            concreteNodes,
            () -> Arrays.stream(clusterState.nodes().resolveNodes(nodesIds)).map(clusterState.nodes()::get).toArray(DiscoveryNode[]::new)
        );
    }
}
