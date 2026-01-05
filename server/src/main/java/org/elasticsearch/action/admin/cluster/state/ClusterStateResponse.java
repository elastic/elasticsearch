/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.state;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

/**
 * The response for getting the cluster state.
 */
public class ClusterStateResponse extends ActionResponse {

    private final ClusterName clusterName;
    private final ClusterState clusterState;
    private final boolean waitForTimedOut;

    public ClusterStateResponse(StreamInput in) throws IOException {
        clusterName = new ClusterName(in);
        clusterState = in.readOptionalWriteable(innerIn -> ClusterState.readFrom(innerIn, null));
        waitForTimedOut = in.readBoolean();
    }

    public ClusterStateResponse(ClusterName clusterName, ClusterState clusterState, boolean waitForTimedOut) {
        this.clusterName = clusterName;
        this.clusterState = clusterState;
        this.waitForTimedOut = waitForTimedOut;
    }

    /**
     * The requested cluster state.  Only the parts of the cluster state that were
     * requested are included in the returned {@link ClusterState} instance.
     */
    public ClusterState getState() {
        return this.clusterState;
    }

    /**
     * The name of the cluster.
     */
    public ClusterName getClusterName() {
        return this.clusterName;
    }

    /**
     * Returns whether the request timed out waiting for a cluster state with a metadata version equal or
     * higher than the specified metadata.
     */
    public boolean isWaitForTimedOut() {
        return waitForTimedOut;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        clusterName.writeTo(out);
        out.writeOptionalWriteable(clusterState);
        out.writeBoolean(waitForTimedOut);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterStateResponse response = (ClusterStateResponse) o;
        return waitForTimedOut == response.waitForTimedOut && Objects.equals(clusterName, response.clusterName) &&
        // Best effort. Only compare cluster state version and master node id,
        // because cluster state doesn't implement equals()
            Objects.equals(getVersion(clusterState), getVersion(response.clusterState))
            && Objects.equals(getMasterNodeId(clusterState), getMasterNodeId(response.clusterState));
    }

    @Override
    public int hashCode() {
        // Best effort. Only use cluster state version and master node id,
        // because cluster state doesn't implement hashcode()
        return Objects.hash(clusterName, getVersion(clusterState), getMasterNodeId(clusterState), waitForTimedOut);
    }

    private static String getMasterNodeId(ClusterState clusterState) {
        if (clusterState == null) {
            return null;
        }
        DiscoveryNodes nodes = clusterState.getNodes();
        if (nodes != null) {
            return nodes.getMasterNodeId();
        } else {
            return null;
        }
    }

    private static Long getVersion(ClusterState clusterState) {
        if (clusterState != null) {
            return clusterState.getVersion();
        } else {
            return null;
        }
    }

}
