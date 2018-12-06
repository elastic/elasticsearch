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

package org.elasticsearch.action.admin.cluster.state;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.io.IOException;
import java.util.Objects;

/**
 * The response for getting the cluster state.
 */
public class ClusterStateResponse extends ActionResponse {

    private ClusterName clusterName;
    private ClusterState clusterState;
    // the total compressed size of the full cluster state, not just
    // the parts included in this response
    private ByteSizeValue totalCompressedSize;
    private boolean waitForTimedOut = false;

    public ClusterStateResponse() {
    }

    public ClusterStateResponse(ClusterName clusterName, ClusterState clusterState, long sizeInBytes, boolean waitForTimedOut) {
        this.clusterName = clusterName;
        this.clusterState = clusterState;
        this.totalCompressedSize = new ByteSizeValue(sizeInBytes);
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
     * The total compressed size of the full cluster state, not just the parts
     * returned by {@link #getState()}.  The total compressed size is the size
     * of the cluster state as it would be transmitted over the network during
     * intra-node communication.
     */
    public ByteSizeValue getTotalCompressedSize() {
        return totalCompressedSize;
    }

    /**
     * Returns whether the request timed out waiting for a cluster state with a metadata version equal or
     * higher than the specified metadata.
     */
    public boolean isWaitForTimedOut() {
        return waitForTimedOut;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        clusterName = new ClusterName(in);
        if (in.getVersion().onOrAfter(Version.V_6_6_0)) {
            clusterState = in.readOptionalWriteable(innerIn -> ClusterState.readFrom(innerIn, null));
        } else {
            clusterState = ClusterState.readFrom(in, null);
        }
        if (in.getVersion().onOrAfter(Version.V_6_0_0_alpha1)) {
            totalCompressedSize = new ByteSizeValue(in);
        } else {
            // in a mixed cluster, if a pre 6.0 node processes the get cluster state
            // request, then a compressed size won't be returned, so just return 0;
            // its a temporary situation until all nodes in the cluster have been upgraded,
            // at which point the correct cluster state size will always be reported
            totalCompressedSize = new ByteSizeValue(0L);
        }
        if (in.getVersion().onOrAfter(Version.V_6_6_0)) {
            waitForTimedOut = in.readBoolean();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        clusterName.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_6_6_0)) {
            out.writeOptionalWriteable(clusterState);
        } else {
            if (out.getVersion().onOrAfter(Version.V_6_3_0)) {
                clusterState.writeTo(out);
            } else {
                ClusterModule.filterCustomsForPre63Clients(clusterState).writeTo(out);
            }
        }
        if (out.getVersion().onOrAfter(Version.V_6_0_0_alpha1)) {
            totalCompressedSize.writeTo(out);
        }
        if (out.getVersion().onOrAfter(Version.V_6_6_0)) {
            out.writeBoolean(waitForTimedOut);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterStateResponse response = (ClusterStateResponse) o;
        return waitForTimedOut == response.waitForTimedOut &&
            Objects.equals(clusterName, response.clusterName) &&
            // Best effort. Only compare cluster state version and master node id,
            // because cluster state doesn't implement equals()
            Objects.equals(getVersion(clusterState), getVersion(response.clusterState)) &&
            Objects.equals(getMasterNodeId(clusterState), getMasterNodeId(response.clusterState)) &&
            Objects.equals(totalCompressedSize, response.totalCompressedSize);
    }

    @Override
    public int hashCode() {
        // Best effort. Only use cluster state version and master node id,
        // because cluster state doesn't implement  hashcode()
        return Objects.hash(
            clusterName,
            getVersion(clusterState),
            getMasterNodeId(clusterState),
            totalCompressedSize,
            waitForTimedOut
        );
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
