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

package org.elasticsearch.discovery.zen;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Class encapsulating stats about the PublishClusterStateAction
 */
public class PublishClusterStateStats implements Writeable, ToXContent {

    private final long fullClusterStateSentCount;
    private final long clusterStateDiffSentCount;
    private final long incompatibleClusterStateDiffSentCount;

    private final long fullClusterStateReceivedCount;
    private final long clusterStateDiffReceivedCount;
    private final long compatibleClusterStateDiffReceivedCount;

    /**
     * @param fullClusterStateSentCount  the number of times this node, as master, has sent the full cluster state to a follower.
     * @param clusterStateDiffSentCount  the number of times this node, as master, has sent a cluster-state diff to a follower.
     * @param incompatibleClusterStateDiffSentCount  the number of times a cluster-state diff sent by this node was rejected by a follower
     *                                               because the follower's state was incompatible with the diff that was sent.
     *
     * @param fullClusterStateReceivedCount the number of times this node has received a full copy of the cluster state from the master.
     * @param clusterStateDiffReceivedCount the number of times this node has received a cluster-state diff from the master.
     * @param compatibleClusterStateDiffReceivedCount the number of times that received cluster-state diffs were compatible with
     *                                                the current cluster state.
     */
    public PublishClusterStateStats(long fullClusterStateSentCount,
                                    long clusterStateDiffSentCount,
                                    long incompatibleClusterStateDiffSentCount,
                                    long fullClusterStateReceivedCount,
                                    long clusterStateDiffReceivedCount,
                                    long compatibleClusterStateDiffReceivedCount) {
        this.fullClusterStateSentCount = fullClusterStateSentCount;
        this.clusterStateDiffSentCount = clusterStateDiffSentCount;
        this.incompatibleClusterStateDiffSentCount = incompatibleClusterStateDiffSentCount;
        this.fullClusterStateReceivedCount = fullClusterStateReceivedCount;
        this.clusterStateDiffReceivedCount = clusterStateDiffReceivedCount;
        this.compatibleClusterStateDiffReceivedCount = compatibleClusterStateDiffReceivedCount;
    }

    public PublishClusterStateStats(StreamInput in) throws IOException {
        fullClusterStateSentCount = in.readVLong();
        clusterStateDiffSentCount = in.readVLong();
        incompatibleClusterStateDiffSentCount = in.readVLong();
        fullClusterStateReceivedCount = in.readVLong();
        clusterStateDiffReceivedCount = in.readVLong();
        compatibleClusterStateDiffReceivedCount = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(fullClusterStateSentCount);
        out.writeVLong(clusterStateDiffSentCount);
        out.writeVLong(incompatibleClusterStateDiffSentCount);
        out.writeVLong(fullClusterStateReceivedCount);
        out.writeVLong(clusterStateDiffReceivedCount);
        out.writeVLong(compatibleClusterStateDiffReceivedCount);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("publish_cluster_state");
        builder.startObject("sent");
        builder.field("full_cluster_states", fullClusterStateSentCount);
        builder.field("cluster_state_diffs", clusterStateDiffSentCount);
        builder.field("incompatible_cluster_state_diffs", incompatibleClusterStateDiffSentCount);
        builder.endObject();
        builder.startObject("received");
        builder.field("full_cluster_states", fullClusterStateReceivedCount);
        builder.field("cluster_state_diffs", clusterStateDiffReceivedCount);
        builder.field("compatible_cluster_state_diffs", compatibleClusterStateDiffReceivedCount);
        builder.endObject();
        builder.endObject();
        return builder;
    }

    public long getFullClusterStateSentCount() {
        return fullClusterStateSentCount;
    }

    public long getClusterStateDiffSentCount() {
        return clusterStateDiffSentCount;
    }

    public long getIncompatibleClusterStateDiffSentCount() {
        return incompatibleClusterStateDiffSentCount;
    }

    public long getFullClusterStateReceivedCount() { return fullClusterStateReceivedCount; }

    public long getClusterStateDiffReceivedCount() { return clusterStateDiffReceivedCount; }

    public long getCompatibleClusterStateDiffReceivedCount() { return compatibleClusterStateDiffReceivedCount; }

    @Override
    public String toString() {
        return "PublishClusterStateStats(sent(full=" + fullClusterStateSentCount
            + ", diffs=" + clusterStateDiffSentCount
            + ", incompatible=" + incompatibleClusterStateDiffSentCount
            + "), received(full=" + fullClusterStateReceivedCount
            + ", diffs=" + clusterStateDiffReceivedCount
            + ", compatible=" + compatibleClusterStateDiffReceivedCount
            + "))";
    }
}
