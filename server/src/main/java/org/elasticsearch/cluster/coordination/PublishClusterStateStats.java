/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Class encapsulating stats about the PublishClusterStateAction
 */
public class PublishClusterStateStats implements Writeable, ToXContentObject {

    private final long fullClusterStateReceivedCount;
    private final long incompatibleClusterStateDiffReceivedCount;
    private final long compatibleClusterStateDiffReceivedCount;
    private final ClusterStateSerializationStats clusterStateSerializationStats;

    /**
     * @param fullClusterStateReceivedCount the number of times this node has received a full copy of the cluster state from the master.
     * @param incompatibleClusterStateDiffReceivedCount the number of times this node has received a cluster-state diff from the master.
     * @param compatibleClusterStateDiffReceivedCount the number of times that received cluster-state diffs were compatible with
     */
    public PublishClusterStateStats(
        long fullClusterStateReceivedCount,
        long incompatibleClusterStateDiffReceivedCount,
        long compatibleClusterStateDiffReceivedCount,
        ClusterStateSerializationStats clusterStateSerializationStats
    ) {
        this.fullClusterStateReceivedCount = fullClusterStateReceivedCount;
        this.incompatibleClusterStateDiffReceivedCount = incompatibleClusterStateDiffReceivedCount;
        this.compatibleClusterStateDiffReceivedCount = compatibleClusterStateDiffReceivedCount;
        this.clusterStateSerializationStats = clusterStateSerializationStats;
    }

    public PublishClusterStateStats(StreamInput in) throws IOException {
        fullClusterStateReceivedCount = in.readVLong();
        incompatibleClusterStateDiffReceivedCount = in.readVLong();
        compatibleClusterStateDiffReceivedCount = in.readVLong();
        clusterStateSerializationStats = new ClusterStateSerializationStats(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(fullClusterStateReceivedCount);
        out.writeVLong(incompatibleClusterStateDiffReceivedCount);
        out.writeVLong(compatibleClusterStateDiffReceivedCount);
        clusterStateSerializationStats.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        builder.field("serialized_cluster_states");
        clusterStateSerializationStats.toXContent(builder, params);
        builder.startObject("published_cluster_states");
        {
            builder.field("full_states", fullClusterStateReceivedCount);
            builder.field("incompatible_diffs", incompatibleClusterStateDiffReceivedCount);
            builder.field("compatible_diffs", compatibleClusterStateDiffReceivedCount);
        }
        builder.endObject();
        return builder;
    }

    public long getFullClusterStateReceivedCount() {
        return fullClusterStateReceivedCount;
    }

    public long getIncompatibleClusterStateDiffReceivedCount() {
        return incompatibleClusterStateDiffReceivedCount;
    }

    public long getCompatibleClusterStateDiffReceivedCount() {
        return compatibleClusterStateDiffReceivedCount;
    }

    public ClusterStateSerializationStats getClusterStateSerializationStats() {
        return clusterStateSerializationStats;
    }

    @Override
    public String toString() {
        return "PublishClusterStateStats(full="
            + fullClusterStateReceivedCount
            + ", incompatible="
            + incompatibleClusterStateDiffReceivedCount
            + ", compatible="
            + compatibleClusterStateDiffReceivedCount
            + ", serializationStats="
            + Strings.toString(clusterStateSerializationStats)
            + ")";
    }
}
