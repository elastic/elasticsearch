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
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Class encapsulating stats about the PublishClusterStateAction
 */
public class PublishClusterStateStats implements Writeable, ToXContentObject {

    private final long fullClusterStateReceivedCount;
    private final long incompatibleClusterStateDiffReceivedCount;
    private final long compatibleClusterStateDiffReceivedCount;

    /**
     * @param fullClusterStateReceivedCount the number of times this node has received a full copy of the cluster state from the master.
     * @param incompatibleClusterStateDiffReceivedCount the number of times this node has received a cluster-state diff from the master.
     * @param compatibleClusterStateDiffReceivedCount the number of times that received cluster-state diffs were compatible with
     */
    public PublishClusterStateStats(long fullClusterStateReceivedCount,
                                    long incompatibleClusterStateDiffReceivedCount,
                                    long compatibleClusterStateDiffReceivedCount) {
        this.fullClusterStateReceivedCount = fullClusterStateReceivedCount;
        this.incompatibleClusterStateDiffReceivedCount = incompatibleClusterStateDiffReceivedCount;
        this.compatibleClusterStateDiffReceivedCount = compatibleClusterStateDiffReceivedCount;
    }

    public PublishClusterStateStats(StreamInput in) throws IOException {
        fullClusterStateReceivedCount = in.readVLong();
        incompatibleClusterStateDiffReceivedCount = in.readVLong();
        compatibleClusterStateDiffReceivedCount = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(fullClusterStateReceivedCount);
        out.writeVLong(incompatibleClusterStateDiffReceivedCount);
        out.writeVLong(compatibleClusterStateDiffReceivedCount);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("published_cluster_states");
        {
            builder.field("full_states", fullClusterStateReceivedCount);
            builder.field("incompatible_diffs", incompatibleClusterStateDiffReceivedCount);
            builder.field("compatible_diffs", compatibleClusterStateDiffReceivedCount);
        }
        builder.endObject();
        return builder;
    }

    long getFullClusterStateReceivedCount() { return fullClusterStateReceivedCount; }

    long getIncompatibleClusterStateDiffReceivedCount() { return incompatibleClusterStateDiffReceivedCount; }

    long getCompatibleClusterStateDiffReceivedCount() { return compatibleClusterStateDiffReceivedCount; }

    @Override
    public String toString() {
        return "PublishClusterStateStats(full=" + fullClusterStateReceivedCount
            + ", incompatible=" + incompatibleClusterStateDiffReceivedCount
            + ", compatible=" + compatibleClusterStateDiffReceivedCount
            + ")";
    }
}
