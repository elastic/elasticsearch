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
    private final long incompatibleClusterStateDiffVersionCount;

    public PublishClusterStateStats(long fullClusterStateSentCount,
                                    long clusterStateDiffSentCount,
                                    long incompatibleClusterStateDiffVersionCount) {
        this.fullClusterStateSentCount = fullClusterStateSentCount;
        this.clusterStateDiffSentCount = clusterStateDiffSentCount;
        this.incompatibleClusterStateDiffVersionCount = incompatibleClusterStateDiffVersionCount;
    }

    public PublishClusterStateStats(StreamInput in) throws IOException {
        fullClusterStateSentCount = in.readVLong();
        clusterStateDiffSentCount = in.readVLong();
        incompatibleClusterStateDiffVersionCount = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(fullClusterStateSentCount);
        out.writeVLong(clusterStateDiffSentCount);
        out.writeVLong(incompatibleClusterStateDiffVersionCount);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("publish_cluster_state");
        builder.field("full_cluster_states_sent", fullClusterStateSentCount);
        builder.field("cluster_state_diffs_sent", clusterStateDiffSentCount);
        builder.field("incompatible_cluster_state_diffs_sent", incompatibleClusterStateDiffVersionCount);
        builder.endObject();
        return builder;
    }

    public long getFullClusterStateSentCount() {
        return fullClusterStateSentCount;
    }

    public long getClusterStateDiffSentCount() {
        return clusterStateDiffSentCount;
    }

    public long getIncompatibleClusterStateDiffVersionCount() {
        return incompatibleClusterStateDiffVersionCount;
    }

    @Override
    public String toString() {
        return "PublishClusterStateStats(full=" + fullClusterStateSentCount
            + ", diffs=" + clusterStateDiffSentCount
            + ", incompatible=" + incompatibleClusterStateDiffVersionCount
            + ")";
    }
}
