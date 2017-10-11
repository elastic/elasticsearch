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
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Class encapsulating stats about the PublishClusterStateAction
 */
public class PublishClusterStateStats implements Writeable, ToXContentFragment {

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

    public PublishClusterStateStats(StreamInput streamInput) throws IOException {
        fullClusterStateSentCount = streamInput.readVLong();
        clusterStateDiffSentCount = streamInput.readVLong();
        incompatibleClusterStateDiffVersionCount = streamInput.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(fullClusterStateSentCount);
        out.writeVLong(clusterStateDiffSentCount);
        out.writeVLong(incompatibleClusterStateDiffVersionCount);
    }

    static final class Fields {
        static final String PUBLISH_CLUSTER_STATE = "publish_cluster_state";
        static final String FULL_SENT = "full_cluster_states_sent";
        static final String DIFFS_SENT = "cluster_state_diffs_sent";
        static final String INCOMPATIBLE_DIFFS = "incompatible_cluster_state_diffs_sent";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.PUBLISH_CLUSTER_STATE);
        builder.field(Fields.FULL_SENT, fullClusterStateSentCount);
        builder.field(Fields.DIFFS_SENT, clusterStateDiffSentCount);
        builder.field(Fields.INCOMPATIBLE_DIFFS, incompatibleClusterStateDiffVersionCount);
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
