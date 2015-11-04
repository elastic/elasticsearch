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

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

/**
 *
 */
public class ClusterStatsResponse extends BaseNodesResponse<ClusterStatsNodeResponse> implements ToXContent {

    ClusterStatsNodes nodesStats;
    ClusterStatsIndices indicesStats;
    String clusterUUID;
    ClusterHealthStatus status;
    long timestamp;


    ClusterStatsResponse() {
    }

    public ClusterStatsResponse(long timestamp, ClusterName clusterName, String clusterUUID, ClusterStatsNodeResponse[] nodes) {
        super(clusterName, null);
        this.timestamp = timestamp;
        this.clusterUUID = clusterUUID;
        nodesStats = new ClusterStatsNodes(nodes);
        indicesStats = new ClusterStatsIndices(nodes);
        for (ClusterStatsNodeResponse response : nodes) {
            // only the master node populates the status
            if (response.clusterStatus() != null) {
                status = response.clusterStatus();
                break;
            }
        }
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public ClusterHealthStatus getStatus() {
        return this.status;
    }

    public ClusterStatsNodes getNodesStats() {
        return nodesStats;
    }

    public ClusterStatsIndices getIndicesStats() {
        return indicesStats;
    }

    @Override
    public ClusterStatsNodeResponse[] getNodes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, ClusterStatsNodeResponse> getNodesMap() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClusterStatsNodeResponse getAt(int position) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<ClusterStatsNodeResponse> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        timestamp = in.readVLong();
        status = null;
        if (in.readBoolean()) {
            // it may be that the master switched on us while doing the operation. In this case the status may be null.
            status = ClusterHealthStatus.fromValue(in.readByte());
        }
        clusterUUID = in.readString();
        nodesStats = ClusterStatsNodes.readNodeStats(in);
        indicesStats = ClusterStatsIndices.readIndicesStats(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(timestamp);
        if (status == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeByte(status.value());
        }
        out.writeString(clusterUUID);
        nodesStats.writeTo(out);
        indicesStats.writeTo(out);
    }

    static final class Fields {
        static final XContentBuilderString NODES = new XContentBuilderString("nodes");
        static final XContentBuilderString INDICES = new XContentBuilderString("indices");
        static final XContentBuilderString UUID = new XContentBuilderString("uuid");
        static final XContentBuilderString CLUSTER_NAME = new XContentBuilderString("cluster_name");
        static final XContentBuilderString STATUS = new XContentBuilderString("status");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("timestamp", getTimestamp());
        builder.field(Fields.CLUSTER_NAME, getClusterName().value());
        if (params.paramAsBoolean("output_uuid", false)) {
            builder.field(Fields.UUID, clusterUUID);
        }
        if (status != null) {
            builder.field(Fields.STATUS, status.name().toLowerCase(Locale.ROOT));
        }
        builder.startObject(Fields.INDICES);
        indicesStats.toXContent(builder, params);
        builder.endObject();
        builder.startObject(Fields.NODES);
        nodesStats.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            return builder.string();
        } catch (IOException e) {
            return "{ \"error\" : \"" + e.getMessage() + "\"}";
        }
    }
}