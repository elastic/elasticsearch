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

package org.elasticsearch.action.admin.indices.dangling;

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Contains information about a dangling index, i.e. an index that Elasticsearch has found
 * on-disk but is not present in the cluster state.
 */
public class DanglingIndexInfo extends BaseNodeResponse implements ToXContentObject {
    private String indexName;
    private String indexUUID;
    private long creationDateMillis;

    public DanglingIndexInfo(DiscoveryNode node, String indexName, String indexUUID, long creationDateMillis) {
        super(node);
        this.indexName = indexName;
        this.indexUUID = indexUUID;
        this.creationDateMillis = creationDateMillis;
    }

    public DanglingIndexInfo(StreamInput in) throws IOException {
        super(in);
        this.indexName = in.readString();
        this.indexUUID = in.readString();
        this.creationDateMillis = in.readLong();
    }

    public String getIndexName() {
        return indexName;
    }

    public String getIndexUUID() {
        return indexUUID;
    }

    public String getNodeId() {
        return this.getNode().getId();
    }

    public long getCreationDateMillis() {
        return creationDateMillis;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("node_id", this.getNodeId());
        builder.field("node_name", this.getNode().getName());
        builder.field("index_name", this.indexName);
        builder.field("index_uuid", this.indexUUID);
        builder.timeField("index_creation_date_millis", "index_creation_date", this.creationDateMillis);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(this.indexName);
        out.writeString(this.indexUUID);
        out.writeLong(this.creationDateMillis);
    }
}
