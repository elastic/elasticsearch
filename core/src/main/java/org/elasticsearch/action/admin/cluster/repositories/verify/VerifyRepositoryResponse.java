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

package org.elasticsearch.action.admin.cluster.repositories.verify;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.repositories.VerificationFailure;

import java.io.IOException;
import java.util.Arrays;

/**
 * Unregister repository response
 */
public class VerifyRepositoryResponse extends ActionResponse implements ToXContent {

    private DiscoveryNode[] nodes;

    private ClusterName clusterName;


    VerifyRepositoryResponse() {
    }

    public VerifyRepositoryResponse(ClusterName clusterName, DiscoveryNode[] nodes) {
        this.clusterName = clusterName;
        this.nodes = nodes;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        clusterName = ClusterName.readClusterName(in);
        nodes = new DiscoveryNode[in.readVInt()];
        for (int i=0; i<nodes.length; i++){
            nodes[i] = DiscoveryNode.readNode(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        clusterName.writeTo(out);
        out.writeVInt(nodes.length);
        for (DiscoveryNode node : nodes) {
            node.writeTo(out);
        }
    }

    public DiscoveryNode[] getNodes() {
        return nodes;
    }

    public ClusterName getClusterName() {
        return clusterName;
    }

    static final class Fields {
        static final XContentBuilderString NODES = new XContentBuilderString("nodes");
        static final XContentBuilderString NAME = new XContentBuilderString("name");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.NODES);
        for (DiscoveryNode node : nodes) {
            builder.startObject(node.id(), XContentBuilder.FieldCaseConversion.NONE);
            builder.field(Fields.NAME, node.name(), XContentBuilder.FieldCaseConversion.NONE);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return XContentHelper.toString(this);
    }
}
