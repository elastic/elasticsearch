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

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Verify repository response
 */
public class VerifyRepositoryResponse extends ActionResponse implements ToXContentObject {

    static final String NODES = "nodes";
    static final String NAME = "name";

    public static class NodeView implements Writeable, ToXContentObject {
        private static final ObjectParser.NamedObjectParser<NodeView, Void> PARSER;
        static {
            ObjectParser<NodeView, Void> internalParser = new ObjectParser<>(NODES);
            internalParser.declareString(NodeView::setName, new ParseField(NAME));
            PARSER = (p, v, name) -> internalParser.parse(p, new NodeView(name), null);
        }

        final String nodeId;
        String name;

        public NodeView(String nodeId) { this.nodeId = nodeId; }

        public NodeView(String nodeId, String name) {
            this(nodeId);
            this.name = name;
        }

        public NodeView(StreamInput in) throws IOException {
            this(in.readString(), in.readString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(nodeId);
            out.writeString(name);
        }

        void setName(String name) { this.name = name; }

        public String getName() { return name; }

        public String getNodeId() { return nodeId; }

        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(nodeId);
            {
                builder.field(NAME, name);
            }
            builder.endObject();
            return builder;
        }

        /**
         * Temporary method that allows turning a {@link NodeView} into a {@link DiscoveryNode}. This representation will never be used in
         * practice, because in >= 6.4 a consumer of the response will only be able to retrieve a representation of {@link NodeView}
         * objects.
         *
         * Effectively this will be used to hold the state of the object in 6.x so there is no need to have 2 backing objects that
         * represent the state of the Response. In practice these will always be read by a consumer as a NodeView, but it eases the
         * transition to master which will not contain any representation of a {@link DiscoveryNode}.
         */
        DiscoveryNode convertToDiscoveryNode() {
            return new DiscoveryNode(name, nodeId, "", "", "", new TransportAddress(TransportAddress.META_ADDRESS, 0),
                Collections.emptyMap(), Collections.emptySet(), Version.CURRENT);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            NodeView other = (NodeView) obj;
            return Objects.equals(nodeId, other.nodeId) &&
                Objects.equals(name, other.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeId, name);
        }
    }

    private List<DiscoveryNode> nodes;

    private ClusterName clusterName;


    private static final ObjectParser<VerifyRepositoryResponse, Void> PARSER =
        new ObjectParser<>(VerifyRepositoryResponse.class.getName(), VerifyRepositoryResponse::new);
    static {
        PARSER.declareNamedObjects(VerifyRepositoryResponse::setNodes, NodeView.PARSER, new ParseField("nodes"));
    }

    VerifyRepositoryResponse() {
    }

    public VerifyRepositoryResponse(ClusterName clusterName, DiscoveryNode[] nodes) {
        this.clusterName = clusterName;
        this.nodes = Arrays.asList(nodes);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        if (in.getVersion().onOrAfter(Version.V_6_4_0)) {
            this.nodes = in.readList(NodeView::new).stream().map(n -> n.convertToDiscoveryNode()).collect(Collectors.toList());
        } else {
            clusterName = new ClusterName(in);
            this.nodes = in.readList(DiscoveryNode::new);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_6_4_0)) {
            out.writeList(getNodes());
        } else {
            clusterName.writeTo(out);
            out.writeList(nodes);
        }
    }

    public List<NodeView> getNodes() {
        return nodes.stream().map(dn -> new NodeView(dn.getId(), dn.getName())).collect(Collectors.toList());
    }

    public ClusterName getClusterName() {
        return clusterName;
    }

    protected void setNodes(List<NodeView> nodes) {
        this.nodes = nodes.stream().map(n -> n.convertToDiscoveryNode()).collect(Collectors.toList());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.startObject(NODES);
            {
                for (DiscoveryNode node : nodes) {
                    builder.startObject(node.getId());
                    {
                        builder.field(NAME, node.getName());
                    }
                    builder.endObject();
                }
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    public static VerifyRepositoryResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        VerifyRepositoryResponse other = (VerifyRepositoryResponse) obj;
        return nodes.equals(other.nodes);
    }

    @Override
    public int hashCode() {
        return nodes.hashCode();
    }
}
