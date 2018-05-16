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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class VerifyRepositoryRestResponse implements ToXContentObject {

    public static class NodeView implements ToXContentObject {
        private static final ObjectParser.NamedObjectParser<NodeView, Void> PARSER;
        static {
            ObjectParser<NodeView, Void> parser = new ObjectParser<>("nodes");
            parser.declareString(NodeView::setName, new ParseField(Fields.NAME));
            PARSER = (XContentParser p, Void v, String name )-> parser.parse(p, new NodeView(name), null);
        }

        final String nodeId;
        String name;

        public NodeView(String nodeId) { this.nodeId = nodeId; }

        public NodeView(String nodeId, String name) {
            this(nodeId);
            this.name = name;
        }

        void setName(String name) { this.name = name; }

        public String getName() { return name; }

        public String getNodeId() { return nodeId; }

        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(nodeId);
            builder.field(Fields.NAME, name);
            builder.endObject();
            return builder;
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

    private static final ObjectParser<VerifyRepositoryRestResponse, Void> PARSER =
        new ObjectParser<>(VerifyRepositoryRestResponse.class.getName(), VerifyRepositoryRestResponse::new);
    static {
        PARSER.declareNamedObjects(VerifyRepositoryRestResponse::nodes, NodeView.PARSER, new ParseField("nodes"));
    }

    private List<NodeView> nodes = new ArrayList<>();

    VerifyRepositoryRestResponse() {
    }

    public List<NodeView> nodes() {
        return nodes;
    }

    void nodes(List<NodeView> nodes) {
        this.nodes = nodes;
    }

    static final class Fields {
        static final String NODES = "nodes";
        static final String NAME = "name";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(Fields.NODES);
        for (NodeView node : nodes) {
            node.toXContent(builder, params);
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static VerifyRepositoryRestResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        VerifyRepositoryRestResponse other = (VerifyRepositoryRestResponse) obj;
        return nodes.equals(other.nodes);
    }

    @Override
    public int hashCode() {
        return nodes.hashCode();
    }
}
