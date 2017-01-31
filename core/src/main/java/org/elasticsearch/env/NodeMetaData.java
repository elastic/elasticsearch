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

package org.elasticsearch.env;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.gateway.MetaDataStateFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

/**
 * Metadata associated with this node. Currently only contains the unique uuid describing this node.
 * The metadata is persisted in the data folder of this node and is reused across restarts.
 */
public final class NodeMetaData {

    private static final String NODE_ID_KEY = "node_id";

    private final String nodeId;

    public NodeMetaData(final String nodeId) {
        this.nodeId = Objects.requireNonNull(nodeId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        NodeMetaData that = (NodeMetaData) o;

        return Objects.equals(this.nodeId, that.nodeId);
    }

    @Override
    public int hashCode() {
        return this.nodeId.hashCode();
    }

    @Override
    public String toString() {
        return "node_id [" + nodeId + "]";
    }

    private static ObjectParser<Builder, Void> PARSER = new ObjectParser<>("node_meta_data", Builder::new);

    static {
        PARSER.declareString(Builder::setNodeId, new ParseField(NODE_ID_KEY));
    }

    public String nodeId() {
        return nodeId;
    }

    private static class Builder {
        String nodeId;

        public void setNodeId(String nodeId) {
            this.nodeId = nodeId;
        }

        public NodeMetaData build() {
            return new NodeMetaData(nodeId);
        }
    }


    public static final MetaDataStateFormat<NodeMetaData> FORMAT = new MetaDataStateFormat<NodeMetaData>(XContentType.SMILE, "node-") {

        @Override
        protected XContentBuilder newXContentBuilder(XContentType type, OutputStream stream) throws IOException {
            XContentBuilder xContentBuilder = super.newXContentBuilder(type, stream);
            xContentBuilder.prettyPrint();
            return xContentBuilder;
        }

        @Override
        public void toXContent(XContentBuilder builder, NodeMetaData nodeMetaData) throws IOException {
            builder.field(NODE_ID_KEY, nodeMetaData.nodeId);
        }

        @Override
        public NodeMetaData fromXContent(XContentParser parser) throws IOException {
            return PARSER.apply(parser, null).build();
        }
    };
}
