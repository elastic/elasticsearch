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

package org.elasticsearch.cluster.factory;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;

public class StringClusterStatePartFactory extends ClusterStatePartFactory<String> {
    public StringClusterStatePartFactory(String type) {
        super(type);
    }

    public StringClusterStatePartFactory(String type, EnumSet<XContentContext> context) {
        super(type, context);
    }

    @Override
    public String readFrom(StreamInput in, String partName, @Nullable DiscoveryNode localNode) throws IOException {
        return in.readString();
    }

    @Override
    public void writeTo(String part, StreamOutput out) throws IOException {
        out.writeString(part);
    }

    @Override
    public String fromXContent(XContentParser parser, String partName, @Nullable DiscoveryNode localNode) throws IOException {
        return parser.text();
    }

    @Override
    public void toXContent(String part, XContentBuilder builder, Params params) throws IOException {
        builder.value(part);
    }
}
