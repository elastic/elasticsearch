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

package org.elasticsearch.action.admin.cluster.shards;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.internal.AliasFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class ClusterSearchShardsResponse extends ActionResponse implements ToXContentObject {

    public static final ClusterSearchShardsResponse EMPTY = new ClusterSearchShardsResponse(new ClusterSearchShardsGroup[0],
            new DiscoveryNode[0], Collections.emptyMap());

    private static final ParseField NODES = new ParseField("nodes");
    private static final ParseField INDICES = new ParseField("indices");
    private static final ParseField SHARDS = new ParseField("shards");

    private static final ObjectParser<ClusterSearchShardsResponse, Void> PARSER = new ObjectParser<>("cluster_update_settings_request",
        true, ClusterSearchShardsResponse::new);

    static {
        PARSER.declareObject((r, p) -> r.nodes = p, (p, c) -> {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, p.currentToken(), p::getTokenLocation);
            p.nextToken();
            List<DiscoveryNode> nodes = new ArrayList<>();
            while (p.currentToken() != XContentParser.Token.END_OBJECT) {
                final DiscoveryNode node = DiscoveryNode.fromXContent(p);
                nodes.add(node);
            }
            return nodes.toArray(new DiscoveryNode[0]);
        }, NODES);

        PARSER.declareObject((r, p) -> r.indicesAndFilters = p, (p, c) -> {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, p.currentToken(), p::getTokenLocation);
            p.nextToken();
            Map<String, AliasFilter> map = new HashMap<>();
            while (p.currentToken() != XContentParser.Token.END_OBJECT) {
                ensureExpectedToken(XContentParser.Token.FIELD_NAME, p.currentToken(), p::getTokenLocation);
                String index = p.currentName();
                AliasFilter filter = AliasFilter.fromXContent(p);
                map.put(index, filter);
                ensureExpectedToken(XContentParser.Token.END_OBJECT, p.currentToken(), p::getTokenLocation);
                p.nextToken();
            }
            return map;
        }, INDICES);

        PARSER.declareField(
            (r, p) -> r.groups = p,
            (p, c) -> {
                ensureExpectedToken(XContentParser.Token.START_ARRAY, p.currentToken(), p::getTokenLocation);
                p.nextToken();
                List<ClusterSearchShardsGroup> shardsGroups = new ArrayList<>();
                while (p.currentToken() != XContentParser.Token.END_ARRAY) {
                    final ClusterSearchShardsGroup group = ClusterSearchShardsGroup.fromXContent(p);
                    shardsGroups.add(group);
                    p.nextToken();
                }
                ensureExpectedToken(XContentParser.Token.END_ARRAY, p.currentToken(), p::getTokenLocation);
                return shardsGroups.toArray(new ClusterSearchShardsGroup[0]);
            },
            SHARDS,
            ObjectParser.ValueType.OBJECT_ARRAY);
    }

    private ClusterSearchShardsGroup[] groups;
    private DiscoveryNode[] nodes;
    private Map<String, AliasFilter> indicesAndFilters;

    public ClusterSearchShardsResponse() {

    }

    public ClusterSearchShardsResponse(ClusterSearchShardsGroup[] groups, DiscoveryNode[] nodes,
                                       Map<String, AliasFilter> indicesAndFilters) {
        this.groups = groups;
        this.nodes = nodes;
        this.indicesAndFilters = indicesAndFilters;
    }

    public ClusterSearchShardsGroup[] getGroups() {
        return groups;
    }

    public DiscoveryNode[] getNodes() {
        return nodes;
    }

    public Map<String, AliasFilter> getIndicesAndFilters() {
        return indicesAndFilters;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        groups = new ClusterSearchShardsGroup[in.readVInt()];
        for (int i = 0; i < groups.length; i++) {
            groups[i] = ClusterSearchShardsGroup.readSearchShardsGroupResponse(in);
        }
        nodes = new DiscoveryNode[in.readVInt()];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = new DiscoveryNode(in);
        }
        if (in.getVersion().onOrAfter(Version.V_5_1_1)) {
            int size = in.readVInt();
            indicesAndFilters = new HashMap<>();
            for (int i = 0; i < size; i++) {
                String index = in.readString();
                AliasFilter aliasFilter = new AliasFilter(in);
                indicesAndFilters.put(index, aliasFilter);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(groups.length);
        for (ClusterSearchShardsGroup response : groups) {
            response.writeTo(out);
        }
        out.writeVInt(nodes.length);
        for (DiscoveryNode node : nodes) {
            node.writeTo(out);
        }
        if (out.getVersion().onOrAfter(Version.V_5_1_1)) {
            out.writeVInt(indicesAndFilters.size());
            for (Map.Entry<String, AliasFilter> entry : indicesAndFilters.entrySet()) {
                out.writeString(entry.getKey());
                entry.getValue().writeTo(out);
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(NODES);
        for (DiscoveryNode node : nodes) {
            node.toXContent(builder, params);
        }
        builder.endObject();
        if (indicesAndFilters != null) {
            builder.startObject(INDICES);
            for (Map.Entry<String, AliasFilter> entry : indicesAndFilters.entrySet()) {
                String index = entry.getKey();
                builder.startObject(index);
                AliasFilter aliasFilter = entry.getValue();
                aliasFilter.toXContent(builder, params);
                builder.endObject();
            }
            builder.endObject();
        }
        builder.startArray(SHARDS);
        for (ClusterSearchShardsGroup group : groups) {
            group.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public static ClusterSearchShardsResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClusterSearchShardsResponse that = (ClusterSearchShardsResponse) o;

        return Arrays.equals(groups, that.groups)
            && Arrays.equals(nodes, that.nodes)
            && (indicesAndFilters != null ? indicesAndFilters.equals(that.indicesAndFilters) : that.indicesAndFilters == null);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(groups);
        result = 31 * result + Arrays.hashCode(nodes);
        result = 31 * result + (indicesAndFilters != null ? indicesAndFilters.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ClusterSearchShardsResponse{" +
            "groups=" + (groups == null ? null : Arrays.asList(groups)) +
            ", nodes=" + (nodes == null ? null : Arrays.asList(nodes)) +
            ", indicesAndFilters=" + indicesAndFilters +
            '}';
    }
}
