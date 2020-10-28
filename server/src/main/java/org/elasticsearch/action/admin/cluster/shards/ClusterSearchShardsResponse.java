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

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.internal.AliasFilter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class ClusterSearchShardsResponse extends ActionResponse implements ToXContentObject {

    private final ClusterSearchShardsGroup[] groups;
    private final DiscoveryNode[] nodes;
    private final Map<String, AliasFilter> indicesAndFilters;

    public ClusterSearchShardsResponse(StreamInput in) throws IOException {
        super(in);
        groups = in.readArray(ClusterSearchShardsGroup::new, ClusterSearchShardsGroup[]::new);
        nodes = in.readArray(DiscoveryNode::new, DiscoveryNode[]::new);
        indicesAndFilters = in.readMap(StreamInput::readString, AliasFilter::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeArray(groups);
        out.writeArray(nodes);
        out.writeMap(indicesAndFilters, StreamOutput::writeString, (o, s) -> s.writeTo(o));
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
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject("nodes");
        for (DiscoveryNode node : nodes) {
            node.toXContent(builder, params);
        }
        builder.endObject();
        if (indicesAndFilters != null) {
            builder.startObject("indices");
            for (Map.Entry<String, AliasFilter> entry : indicesAndFilters.entrySet()) {
                String index = entry.getKey();
                builder.startObject(index);
                AliasFilter aliasFilter = entry.getValue();
                String[] aliases = aliasFilter.getAliases();
                if (aliases.length > 0) {
                    Arrays.sort(aliases); // we want consistent ordering here and these values might be generated from a set / map
                    builder.array("aliases", aliases);
                    if (aliasFilter.getQueryBuilder() != null) { // might be null if we include non-filtering aliases
                        builder.field("filter");
                        aliasFilter.getQueryBuilder().toXContent(builder, params);
                    }
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.startArray("shards");
        for (ClusterSearchShardsGroup group : groups) {
            group.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }
}
