/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.AliasFilter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A response of {@link SearchShardsRequest} which contains the target shards grouped by {@link org.elasticsearch.index.shard.ShardId}
 */
public final class SearchShardsResponse extends ActionResponse {
    private final Collection<SearchShardsGroup> groups;
    private final Collection<DiscoveryNode> nodes;
    private final Map<String, AliasFilter> aliasFilters;

    public SearchShardsResponse(
        Collection<SearchShardsGroup> groups,
        Collection<DiscoveryNode> nodes,
        Map<String, AliasFilter> aliasFilters
    ) {
        this.groups = groups;
        this.nodes = nodes;
        this.aliasFilters = aliasFilters;
    }

    public SearchShardsResponse(StreamInput in) throws IOException {
        super(in);
        this.groups = in.readList(SearchShardsGroup::new);
        this.nodes = in.readList(DiscoveryNode::new);
        this.aliasFilters = in.readMap(StreamInput::readString, AliasFilter::readFrom);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(groups);
        out.writeCollection(nodes);
        out.writeMap(aliasFilters, StreamOutput::writeString, (o, v) -> v.writeTo(o));
    }

    /**
     * List of nodes in the cluster
     */
    public Collection<DiscoveryNode> getNodes() {
        return nodes;
    }

    /**
     * List of target shards grouped by ShardId
     */
    public Collection<SearchShardsGroup> getGroups() {
        return groups;
    }

    /**
     * A map from index uuid to alias filters
     */
    public Map<String, AliasFilter> getAliasFilters() {
        return aliasFilters;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchShardsResponse that = (SearchShardsResponse) o;
        return groups.equals(that.groups) && nodes.equals(that.nodes) && aliasFilters.equals(that.aliasFilters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groups, nodes, aliasFilters);
    }

    static SearchShardsResponse fromLegacyResponse(ClusterSearchShardsResponse oldResp) {
        Map<String, Index> indexByNames = new HashMap<>();
        for (ClusterSearchShardsGroup oldGroup : oldResp.getGroups()) {
            ShardId shardId = oldGroup.getShardId();
            indexByNames.put(shardId.getIndexName(), shardId.getIndex());
        }
        // convert index_name -> alias_filters to index_uuid -> alias_filters
        Map<String, AliasFilter> aliasFilters = Maps.newMapWithExpectedSize(oldResp.getIndicesAndFilters().size());
        for (Map.Entry<String, AliasFilter> e : oldResp.getIndicesAndFilters().entrySet()) {
            Index index = indexByNames.get(e.getKey());
            aliasFilters.put(index.getUUID(), e.getValue());
        }
        List<SearchShardsGroup> groups = Arrays.stream(oldResp.getGroups()).map(SearchShardsGroup::new).toList();
        assert groups.stream().noneMatch(SearchShardsGroup::preFiltered) : "legacy responses must not have preFiltered set";
        return new SearchShardsResponse(groups, Arrays.asList(oldResp.getNodes()), aliasFilters);
    }
}
