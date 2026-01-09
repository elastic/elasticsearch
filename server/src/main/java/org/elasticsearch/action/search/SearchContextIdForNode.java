/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.internal.ShardSearchContextId;

import java.io.IOException;
import java.util.Objects;

public final class SearchContextIdForNode implements Writeable {
    private final String node;
    private final ShardSearchContextId searchContextId;
    private final String clusterAlias;

    /**
     * Contains the details required to retrieve a {@link ShardSearchContextId} for a shard on a specific node.
     *
     * @param clusterAlias The alias of the cluster, or {@code null} if the shard is local.
     * @param node The target node where the search context ID is defined, or {@code null} if the shard is missing or unavailable.
     * @param searchContextId The {@link ShardSearchContextId}, or {@code null} if the shard is missing or unavailable.
     */
    SearchContextIdForNode(@Nullable String clusterAlias, @Nullable String node, @Nullable ShardSearchContextId searchContextId) {
        this.node = node;
        this.clusterAlias = clusterAlias;
        this.searchContextId = searchContextId;
    }

    SearchContextIdForNode(StreamInput in) throws IOException {
        this.node = in.readOptionalString();
        this.clusterAlias = in.readOptionalString();
        this.searchContextId = in.readOptionalWriteable(ShardSearchContextId::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(node);
        out.writeOptionalString(clusterAlias);
        out.writeOptionalWriteable(searchContextId);
    }

    @Nullable
    public String getNode() {
        return node;
    }

    @Nullable
    public String getClusterAlias() {
        return clusterAlias;
    }

    @Nullable
    public ShardSearchContextId getSearchContextId() {
        return searchContextId;
    }

    @Override
    public String toString() {
        return "SearchContextIdForNode{"
            + "node='"
            + node
            + '\''
            + ", seachContextId="
            + searchContextId
            + ", clusterAlias='"
            + clusterAlias
            + '\''
            + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        SearchContextIdForNode that = (SearchContextIdForNode) o;
        return Objects.equals(node, that.node)
            && Objects.equals(searchContextId, that.searchContextId)
            && Objects.equals(clusterAlias, that.clusterAlias);
    }

    @Override
    public int hashCode() {
        return Objects.hash(node, searchContextId, clusterAlias);
    }
}
