/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;

import java.util.Comparator;
import java.util.Objects;

/**
 * A class that encapsulates the {@link ShardId} and the cluster alias
 * of a shard used during the search action.
 */
public final class SearchShard implements Comparable<SearchShard> {
    @Nullable
    private final String clusterAlias;
    private final ShardId shardId;

    public SearchShard(@Nullable String clusterAlias, ShardId shardId) {
        this.clusterAlias = clusterAlias;
        this.shardId = shardId;
    }

    /**
     * Return the cluster alias if we are executing a cross cluster search request, <code>null</code> otherwise.
     */
    @Nullable
    public String getClusterAlias() {
        return clusterAlias;
    }

    /**
     * Return the {@link ShardId} of this shard.
     */
    public ShardId getShardId() {
        return shardId;
    }

    @Override
    public int compareTo(SearchShard o) {
        int cmp = Objects.compare(clusterAlias, o.clusterAlias, Comparator.nullsFirst(Comparator.naturalOrder()));
        return cmp != 0 ? cmp : shardId.compareTo(o.shardId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchShard that = (SearchShard) o;
        return Objects.equals(clusterAlias, that.clusterAlias)
            && shardId.equals(that.shardId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterAlias, shardId);
    }

    @Override
    public String toString() {
        return "SearchShard{" +
            "clusterAlias='" + clusterAlias + '\'' +
            ", shardId=" + shardId +
            '}';
    }
}
