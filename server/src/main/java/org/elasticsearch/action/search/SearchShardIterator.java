/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.cluster.routing.PlainShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.util.Countable;
import org.elasticsearch.common.util.PlainIterator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.ShardSearchContextId;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Extension of {@link PlainShardIterator} used in the search api, which also holds the {@link OriginalIndices}
 * of the search request (useful especially with cross-cluster search, as each cluster has its own set of original indices) as well as
 * the cluster alias.
 * @see OriginalIndices
 */
public final class SearchShardIterator implements Comparable<SearchShardIterator>, Countable {

    private final OriginalIndices originalIndices;
    private final String clusterAlias;
    private final ShardId shardId;
    private boolean skip;
    private final boolean prefiltered;

    private final ShardSearchContextId searchContextId;
    private final TimeValue searchContextKeepAlive;
    private final PlainIterator<String> targetNodesIterator;

    /**
     * Creates a {@link PlainShardIterator} instance that iterates over a subset of the given shards
     * this the a given <code>shardId</code>.
     *
     * @param clusterAlias    the alias of the cluster where the shard is located
     * @param shardId         shard id of the group
     * @param shards          shards to iterate
     * @param originalIndices the indices that the search request originally related to (before any rewriting happened)
     */
    public SearchShardIterator(@Nullable String clusterAlias, ShardId shardId, List<ShardRouting> shards, OriginalIndices originalIndices) {
        this(clusterAlias, shardId, shards.stream().map(ShardRouting::currentNodeId).toList(), originalIndices, null, null, false, false);
    }

    /**
     * Creates a {@link PlainShardIterator} instance that iterates over a subset of the given shards
     *
     * @param clusterAlias           the alias of the cluster where the shard is located
     * @param shardId                shard id of the group
     * @param targetNodeIds          the list of nodes hosting shard copies
     * @param originalIndices        the indices that the search request originally related to (before any rewriting happened)
     * @param searchContextId        the point-in-time specified for this group if exists
     * @param searchContextKeepAlive the time interval that data nodes should extend the keep alive of the point-in-time
     * @param prefiltered            if true, then this group already executed the can_match phase
     * @param skip                   if true, then this group won't have matches, and it can be safely skipped from the search
     */
    public SearchShardIterator(
        @Nullable String clusterAlias,
        ShardId shardId,
        List<String> targetNodeIds,
        OriginalIndices originalIndices,
        ShardSearchContextId searchContextId,
        TimeValue searchContextKeepAlive,
        boolean prefiltered,
        boolean skip
    ) {
        this.shardId = shardId;
        this.targetNodesIterator = new PlainIterator<>(targetNodeIds);
        this.originalIndices = originalIndices;
        this.clusterAlias = clusterAlias;
        this.searchContextId = searchContextId;
        this.searchContextKeepAlive = searchContextKeepAlive;
        assert searchContextKeepAlive == null || searchContextId != null;
        this.prefiltered = prefiltered;
        this.skip = skip;
        assert skip == false || prefiltered : "only prefiltered shards are skip-able";
    }

    /**
     * Returns the original indices associated with this shard iterator, specifically with the cluster that this shard belongs to.
     */
    public OriginalIndices getOriginalIndices() {
        return originalIndices;
    }

    /**
     * Returns the alias of the cluster where the shard is located.
     */
    @Nullable
    public String getClusterAlias() {
        return clusterAlias;
    }

    SearchShardTarget nextOrNull() {
        final String nodeId = targetNodesIterator.nextOrNull();
        if (nodeId != null) {
            return new SearchShardTarget(nodeId, shardId, clusterAlias);
        }
        return null;
    }

    int remaining() {
        return targetNodesIterator.remaining();
    }

    /**
     * Returns a non-null value if this request should use a specific search context instead of the latest one.
     */
    ShardSearchContextId getSearchContextId() {
        return searchContextId;
    }

    TimeValue getSearchContextKeepAlive() {
        return searchContextKeepAlive;
    }

    List<String> getTargetNodeIds() {
        return targetNodesIterator.asList();
    }

    void reset() {
        targetNodesIterator.reset();
    }

    /**
     * Returns <code>true</code> if the search execution should skip this shard since it can not match any documents given the query.
     */
    boolean skip() {
        return skip;
    }

    /**
     * Specifies if the search execution should skip this shard copies
     */
    void skip(boolean skip) {
        this.skip = skip;
    }

    /**
     * Returns {@code true} if this iterator was applied pre-filtered
     */
    boolean prefiltered() {
        return prefiltered;
    }

    @Override
    public int size() {
        return targetNodesIterator.size();
    }

    ShardId shardId() {
        return shardId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchShardIterator that = (SearchShardIterator) o;
        return shardId.equals(that.shardId) && Objects.equals(clusterAlias, that.clusterAlias);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterAlias, shardId);
    }

    private static final Comparator<SearchShardIterator> COMPARATOR = Comparator.comparing(SearchShardIterator::shardId)
        .thenComparing(SearchShardIterator::getClusterAlias, Comparator.nullsFirst(String::compareTo));

    @Override
    public int compareTo(SearchShardIterator o) {
        return COMPARATOR.compare(this, o);
    }
}
