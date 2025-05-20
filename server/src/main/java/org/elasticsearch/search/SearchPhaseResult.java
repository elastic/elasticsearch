/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rank.feature.RankFeatureResult;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;

/**
 * This class is a base class for all search related results. It contains the shard target it
 * was executed against, a shard index used to reference the result on the coordinating node
 * and a request ID that is used to reference the request context on the executing node. The
 * request ID is particularly important since it is used to reference and maintain a context
 * across search phases to ensure the same point in time snapshot is used for querying and
 * fetching etc.
 */
public abstract class SearchPhaseResult extends TransportResponse {

    private SearchShardTarget searchShardTarget;
    private int shardIndex = -1;
    protected ShardSearchContextId contextId;
    private ShardSearchRequest shardSearchRequest;
    private RescoreDocIds rescoreDocIds = RescoreDocIds.EMPTY;

    protected SearchPhaseResult() {}

    /**
     * Specifies whether the specific search phase results are associated with an opened SearchContext on the shards that
     * executed the request.
     */
    public boolean hasSearchContext() {
        return false;
    }

    /**
     * Returns the search context ID that is used to reference the search context on the executing node
     * or <code>null</code> if no context was created.
     */
    @Nullable
    public ShardSearchContextId getContextId() {
        return contextId;
    }

    /**
     * Null out the context id and request tracked in this instance. This is used to mark shards for which merging results on the data node
     * made it clear that their search context won't be used in the fetch phase.
     */
    public void clearContextId() {
        this.shardSearchRequest = null;
        this.contextId = null;
    }

    /**
     * Returns the shard index in the context of the currently executing search request that is
     * used for accounting on the coordinating node
     */
    public int getShardIndex() {
        assert shardIndex != -1 : "shardIndex is not set";
        return shardIndex;
    }

    public SearchShardTarget getSearchShardTarget() {
        return searchShardTarget;
    }

    public void setSearchShardTarget(SearchShardTarget shardTarget) {
        this.searchShardTarget = shardTarget;
    }

    public void setShardIndex(int shardIndex) {
        assert shardIndex >= 0 : "shardIndex must be >= 0 but was: " + shardIndex;
        this.shardIndex = shardIndex;
    }

    /**
     * Returns the query result iff it's included in this response otherwise <code>null</code>
     */
    public QuerySearchResult queryResult() {
        return null;
    }

    /**
     * Returns the rank feature result iff it's included in this response otherwise <code>null</code>
     */
    public RankFeatureResult rankFeatureResult() {
        return null;
    }

    /**
     * Returns the fetch result iff it's included in this response otherwise <code>null</code>
     */
    public FetchSearchResult fetchResult() {
        return null;
    }

    @Nullable
    public ShardSearchRequest getShardSearchRequest() {
        return shardSearchRequest;
    }

    public void setShardSearchRequest(ShardSearchRequest shardSearchRequest) {
        this.shardSearchRequest = shardSearchRequest;
    }

    public RescoreDocIds getRescoreDocIds() {
        return rescoreDocIds;
    }

    public void setRescoreDocIds(RescoreDocIds rescoreDocIds) {
        this.rescoreDocIds = rescoreDocIds;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // TODO: this seems wrong, SearchPhaseResult should have a writeTo?
    }
}
