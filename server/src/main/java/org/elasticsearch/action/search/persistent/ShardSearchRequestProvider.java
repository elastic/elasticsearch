/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search.persistent;

import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchRequest;

import java.util.Map;

public class ShardSearchRequestProvider {
    private final SearchRequest searchRequest;
    private final OriginalIndices originalIndices;
    private final TransportSearchAction.SearchTimeProvider searchTimeProvider;
    private final Map<String, AliasFilter> aliasFiltersByIndex;
    private final Map<String, Float> indicesBoost;

    public ShardSearchRequestProvider(SearchRequest searchRequest,
                                      OriginalIndices originalIndices,
                                      TransportSearchAction.SearchTimeProvider searchTimeProvider,
                                      Map<String, AliasFilter> aliasFiltersByIndex,
                                      Map<String, Float> indicesBoost) {
        this.searchRequest = searchRequest;
        this.originalIndices = originalIndices;
        this.searchTimeProvider = searchTimeProvider;
        this.aliasFiltersByIndex = aliasFiltersByIndex;
        this.indicesBoost = indicesBoost;
    }

    public ShardSearchRequest createRequest(ShardId shardId, int shardIndex, int shardCount) {
        AliasFilter filter = aliasFiltersByIndex.getOrDefault(shardId.getIndexName(), AliasFilter.EMPTY);
        float indexBoost = indicesBoost.getOrDefault(shardId.getIndex().getUUID(), 1.0f);
        ShardSearchRequest shardRequest = new ShardSearchRequest(
            originalIndices,
            searchRequest,
            shardId,
            shardIndex,
            shardCount,
            filter,
            indexBoost,
            searchTimeProvider.getAbsoluteStartMillis(),
            null,
            null,
            null
        );
        shardRequest.canReturnNullResponseIfMatchNoDocs(false);
        return shardRequest;
    }
}
