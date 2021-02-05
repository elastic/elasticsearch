/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.persistent.PersistentSearchResponse;

import java.util.ArrayList;
import java.util.List;

class PersistentSearchResponseMerger {
    private final String searchId;
    private final long expirationTime;
    private final SearchResponseMerger searchResponseMerger;
    @Nullable
    private final PersistentSearchResponse baseResponse;
    private final List<Integer> reducedShardsIndex;

    PersistentSearchResponseMerger(String searchId,
                                   long expirationTime,
                                   TransportSearchAction.SearchTimeProvider searchTimeProvider,
                                   InternalAggregation.ReduceContextBuilder aggReduceContextBuilder,
                                   boolean performFinalReduce,
                                   PersistentSearchResponse baseResponse) {
        this.searchId = searchId;
        this.expirationTime = expirationTime;
        this.searchResponseMerger = new SearchResponseMerger(SearchService.DEFAULT_FROM,
            SearchService.DEFAULT_SIZE,
            SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO,
            searchTimeProvider,
            aggReduceContextBuilder,
            performFinalReduce
        );
        this.baseResponse = baseResponse;
        this.reducedShardsIndex = new ArrayList<>();
        if (baseResponse != null) {
            searchResponseMerger.add(baseResponse.getSearchResponse());
            addReducedShardIndices(baseResponse);
        }
    }

    void addResponse(PersistentSearchResponse searchResponse) {
        searchResponseMerger.add(searchResponse.getSearchResponse());
        addReducedShardIndices(searchResponse);
    }

    PersistentSearchResponse getMergedResponse() {
        final SearchResponse mergedResponse = searchResponseMerger.getMergedResponse(SearchResponse.Clusters.EMPTY);

        return new PersistentSearchResponse(searchId,
            searchId,
            mergedResponse,
            expirationTime,
            List.copyOf(reducedShardsIndex),
            baseResponse == null ? 0 : baseResponse.getVersion() + 1
        );
    }

    private void addReducedShardIndices(PersistentSearchResponse response) {
        reducedShardsIndex.addAll(response.getReducedShardIndices());
    }
}
