/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank.context;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.action.search.SearchPhaseController.TopDocsStats;
import org.elasticsearch.search.query.QuerySearchResult;

import java.util.List;

/**
 * {@link QueryPhaseRankCoordinatorContext} is running on the coordinator node and is
 * responsible for combining the query phase results from the shards and rank them accordingly.
 * The output is a `rank_window_size` ranked list of ordered results from all shards.
 * Note: Currently this can use only sort by score; sort by field is not supported.
 */
public abstract class QueryPhaseRankCoordinatorContext {

    protected final int rankWindowSize;

    public QueryPhaseRankCoordinatorContext(int rankWindowSize) {
        this.rankWindowSize = rankWindowSize;
    }

    /**
     * This is used to pull information passed back from the shards as part of {@link QuerySearchResult#getRankShardResult()}
     * and return a {@link ScoreDoc[]} of the `rank_window_size` ranked results. Note that {@link TopDocsStats} is included so that
     * appropriate stats may be updated based on rank results.
     * This is called when reducing query results through {@code SearchPhaseController#reducedQueryPhase()}.
     */
    public abstract ScoreDoc[] rankQueryPhaseResults(List<QuerySearchResult> querySearchResults, TopDocsStats topDocStats);
}
