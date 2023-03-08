/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;

/**
 * Used to indicate to {@link DefaultSearchContext} what {@link SearchPhaseResult} sub-class we're expecting.  This should roughly
 * correspond to the phase being executed.  There is an option included for NONE, because there are many places we create a search context
 * but never ask it for search results.  Often this is in testing, but there are production uses of this as well.
 */
public enum ResultsMode {
    DFS {
        @Override
        public SearchPhaseResult createSearchPhaseResult(
            ShardSearchContextId contextId,
            SearchShardTarget shardTarget,
            ShardSearchRequest shardSearchRequest
        ) {
            return new DfsSearchResult(contextId, shardTarget, shardSearchRequest);
        }
    },
    QUERY {
        @Override
        public SearchPhaseResult createSearchPhaseResult(
            ShardSearchContextId contextId,
            SearchShardTarget shardTarget,
            ShardSearchRequest shardSearchRequest
        ) {
            return new QuerySearchResult(contextId, shardTarget, shardSearchRequest);
        }
    },
    FETCH {
        @Override
        public SearchPhaseResult createSearchPhaseResult(
            ShardSearchContextId contextId,
            SearchShardTarget shardTarget,
            ShardSearchRequest shardSearchRequest
        ) {
            return new FetchSearchResult(contextId, shardTarget);
        }
    },
    NONE {
        @Override
        public SearchPhaseResult createSearchPhaseResult(
            ShardSearchContextId contextId,
            SearchShardTarget shardTarget,
            ShardSearchRequest shardSearchRequest
        ) {
            return null;
        }
    };

    public abstract SearchPhaseResult createSearchPhaseResult(
        ShardSearchContextId contextId,
        SearchShardTarget shardTarget,
        ShardSearchRequest shardSearchRequest
    );
}
