/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank.context;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.search.rank.RankShardResult;

import java.util.List;

/**
 * {@link QueryPhaseRankShardContext} is used to generate the top {@code rank_window_size}
 * results on each shard. It specifies the queries to run during {@code QueryPhase} and is responsible for combining all query scores and
 * order all results through the {@link QueryPhaseRankShardContext#combineQueryPhaseResults} method.
 */
public abstract class QueryPhaseRankShardContext {

    protected final List<Query> queries;
    protected final int rankWindowSize;

    public QueryPhaseRankShardContext(List<Query> queries, int rankWindowSize) {
        this.queries = queries;
        this.rankWindowSize = rankWindowSize;
    }

    public List<Query> queries() {
        return queries;
    }

    public int rankWindowSize() {
        return rankWindowSize;
    }

    /**
     * This is used to reduce the number of required results that are serialized
     * to the coordinating node. Normally we would have to serialize {@code queries * rank_window_size}
     * results, but we can infer that there will likely be overlap of document results. Given that we
     * know any searches that match the same document must be on the same shard, we can sort on the shard
     * instead for a top rank_window_size set of results and reduce the amount of data we serialize.
     */
    public abstract RankShardResult combineQueryPhaseResults(List<TopDocs> rankResults);
}
