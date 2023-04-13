/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank;

import org.apache.lucene.search.Query;
import org.elasticsearch.search.internal.FilteredSearchContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.query.QueryPhase;
import org.elasticsearch.search.query.QuerySearchResult;

import java.io.IOException;

/**
 * Manages the appropriate values when executing multiple queries
 * on behalf of ranking.
 * <br>
 * The rank query is set for each query in the {@link QueryPhase} and executed
 * a single time with new results. The results must be collected out of
 * {@link QuerySearchResult} after each execution.
 */
public class RankSearchContext extends FilteredSearchContext {

    private Query rankQuery;
    private final int windowSize;
    private QuerySearchResult querySearchResult;

    public RankSearchContext(SearchContext in, int windowSize) {
        super(in);
        this.windowSize = windowSize;
    }

    public void rankQuery(Query rankQuery) throws IOException {
        this.rankQuery = searcher().rewrite(buildFilteredQuery(rankQuery));
        querySearchResult = new QuerySearchResult();
    }

    @Override
    public Query rewrittenQuery() {
        return rankQuery;
    }

    @Override
    public int trackTotalHitsUpTo() {
        return 0;
    }

    @Override
    public Query query() {
        return rankQuery;
    }

    @Override
    public int size() {
        return windowSize;
    }

    @Override
    public QuerySearchResult queryResult() {
        return querySearchResult;
    }
}
