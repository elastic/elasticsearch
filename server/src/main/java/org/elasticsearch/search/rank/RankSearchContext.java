/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank;

import org.apache.lucene.search.Query;
import org.elasticsearch.search.aggregations.SearchContextAggregations;
import org.elasticsearch.search.internal.FilteredSearchContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.profile.Profilers;
import org.elasticsearch.search.query.QueryPhase;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.suggest.SuggestionSearchContext;

import java.io.IOException;

/**
 * Manages the appropriate values when executing multiple queries
 * on behalf of ranking.
 *
 * If the rank query is not set, this will behave like a default search
 * context for aggregations and hit tracking with the important exception
 * that size is set to [0]. This allows that query to be run without scoring.
 *
 * The rank query is set for each query in the {@link QueryPhase} and executed
 * a single time with new results. The results must be collected out of
 * {@link QuerySearchResult} after each execution.
 */
public class RankSearchContext extends FilteredSearchContext {

    private Query rankQuery;
    private int windowSize;
    private QuerySearchResult querySearchResult;

    public RankSearchContext(SearchContext in) {
        super(in);
    }

    public void rankQuery(Query rankQuery) throws IOException {
        this.rankQuery = searcher().rewrite(buildFilteredQuery(rankQuery));
        querySearchResult = new QuerySearchResult();
    }

    public void windowSize(int windowSize) {
        this.windowSize = windowSize;
    }

    @Override
    public Query rewrittenQuery() {
        return rankQuery == null ? super.rewrittenQuery() : rankQuery;
    }

    @Override
    public SearchContextAggregations aggregations() {
        return rankQuery == null ? super.aggregations() : null;
    }

    @Override
    public SuggestionSearchContext suggest() {
        return rankQuery == null ? super.suggest() : null;
    }

    @Override
    public int trackTotalHitsUpTo() {
        return rankQuery == null ? super.trackTotalHitsUpTo() : 0;
    }

    @Override
    public Query query() {
        return rankQuery == null ? super.query() : rankQuery;
    }

    @Override
    public int size() {
        return rankQuery == null ? 0 : windowSize;
    }

    @Override
    public boolean explain() {
        return rankQuery == null && super.explain();
    }

    @Override
    public Profilers getProfilers() {
        return rankQuery == null ? super.getProfilers() : null;
    }

    @Override
    public QuerySearchResult queryResult() {
        return querySearchResult == null ? super.queryResult() : querySearchResult;
    }
}
