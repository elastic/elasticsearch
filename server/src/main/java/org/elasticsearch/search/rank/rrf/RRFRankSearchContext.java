/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank.rrf;

import org.apache.lucene.search.Query;
import org.elasticsearch.search.aggregations.SearchContextAggregations;
import org.elasticsearch.search.internal.FilteredSearchContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.profile.Profilers;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.suggest.SuggestionSearchContext;

import java.io.IOException;

/**
 * Manages the appropriate values when executing multiple queries
 * on behalf of RRF.
 *
 * If the rrf query is not set, this will behave like a default search
 * context for aggregations, suggesters, and hit tracking with the
 * important exception that size is set to [0]. This allows that query
 * to be run without scoring.
 *
 * The rrf query needs to be set for each query executed for ranking by
 * the {@link RRFRankSearchContext} while the results are consumed
 * immediately after each query.
 */
public class RRFRankSearchContext extends FilteredSearchContext {

    private Query rrfRankQuery;
    private int windowSize;
    private QuerySearchResult querySearchResult;

    public RRFRankSearchContext(SearchContext in) {
        super(in);
    }

    public void rrfRankQuery(Query rrfRankQuery) throws IOException {
        this.rrfRankQuery = searcher().rewrite(buildFilteredQuery(rrfRankQuery));
        querySearchResult = new QuerySearchResult();
    }

    public void windowSize(int windowSize) {
        this.windowSize = windowSize;
    }

    @Override
    public Query rewrittenQuery() {
        return rrfRankQuery == null ? super.rewrittenQuery() : rrfRankQuery;
    }

    @Override
    public SearchContextAggregations aggregations() {
        return rrfRankQuery == null ? super.aggregations() : null;
    }

    @Override
    public SuggestionSearchContext suggest() {
        return rrfRankQuery == null ? super.suggest() : null;
    }

    @Override
    public int trackTotalHitsUpTo() {
        return rrfRankQuery == null ? super.trackTotalHitsUpTo() : 0;
    }

    @Override
    public Query query() {
        return rrfRankQuery == null ? super.query() : rrfRankQuery;
    }

    @Override
    public int size() {
        return rrfRankQuery == null ? 0 : windowSize;
    }

    @Override
    public boolean explain() {
        return rrfRankQuery == null && super.explain();
    }

    @Override
    public Profilers getProfilers() {
        return rrfRankQuery == null ? super.getProfilers() : null;
    }

    @Override
    public QuerySearchResult queryResult() {
        return querySearchResult == null ? super.queryResult() : querySearchResult;
    }
}
