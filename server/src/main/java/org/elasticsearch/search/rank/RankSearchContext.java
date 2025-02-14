/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.rank;

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.mapper.IdLoader;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.search.SearchExtBuilder;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.SearchContextAggregations;
import org.elasticsearch.search.collapse.CollapseContext;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.fetch.subphase.FetchDocValuesContext;
import org.elasticsearch.search.fetch.subphase.FetchFieldsContext;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.InnerHitsContext;
import org.elasticsearch.search.fetch.subphase.ScriptFieldsContext;
import org.elasticsearch.search.fetch.subphase.highlight.SearchHighlightContext;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.search.internal.ScrollContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.profile.Profilers;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rank.context.QueryPhaseRankShardContext;
import org.elasticsearch.search.rank.feature.RankFeatureResult;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.suggest.SuggestionSearchContext;
import org.elasticsearch.tasks.CancellableTask;

import java.util.List;

/**
 * Manages the appropriate values when executing multiple queries
 * on behalf of ranking for a single ranking query.
 */
public class RankSearchContext extends SearchContext {

    private final SearchContext parent;
    private final Query rankQuery;
    private final int rankWindowSize;
    private final QuerySearchResult querySearchResult;

    @SuppressWarnings("this-escape")
    public RankSearchContext(SearchContext parent, Query rankQuery, int rankWindowSize) {
        this.parent = parent;
        this.rankQuery = parent.buildFilteredQuery(rankQuery);
        this.rankWindowSize = rankWindowSize;
        this.querySearchResult = new QuerySearchResult(parent.readerContext().id(), parent.shardTarget(), parent.request());
        this.addReleasable(querySearchResult::decRef);
    }

    @Override
    public ShardSearchRequest request() {
        return parent.request();
    }

    @Override
    public SearchShardTarget shardTarget() {
        return parent.shardTarget();
    }

    /**
     * Ranking is not allowed with scroll.
     */
    @Override
    public ScrollContext scrollContext() {
        return null;
    }

    /**
     * Aggregations are run as a separate query.
     */
    @Override
    public SearchContextAggregations aggregations() {
        return null;
    }

    /**
     * Rescore is not supported by ranking.
     */
    @Override
    public List<RescoreContext> rescore() {
        return List.of();
    }

    @Override
    public ContextIndexSearcher searcher() {
        return parent.searcher();
    }

    @Override
    public IndexShard indexShard() {
        return parent.indexShard();
    }

    @Override
    public TimeValue timeout() {
        return parent.timeout();
    }

    @Override
    public int terminateAfter() {
        return parent.terminateAfter();
    }

    @Override
    public Float minimumScore() {
        return parent.minimumScore();
    }

    /**
     * Sort is not allowed with ranking.
     */
    @Override
    public SortAndFormats sort() {
        return null;
    }

    @Override
    public boolean trackScores() {
        return parent.trackScores();
    }

    /**
     * Total hits are tracked as part of a separate query.
     */
    @Override
    public int trackTotalHitsUpTo() {
        return 0;
    }

    @Override
    public FieldDoc searchAfter() {
        return parent.searchAfter();
    }

    /**
     * Collapse is not supported by ranking.
     */
    @Override
    public CollapseContext collapse() {
        return null;
    }

    @Override
    public ParsedQuery parsedPostFilter() {
        return parent.parsedPostFilter();
    }

    /**
     * Use a single rank query.
     */
    @Override
    public Query query() {
        return rankQuery;
    }

    @Override
    public int from() {
        return parent.from();
    }

    @Override
    public int size() {
        return rankWindowSize;
    }

    /**
     * Use a separate query search result.
     */
    @Override
    public QuerySearchResult queryResult() {
        return querySearchResult;
    }

    /**
     * Profiling is not supported by ranking.
     */
    @Override
    public Profilers getProfilers() {
        return null;
    }

    @Override
    public long getRelativeTimeInMillis() {
        return parent.getRelativeTimeInMillis();
    }

    /* ---- ALL METHODS ARE UNSUPPORTED BEYOND HERE ---- */

    @Override
    public void setTask(CancellableTask task) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CancellableTask getTask() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCancelled() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void preProcess() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Query buildFilteredQuery(Query query) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ShardSearchContextId id() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String source() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SearchType searchType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int numberOfShards() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SearchContext aggregations(SearchContextAggregations aggregations) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SearchExtBuilder getSearchExt(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SearchHighlightContext highlight() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void highlight(SearchHighlightContext highlight) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InnerHitsContext innerHits() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SuggestionSearchContext suggest() {
        throw new UnsupportedOperationException();
    }

    public void suggest(SuggestionSearchContext suggest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryPhaseRankShardContext queryPhaseRankShardContext() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void queryPhaseRankShardContext(QueryPhaseRankShardContext queryPhaseRankShardContext) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addRescore(RescoreContext rescore) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasScriptFields() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ScriptFieldsContext scriptFields() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean sourceRequested() {
        throw new UnsupportedOperationException();
    }

    @Override
    public FetchSourceContext fetchSourceContext() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SearchContext fetchSourceContext(FetchSourceContext fetchSourceContext) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FetchDocValuesContext docValuesContext() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SearchContext docValuesContext(FetchDocValuesContext docValuesContext) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FetchFieldsContext fetchFieldsContext() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SearchContext fetchFieldsContext(FetchFieldsContext fetchFieldsContext) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BitsetFilterCache bitsetFilterCache() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void terminateAfter(int terminateAfter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean lowLevelCancellation() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SearchContext minimumScore(float minimumScore) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SearchContext sort(SortAndFormats sort) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SearchContext trackScores(boolean trackScores) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SearchContext trackTotalHitsUpTo(int trackTotalHits) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SearchContext searchAfter(FieldDoc searchAfter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SearchContext parsedPostFilter(ParsedQuery postFilter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SearchContext parsedQuery(ParsedQuery query) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ParsedQuery parsedQuery() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SearchContext from(int from) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SearchContext size(int size) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasStoredFields() {
        throw new UnsupportedOperationException();
    }

    @Override
    public StoredFieldsContext storedFieldsContext() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SearchContext storedFieldsContext(StoredFieldsContext storedFieldsContext) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean explain() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void explain(boolean explain) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> groupStats() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean version() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void version(boolean version) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean seqNoAndPrimaryTerm() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void seqNoAndPrimaryTerm(boolean seqNoAndPrimaryTerm) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DfsSearchResult dfsResult() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addDfsResult() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addQueryResult() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TotalHits getTotalHits() {
        throw new UnsupportedOperationException();
    }

    @Override
    public float getMaxScore() {
        throw new UnsupportedOperationException();
    }

    @Override
    public FetchPhase fetchPhase() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addRankFeatureResult() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RankFeatureResult rankFeatureResult() {
        throw new UnsupportedOperationException();
    }

    @Override
    public FetchSearchResult fetchResult() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addFetchResult() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SearchExecutionContext getSearchExecutionContext() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ReaderContext readerContext() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SourceLoader newSourceLoader() {
        throw new UnsupportedOperationException();
    }

    @Override
    public IdLoader newIdLoader() {
        throw new UnsupportedOperationException();
    }
}
