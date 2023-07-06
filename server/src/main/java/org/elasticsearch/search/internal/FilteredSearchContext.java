/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.internal;

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchShardTask;
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
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.InnerHitsContext;
import org.elasticsearch.search.fetch.subphase.ScriptFieldsContext;
import org.elasticsearch.search.fetch.subphase.highlight.SearchHighlightContext;
import org.elasticsearch.search.profile.Profilers;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rank.RankShardContext;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.suggest.SuggestionSearchContext;

import java.util.List;

public abstract class FilteredSearchContext extends SearchContext {

    private final SearchContext in;

    public FilteredSearchContext(SearchContext in) {
        this.in = in;
    }

    @Override
    public boolean hasStoredFields() {
        return in.hasStoredFields();
    }

    @Override
    public StoredFieldsContext storedFieldsContext() {
        return in.storedFieldsContext();
    }

    @Override
    public SearchContext storedFieldsContext(StoredFieldsContext storedFieldsContext) {
        return in.storedFieldsContext(storedFieldsContext);
    }

    @Override
    public void preProcess() {
        in.preProcess();
    }

    @Override
    public Query buildFilteredQuery(Query query) {
        return in.buildFilteredQuery(query);
    }

    @Override
    public ShardSearchContextId id() {
        return in.id();
    }

    @Override
    public String source() {
        return in.source();
    }

    @Override
    public ShardSearchRequest request() {
        return in.request();
    }

    @Override
    public SearchType searchType() {
        return in.searchType();
    }

    @Override
    public SearchShardTarget shardTarget() {
        return in.shardTarget();
    }

    @Override
    public int numberOfShards() {
        return in.numberOfShards();
    }

    @Override
    public ScrollContext scrollContext() {
        return in.scrollContext();
    }

    @Override
    public SearchContextAggregations aggregations() {
        return in.aggregations();
    }

    @Override
    public SearchContext aggregations(SearchContextAggregations aggregations) {
        return in.aggregations(aggregations);
    }

    @Override
    public SearchHighlightContext highlight() {
        return in.highlight();
    }

    @Override
    public void highlight(SearchHighlightContext highlight) {
        in.highlight(highlight);
    }

    @Override
    public InnerHitsContext innerHits() {
        return in.innerHits();
    }

    @Override
    public SuggestionSearchContext suggest() {
        return in.suggest();
    }

    @Override
    public void suggest(SuggestionSearchContext suggest) {
        in.suggest(suggest);
    }

    @Override
    public RankShardContext rankShardContext() {
        return in.rankShardContext();
    }

    @Override
    public void rankShardContext(RankShardContext rankShardContext) {
        in.rankShardContext(rankShardContext);
    }

    @Override
    public List<RescoreContext> rescore() {
        return in.rescore();
    }

    @Override
    public boolean hasScriptFields() {
        return in.hasScriptFields();
    }

    @Override
    public ScriptFieldsContext scriptFields() {
        return in.scriptFields();
    }

    @Override
    public boolean sourceRequested() {
        return in.sourceRequested();
    }

    @Override
    public boolean hasFetchSourceContext() {
        return in.hasFetchSourceContext();
    }

    @Override
    public FetchSourceContext fetchSourceContext() {
        return in.fetchSourceContext();
    }

    @Override
    public SearchContext fetchSourceContext(FetchSourceContext fetchSourceContext) {
        return in.fetchSourceContext(fetchSourceContext);
    }

    @Override
    public ContextIndexSearcher searcher() {
        return in.searcher();
    }

    @Override
    public IndexShard indexShard() {
        return in.indexShard();
    }

    @Override
    public BitsetFilterCache bitsetFilterCache() {
        return in.bitsetFilterCache();
    }

    @Override
    public TimeValue timeout() {
        return in.timeout();
    }

    @Override
    public void timeout(TimeValue timeout) {
        in.timeout(timeout);
    }

    @Override
    public int terminateAfter() {
        return in.terminateAfter();
    }

    @Override
    public void terminateAfter(int terminateAfter) {
        in.terminateAfter(terminateAfter);
    }

    @Override
    public boolean lowLevelCancellation() {
        return in.lowLevelCancellation();
    }

    @Override
    public SearchContext minimumScore(float minimumScore) {
        return in.minimumScore(minimumScore);
    }

    @Override
    public Float minimumScore() {
        return in.minimumScore();
    }

    @Override
    public SearchContext sort(SortAndFormats sort) {
        return in.sort(sort);
    }

    @Override
    public SortAndFormats sort() {
        return in.sort();
    }

    @Override
    public SearchContext trackScores(boolean trackScores) {
        return in.trackScores(trackScores);
    }

    @Override
    public boolean trackScores() {
        return in.trackScores();
    }

    @Override
    public SearchContext trackTotalHitsUpTo(int trackTotalHitsUpTo) {
        return in.trackTotalHitsUpTo(trackTotalHitsUpTo);
    }

    @Override
    public int trackTotalHitsUpTo() {
        return in.trackTotalHitsUpTo();
    }

    @Override
    public SearchContext searchAfter(FieldDoc searchAfter) {
        return in.searchAfter(searchAfter);
    }

    @Override
    public FieldDoc searchAfter() {
        return in.searchAfter();
    }

    @Override
    public SearchContext parsedPostFilter(ParsedQuery postFilter) {
        return in.parsedPostFilter(postFilter);
    }

    @Override
    public ParsedQuery parsedPostFilter() {
        return in.parsedPostFilter();
    }

    @Override
    public SearchContext parsedQuery(ParsedQuery query) {
        return in.parsedQuery(query);
    }

    @Override
    public ParsedQuery parsedQuery() {
        return in.parsedQuery();
    }

    @Override
    public Query query() {
        return in.query();
    }

    @Override
    public int from() {
        return in.from();
    }

    @Override
    public SearchContext from(int from) {
        return in.from(from);
    }

    @Override
    public int size() {
        return in.size();
    }

    @Override
    public SearchContext size(int size) {
        return in.size(size);
    }

    @Override
    public boolean explain() {
        return in.explain();
    }

    @Override
    public void explain(boolean explain) {
        in.explain(explain);
    }

    @Override
    public List<String> groupStats() {
        return in.groupStats();
    }

    @Override
    public void groupStats(List<String> groupStats) {
        in.groupStats(groupStats);
    }

    @Override
    public boolean version() {
        return in.version();
    }

    @Override
    public void version(boolean version) {
        in.version(version);
    }

    @Override
    public boolean seqNoAndPrimaryTerm() {
        return in.seqNoAndPrimaryTerm();
    }

    @Override
    public void seqNoAndPrimaryTerm(boolean seqNoAndPrimaryTerm) {
        in.seqNoAndPrimaryTerm(seqNoAndPrimaryTerm);
    }

    @Override
    public int[] docIdsToLoad() {
        return in.docIdsToLoad();
    }

    @Override
    public SearchContext docIdsToLoad(int[] docIdsToLoad) {
        return in.docIdsToLoad(docIdsToLoad);
    }

    @Override
    public DfsSearchResult dfsResult() {
        return in.dfsResult();
    }

    @Override
    public void addDfsResult() {
        in.addDfsResult();
    }

    @Override
    public QuerySearchResult queryResult() {
        return in.queryResult();
    }

    @Override
    public void addQueryResult() {
        in.addQueryResult();
    }

    @Override
    public TotalHits getTotalHits() {
        return in.getTotalHits();
    }

    @Override
    public float getMaxScore() {
        return in.getMaxScore();
    }

    @Override
    public FetchSearchResult fetchResult() {
        return in.fetchResult();
    }

    @Override
    public void addFetchResult() {
        in.addFetchResult();
    }

    @Override
    public FetchPhase fetchPhase() {
        return in.fetchPhase();
    }

    @Override
    public long getRelativeTimeInMillis() {
        return in.getRelativeTimeInMillis();
    }

    @Override
    public void addSearchExt(SearchExtBuilder searchExtBuilder) {
        in.addSearchExt(searchExtBuilder);
    }

    @Override
    public SearchExtBuilder getSearchExt(String name) {
        return in.getSearchExt(name);
    }

    @Override
    public Profilers getProfilers() {
        return in.getProfilers();
    }

    @Override
    public SearchExecutionContext getSearchExecutionContext() {
        return in.getSearchExecutionContext();
    }

    @Override
    public void setTask(SearchShardTask task) {
        in.setTask(task);
    }

    @Override
    public SearchShardTask getTask() {
        return in.getTask();
    }

    @Override
    public boolean isCancelled() {
        return in.isCancelled();
    }

    @Override
    public SearchContext collapse(CollapseContext collapse) {
        return in.collapse(collapse);
    }

    @Override
    public CollapseContext collapse() {
        return in.collapse();
    }

    @Override
    public void addRescore(RescoreContext rescore) {
        in.addRescore(rescore);
    }

    @Override
    public ReaderContext readerContext() {
        return in.readerContext();
    }

    @Override
    public SourceLoader newSourceLoader() {
        return in.newSourceLoader();
    }

    @Override
    public IdLoader newIdLoader() {
        return in.newIdLoader();
    }
}
