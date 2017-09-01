/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.test;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Counter;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.search.SearchExtBuilder;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.SearchContextAggregations;
import org.elasticsearch.search.collapse.CollapseContext;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.fetch.subphase.DocValueFieldsContext;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.ScriptFieldsContext;
import org.elasticsearch.search.fetch.subphase.highlight.SearchContextHighlight;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.ScrollContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.profile.Profilers;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.suggest.SuggestionSearchContext;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestSearchContext extends SearchContext {

    final BigArrays bigArrays;
    final IndexService indexService;
    final BitsetFilterCache fixedBitSetFilterCache;
    final ThreadPool threadPool;
    final Map<Class<?>, Collector> queryCollectors = new HashMap<>();
    final IndexShard indexShard;
    final Counter timeEstimateCounter = Counter.newCounter();
    final QuerySearchResult queryResult = new QuerySearchResult();
    final QueryShardContext queryShardContext;
    ParsedQuery originalQuery;
    ParsedQuery postFilter;
    Query query;
    Float minScore;
    SearchTask task;
    SortAndFormats sort;
    boolean trackScores = false;
    boolean trackTotalHits = true;

    ContextIndexSearcher searcher;
    int size;
    private int terminateAfter = DEFAULT_TERMINATE_AFTER;
    private SearchContextAggregations aggregations;
    private ScrollContext scrollContext;

    private final long originNanoTime = System.nanoTime();
    private final Map<String, SearchExtBuilder> searchExtBuilders = new HashMap<>();

    public TestSearchContext(ThreadPool threadPool, BigArrays bigArrays, IndexService indexService) {
        this.bigArrays = bigArrays.withCircuitBreaking();
        this.indexService = indexService;
        this.fixedBitSetFilterCache = indexService.cache().bitsetFilterCache();
        this.threadPool = threadPool;
        this.indexShard = indexService.getShardOrNull(0);
        queryShardContext = indexService.newQueryShardContext(0, null, () -> 0L, null);
    }

    public TestSearchContext(QueryShardContext queryShardContext) {
        this(queryShardContext, null);
    }

    public TestSearchContext(QueryShardContext queryShardContext, IndexShard indexShard) {
        this.bigArrays = null;
        this.indexService = null;
        this.threadPool = null;
        this.fixedBitSetFilterCache = null;
        this.indexShard = indexShard;
        this.queryShardContext = queryShardContext;
    }

    @Override
    public void preProcess(boolean rewrite) {
    }

    @Override
    public Query buildFilteredQuery(Query query) {
        return null;
    }

    @Override
    public long id() {
        return 0;
    }

    @Override
    public String source() {
        return null;
    }

    @Override
    public ShardSearchRequest request() {
        return null;
    }

    @Override
    public SearchType searchType() {
        return null;
    }

    @Override
    public SearchShardTarget shardTarget() {
        return null;
    }

    @Override
    public int numberOfShards() {
        return 1;
    }

    @Override
    public float queryBoost() {
        return 0;
    }

    @Override
    public long getOriginNanoTime() {
        return originNanoTime;
    }

    @Override
    public ScrollContext scrollContext() {
        return scrollContext;
    }

    @Override
    public SearchContext scrollContext(ScrollContext scrollContext) {
        this.scrollContext = scrollContext;
        return this;
    }

    @Override
    public SearchContextAggregations aggregations() {
        return aggregations;
    }

    @Override
    public SearchContext aggregations(SearchContextAggregations aggregations) {
        this.aggregations = aggregations;
        return this;
    }

    @Override
    public void addSearchExt(SearchExtBuilder searchExtBuilder) {
        searchExtBuilders.put(searchExtBuilder.getWriteableName(), searchExtBuilder);
    }

    @Override
    public SearchExtBuilder getSearchExt(String name) {
        return searchExtBuilders.get(name);
    }

    @Override
    public SearchContextHighlight highlight() {
        return null;
    }

    @Override
    public void highlight(SearchContextHighlight highlight) {
    }

    @Override
    public SuggestionSearchContext suggest() {
        return null;
    }

    @Override
    public void suggest(SuggestionSearchContext suggest) {
    }

    @Override
    public List<RescoreContext> rescore() {
        return Collections.emptyList();
    }

    @Override
    public void addRescore(RescoreContext rescore) {
    }

    @Override
    public boolean hasScriptFields() {
        return false;
    }

    @Override
    public ScriptFieldsContext scriptFields() {
        return null;
    }

    @Override
    public boolean sourceRequested() {
        return false;
    }

    @Override
    public boolean hasFetchSourceContext() {
        return false;
    }

    @Override
    public FetchSourceContext fetchSourceContext() {
        return null;
    }

    @Override
    public SearchContext fetchSourceContext(FetchSourceContext fetchSourceContext) {
        return null;
    }

    @Override
    public DocValueFieldsContext docValueFieldsContext() {
        return null;
    }

    @Override
    public SearchContext docValueFieldsContext(DocValueFieldsContext docValueFieldsContext) {
        return null;
    }

    @Override
    public ContextIndexSearcher searcher() {
        return searcher;
    }

    public void setSearcher(Engine.Searcher searcher) {
        this.searcher = new ContextIndexSearcher(searcher, indexService.cache().query(), indexShard.getQueryCachingPolicy());
    }

    @Override
    public IndexShard indexShard() {
        return indexShard;
    }

    @Override
    public MapperService mapperService() {
        if (indexService != null) {
            return indexService.mapperService();
        }
        return null;
    }

    @Override
    public SimilarityService similarityService() {
        return null;
    }

    @Override
    public BigArrays bigArrays() {
        return bigArrays;
    }

    @Override
    public BitsetFilterCache bitsetFilterCache() {
        return fixedBitSetFilterCache;
    }

    @Override
    public <IFD extends IndexFieldData<?>> IFD getForField(MappedFieldType fieldType) {
        return queryShardContext.getForField(fieldType);
    }

    @Override
    public TimeValue timeout() {
        return TimeValue.ZERO;
    }

    @Override
    public void timeout(TimeValue timeout) {
    }

    @Override
    public int terminateAfter() {
        return terminateAfter;
    }

    @Override
    public void terminateAfter(int terminateAfter) {
        this.terminateAfter = terminateAfter;
    }

    @Override
    public boolean lowLevelCancellation() {
        return false;
    }

    @Override
    public SearchContext minimumScore(float minimumScore) {
        this.minScore = minimumScore;
        return this;
    }

    @Override
    public Float minimumScore() {
        return minScore;
    }

    @Override
    public SearchContext sort(SortAndFormats sort) {
        this.sort = sort;
        return this;
    }

    @Override
    public SortAndFormats sort() {
        return sort;
    }

    @Override
    public SearchContext trackScores(boolean trackScores) {
        this.trackScores = trackScores;
        return this;
    }

    @Override
    public boolean trackScores() {
        return trackScores;
    }

    @Override
    public SearchContext trackTotalHits(boolean trackTotalHits) {
        this.trackTotalHits = trackTotalHits;
        return this;
    }

    @Override
    public boolean trackTotalHits() {
        return trackTotalHits;
    }

    @Override
    public SearchContext searchAfter(FieldDoc searchAfter) {
        return null;
    }

    @Override
    public FieldDoc searchAfter() {
        return null;
    }

    @Override
    public SearchContext collapse(CollapseContext collapse) {
        return null;
    }

    @Override
    public CollapseContext collapse() {
        return null;
    }

    @Override
    public SearchContext parsedPostFilter(ParsedQuery postFilter) {
        this.postFilter = postFilter;
        return this;
    }

    @Override
    public ParsedQuery parsedPostFilter() {
        return postFilter;
    }

    @Override
    public Query aliasFilter() {
        return null;
    }

    @Override
    public SearchContext parsedQuery(ParsedQuery query) {
        this.originalQuery = query;
        this.query = query.query();
        return this;
    }

    @Override
    public ParsedQuery parsedQuery() {
        return originalQuery;
    }

    @Override
    public Query query() {
        return query;
    }

    @Override
    public int from() {
        return 0;
    }

    @Override
    public SearchContext from(int from) {
        return null;
    }

    @Override
    public int size() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }


    @Override
    public SearchContext size(int size) {
        return null;
    }

    @Override
    public boolean hasStoredFields() {
        return false;
    }

    @Override
    public boolean hasStoredFieldsContext() {
        return false;
    }

    @Override
    public boolean storedFieldsRequested() {
        return false;
    }

    @Override
    public StoredFieldsContext storedFieldsContext() {
        return null;
    }

    @Override
    public SearchContext storedFieldsContext(StoredFieldsContext storedFieldsContext) {
        return null;
    }

    @Override
    public boolean explain() {
        return false;
    }

    @Override
    public void explain(boolean explain) {
    }

    @Override
    public List<String> groupStats() {
        return null;
    }

    @Override
    public void groupStats(List<String> groupStats) {
    }

    @Override
    public boolean version() {
        return false;
    }

    @Override
    public void version(boolean version) {
    }

    @Override
    public int[] docIdsToLoad() {
        return new int[0];
    }

    @Override
    public int docIdsToLoadFrom() {
        return 0;
    }

    @Override
    public int docIdsToLoadSize() {
        return 0;
    }

    @Override
    public SearchContext docIdsToLoad(int[] docIdsToLoad, int docsIdsToLoadFrom, int docsIdsToLoadSize) {
        return null;
    }

    @Override
    public void accessed(long accessTime) {
    }

    @Override
    public long lastAccessTime() {
        return 0;
    }

    @Override
    public long keepAlive() {
        return 0;
    }

    @Override
    public void keepAlive(long keepAlive) {
    }

    @Override
    public DfsSearchResult dfsResult() {
        return null;
    }

    @Override
    public QuerySearchResult queryResult() {
        return queryResult;
    }

    @Override
    public FetchSearchResult fetchResult() {
        return null;
    }

    @Override
    public FetchPhase fetchPhase() {
        return null;
    }

    @Override
    public MappedFieldType smartNameFieldType(String name) {
        if (mapperService() != null) {
            return mapperService().fullName(name);
        }
        return null;
    }

    @Override
    public ObjectMapper getObjectMapper(String name) {
        if (mapperService() != null) {
            return mapperService().getObjectMapper(name);
        }
        return null;
    }

    @Override
    public void doClose() {
    }

    @Override
    public Counter timeEstimateCounter() {
        return timeEstimateCounter;
    }

    @Override
    public Profilers getProfilers() {
        return null; // no profiling
    }

    @Override
    public Map<Class<?>, Collector> queryCollectors() {return queryCollectors;}

    @Override
    public QueryShardContext getQueryShardContext() {
        return queryShardContext;
    }

    @Override
    public void setTask(SearchTask task) {
        this.task = task;
    }

    @Override
    public SearchTask getTask() {
        return task;
    }

    @Override
    public boolean isCancelled() {
        return task.isCancelled();
    }
}
