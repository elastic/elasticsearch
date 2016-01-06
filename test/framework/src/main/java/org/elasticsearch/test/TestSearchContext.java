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

import com.carrotsearch.hppc.ObjectObjectAssociativeContainer;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.util.Counter;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.common.HasContext;
import org.elasticsearch.common.HasContextAndHeaders;
import org.elasticsearch.common.HasHeaders;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.SearchContextAggregations;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseContext;
import org.elasticsearch.search.fetch.innerhits.InnerHitsContext;
import org.elasticsearch.search.fetch.script.ScriptFieldsContext;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.search.highlight.SearchContextHighlight;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.ScrollContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.profile.Profilers;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rescore.RescoreSearchContext;
import org.elasticsearch.search.suggest.SuggestionSearchContext;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestSearchContext extends SearchContext {

    final PageCacheRecycler pageCacheRecycler;
    final BigArrays bigArrays;
    final IndexService indexService;
    final IndexFieldDataService indexFieldDataService;
    final BitsetFilterCache fixedBitSetFilterCache;
    final ThreadPool threadPool;
    final Map<Class<?>, Collector> queryCollectors = new HashMap<>();
    final IndexShard indexShard;
    final Counter timeEstimateCounter = Counter.newCounter();
    final QuerySearchResult queryResult = new QuerySearchResult();
    ScriptService scriptService;
    ParsedQuery originalQuery;
    ParsedQuery postFilter;
    Query query;
    Float minScore;

    ContextIndexSearcher searcher;
    int size;
    private int terminateAfter = DEFAULT_TERMINATE_AFTER;
    private String[] types;
    private SearchContextAggregations aggregations;

    private final long originNanoTime = System.nanoTime();
    private final Map<String, FetchSubPhaseContext> subPhaseContexts = new HashMap<>();

    public TestSearchContext(ThreadPool threadPool,PageCacheRecycler pageCacheRecycler, BigArrays bigArrays, ScriptService scriptService, IndexService indexService) {
        super(ParseFieldMatcher.STRICT, null);
        this.pageCacheRecycler = pageCacheRecycler;
        this.bigArrays = bigArrays.withCircuitBreaking();
        this.indexService = indexService;
        this.indexFieldDataService = indexService.fieldData();
        this.fixedBitSetFilterCache = indexService.cache().bitsetFilterCache();
        this.threadPool = threadPool;
        this.indexShard = indexService.getShardOrNull(0);
        this.scriptService = scriptService;
    }

    public TestSearchContext() {
        super(ParseFieldMatcher.STRICT, null);
        this.pageCacheRecycler = null;
        this.bigArrays = null;
        this.indexService = null;
        this.indexFieldDataService = null;
        this.threadPool = null;
        this.fixedBitSetFilterCache = null;
        this.indexShard = null;
        scriptService = null;
    }

    public void setTypes(String... types) {
        this.types = types;
    }

    @Override
    public void preProcess() {
    }

    @Override
    public Query searchFilter(String[] types) {
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
    public SearchContext searchType(SearchType searchType) {
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
    public boolean hasTypes() {
        return false;
    }

    @Override
    public String[] types() {
        return new String[0];
    }

    @Override
    public float queryBoost() {
        return 0;
    }

    @Override
    public SearchContext queryBoost(float queryBoost) {
        return null;
    }

    @Override
    public long getOriginNanoTime() {
        return originNanoTime;
    }

    @Override
    protected long nowInMillisImpl() {
        return 0;
    }

    @Override
    public ScrollContext scrollContext() {
        return null;
    }

    @Override
    public SearchContext scrollContext(ScrollContext scrollContext) {
        throw new UnsupportedOperationException();
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
    public <SubPhaseContext extends FetchSubPhaseContext> SubPhaseContext getFetchSubPhaseContext(FetchSubPhase.ContextFactory<SubPhaseContext> contextFactory) {
        String subPhaseName = contextFactory.getName();
        if (subPhaseContexts.get(subPhaseName) == null) {
            subPhaseContexts.put(subPhaseName, contextFactory.newContextInstance());
        }
        return (SubPhaseContext) subPhaseContexts.get(subPhaseName);
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
    public List<RescoreSearchContext> rescore() {
        return null;
    }

    @Override
    public void addRescore(RescoreSearchContext rescore) {
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
    public AnalysisService analysisService() { return indexService.analysisService();}

    @Override
    public SimilarityService similarityService() {
        return null;
    }

    @Override
    public ScriptService scriptService() {
        return scriptService;
    }

    @Override
    public PageCacheRecycler pageCacheRecycler() {
        return pageCacheRecycler;
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
    public IndexFieldDataService fieldData() {
        return indexFieldDataService;
    }

    @Override
    public long timeoutInMillis() {
        return 0;
    }

    @Override
    public void timeoutInMillis(long timeoutInMillis) {
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
    public SearchContext minimumScore(float minimumScore) {
        this.minScore = minimumScore;
        return this;
    }

    @Override
    public Float minimumScore() {
        return minScore;
    }

    @Override
    public SearchContext sort(Sort sort) {
        return null;
    }

    @Override
    public Sort sort() {
        return null;
    }

    @Override
    public SearchContext trackScores(boolean trackScores) {
        return null;
    }

    @Override
    public boolean trackScores() {
        return false;
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
    public boolean hasFieldNames() {
        return false;
    }

    @Override
    public List<String> fieldNames() {
        return null;
    }

    @Override
    public void emptyFieldNames() {
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
    public SearchLookup lookup() {
        return new SearchLookup(mapperService(), fieldData(), null);
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
    public void innerHits(InnerHitsContext innerHitsContext) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InnerHitsContext innerHits() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <V> V putInContext(Object key, Object value) {
        return null;
    }

    @Override
    public void putAllInContext(ObjectObjectAssociativeContainer<Object, Object> map) {
    }

    @Override
    public <V> V getFromContext(Object key) {
        return null;
    }

    @Override
    public <V> V getFromContext(Object key, V defaultValue) {
        return defaultValue;
    }

    @Override
    public boolean hasInContext(Object key) {
        return false;
    }

    @Override
    public int contextSize() {
        return 0;
    }

    @Override
    public boolean isContextEmpty() {
        return true;
    }

    @Override
    public ImmutableOpenMap<Object, Object> getContext() {
        return ImmutableOpenMap.of();
    }

    @Override
    public void copyContextFrom(HasContext other) {
    }

    @Override
    public <V> void putHeader(String key, V value) {}

    @Override
    public <V> V getHeader(String key) {
        return null;
    }

    @Override
    public boolean hasHeader(String key) {
        return false;
    }

    @Override
    public Set<String> getHeaders() {
        return Collections.emptySet();
    }

    @Override
    public void copyHeadersFrom(HasHeaders from) {}

    @Override
    public void copyContextAndHeadersFrom(HasContextAndHeaders other) {}

    @Override
    public Profilers getProfilers() {
        return null; // no profiling
    }

    @Override
    public Map<Class<?>, Collector> queryCollectors() {return queryCollectors;}

}
