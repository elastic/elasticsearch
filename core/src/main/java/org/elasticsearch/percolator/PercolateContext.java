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
package org.elasticsearch.percolator;

import com.carrotsearch.hppc.ObjectObjectAssociativeContainer;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.util.Counter;
import org.elasticsearch.action.percolate.PercolateShardRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.SearchContextAggregations;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseContext;
import org.elasticsearch.search.fetch.innerhits.InnerHitsContext;
import org.elasticsearch.search.fetch.script.ScriptFieldsContext;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.search.highlight.SearchContextHighlight;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHitField;
import org.elasticsearch.search.internal.ScrollContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.profile.Profilers;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rescore.RescoreSearchContext;
import org.elasticsearch.search.suggest.SuggestionSearchContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class PercolateContext extends SearchContext {

    private int size = 10;
    private boolean trackScores;

    private final SearchShardTarget searchShardTarget;
    private final IndexService indexService;
    private final IndexFieldDataService fieldDataService;
    private final IndexShard indexShard;
    private final PageCacheRecycler pageCacheRecycler;
    private final BigArrays bigArrays;
    private final ScriptService scriptService;
    private final MapperService mapperService;
    private final int numberOfShards;
    private final Query aliasFilter;
    private final long originNanoTime = System.nanoTime();
    private final long startTime;
    private final boolean onlyCount;
    private String[] types;

    private Engine.Searcher docSearcher;
    private Engine.Searcher engineSearcher;
    private ContextIndexSearcher searcher;

    private SearchContextHighlight highlight;
    private SearchLookup searchLookup;
    private ParsedQuery parsedQuery;
    private Query query;
    private Query percolateQuery;
    private FetchSubPhase.HitContext hitContext;
    private SearchContextAggregations aggregations;
    private QuerySearchResult querySearchResult;
    private Sort sort;
    private final Map<String, FetchSubPhaseContext> subPhaseContexts = new HashMap<>();
    private final Map<Class<?>, Collector> queryCollectors = new HashMap<>();
    private final FetchPhase fetchPhase;

    public PercolateContext(PercolateShardRequest request, SearchShardTarget searchShardTarget, IndexShard indexShard,
            IndexService indexService, PageCacheRecycler pageCacheRecycler, BigArrays bigArrays, ScriptService scriptService,
            Query aliasFilter, ParseFieldMatcher parseFieldMatcher, FetchPhase fetchPhase) {
        super(parseFieldMatcher);
        this.indexShard = indexShard;
        this.indexService = indexService;
        this.fetchPhase = fetchPhase;
        this.fieldDataService = indexService.fieldData();
        this.mapperService = indexService.mapperService();
        this.searchShardTarget = searchShardTarget;
        this.types = new String[]{request.documentType()};
        this.pageCacheRecycler = pageCacheRecycler;
        this.bigArrays = bigArrays.withCircuitBreaking();
        this.querySearchResult = new QuerySearchResult(0, searchShardTarget);
        this.engineSearcher = indexShard.acquireSearcher("percolate");
        this.searcher = new ContextIndexSearcher(engineSearcher, indexService.cache().query(), indexShard.getQueryCachingPolicy());
        this.scriptService = scriptService;
        this.numberOfShards = request.getNumberOfShards();
        this.aliasFilter = aliasFilter;
        this.startTime = request.getStartTime();
        this.onlyCount = request.onlyCount();
    }

    // for testing:
    PercolateContext(PercolateShardRequest request, SearchShardTarget searchShardTarget, MapperService mapperService) {
        super(null);
        this.searchShardTarget = searchShardTarget;
        this.mapperService = mapperService;
        this.indexService = null;
        this.indexShard = null;
        this.fieldDataService = null;
        this.pageCacheRecycler = null;
        this.bigArrays = null;
        this.scriptService = null;
        this.aliasFilter = null;
        this.startTime = 0;
        this.numberOfShards = 0;
        this.onlyCount = true;
        this.fetchPhase = null;
    }

    public IndexSearcher docSearcher() {
        return docSearcher.searcher();
    }

    public void initialize(Engine.Searcher docSearcher, ParsedDocument parsedDocument) {
        this.docSearcher = docSearcher;

        IndexReader indexReader = docSearcher.reader();
        LeafReaderContext atomicReaderContext = indexReader.leaves().get(0);
        LeafSearchLookup leafLookup = lookup().getLeafSearchLookup(atomicReaderContext);
        leafLookup.setDocument(0);
        leafLookup.source().setSource(parsedDocument.source());

        Map<String, SearchHitField> fields = new HashMap<>();
        for (IndexableField field : parsedDocument.rootDoc().getFields()) {
            fields.put(field.name(), new InternalSearchHitField(field.name(), Collections.emptyList()));
        }
        hitContext().reset(
                new InternalSearchHit(0, "unknown", new Text(parsedDocument.type()), fields),
                atomicReaderContext, 0, docSearcher.searcher()
        );
    }

    @Override
    public IndexShard indexShard() {
        return indexShard;
    }

    public IndexService indexService() {
        return indexService;
    }

    public Query percolateQuery() {
        return percolateQuery;
    }

    public void percolateQuery(Query percolateQuery) {
        this.percolateQuery = percolateQuery;
    }

    public FetchSubPhase.HitContext hitContext() {
        if (hitContext == null) {
            hitContext = new FetchSubPhase.HitContext();
        }
        return hitContext;
    }

    public boolean isOnlyCount() {
        return onlyCount;
    }

    public Query percolatorTypeFilter(){
        return indexService().mapperService().documentMapper(PercolatorService.TYPE_NAME).typeFilter();
    }

    @Override
    public SearchContextHighlight highlight() {
        return highlight;
    }

    @Override
    public void highlight(SearchContextHighlight highlight) {
        if (highlight != null) {
            // Enforce highlighting by source, because MemoryIndex doesn't support stored fields.
            highlight.globalForceSource(true);
        }
        this.highlight = highlight;
    }

    @Override
    public SearchShardTarget shardTarget() {
        return searchShardTarget;
    }

    @Override
    public SearchLookup lookup() {
        if (searchLookup == null) {
            searchLookup = new SearchLookup(mapperService(), fieldData(), types);
        }
        return searchLookup;
    }

    @Override
    protected void doClose() {
        Releasables.close(engineSearcher, docSearcher);
    }

    @Override
    public MapperService mapperService() {
        return mapperService;
    }

    @Override
    public SearchContext parsedQuery(ParsedQuery query) {
        this.parsedQuery = query;
        this.query = query.query();
        return this;
    }

    @Override
    public ParsedQuery parsedQuery() {
        return parsedQuery;
    }

    @Override
    public Query query() {
        return query;
    }

    @Override
    public String[] types() {
        return types;
    }

    public void types(String[] types) {
        this.types = types;
        searchLookup = new SearchLookup(mapperService(), fieldData(), types);
    }

    @Override
    public IndexFieldDataService fieldData() {
        return fieldDataService;
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

    // Unused:
    @Override
    public void preProcess() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Query searchFilter(String[] types) {
        return aliasFilter();
    }

    @Override
    public long id() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String source() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ShardSearchRequest request() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SearchType searchType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SearchContext searchType(SearchType searchType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int numberOfShards() {
        return numberOfShards;
    }

    @Override
    public boolean hasTypes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public float queryBoost() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SearchContext queryBoost(float queryBoost) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getOriginNanoTime() {
        return originNanoTime;
    }

    @Override
    protected long nowInMillisImpl() {
        return startTime;
    }

    @Override
    public ScrollContext scrollContext() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SearchContext scrollContext(ScrollContext scroll) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SuggestionSearchContext suggest() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void suggest(SuggestionSearchContext suggest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<RescoreSearchContext> rescore() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addRescore(RescoreSearchContext rescore) {
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
    public boolean hasFetchSourceContext() {
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
    public ContextIndexSearcher searcher() {
        return searcher;
    }

    @Override
    public AnalysisService analysisService() {
        return indexService.analysisService();
    }

    @Override
    public SimilarityService similarityService() {
        return indexService.similarityService();
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
        return indexService.cache().bitsetFilterCache();
    }

    @Override
    public long timeoutInMillis() {
        return -1;
    }

    @Override
    public void timeoutInMillis(long timeoutInMillis) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int terminateAfter() {
        return DEFAULT_TERMINATE_AFTER;
    }

    @Override
    public void terminateAfter(int terminateAfter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SearchContext minimumScore(float minimumScore) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Float minimumScore() {
        return null;
    }

    @Override
    public SearchContext sort(Sort sort) {
        this.sort = sort;
        return this;
    }

    @Override
    public Sort sort() {
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
    public SearchContext searchAfter(FieldDoc searchAfter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FieldDoc searchAfter() {
        return null;
    }

    @Override
    public SearchContext parsedPostFilter(ParsedQuery postFilter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ParsedQuery parsedPostFilter() {
        return null;
    }

    @Override
    public Query aliasFilter() {
        return aliasFilter;
    }

    @Override
    public int from() {
        return 0;
    }

    @Override
    public SearchContext from(int from) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public SearchContext size(int size) {
        this.size = size;
        return this;
    }

    @Override
    public boolean hasFieldNames() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> fieldNames() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void emptyFieldNames() {
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
    public void groupStats(List<String> groupStats) {
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
    public int[] docIdsToLoad() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int docIdsToLoadFrom() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int docIdsToLoadSize() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SearchContext docIdsToLoad(int[] docIdsToLoad, int docsIdsToLoadFrom, int docsIdsToLoadSize) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void accessed(long accessTime) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long lastAccessTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long keepAlive() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void keepAlive(long keepAlive) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DfsSearchResult dfsResult() {
        throw new UnsupportedOperationException();
    }

    @Override
    public QuerySearchResult queryResult() {
        return querySearchResult;
    }

    @Override
    public FetchSearchResult fetchResult() {
        throw new UnsupportedOperationException();
    }

    @Override
    public FetchPhase fetchPhase() {
        return fetchPhase;
    }

    @Override
    public MappedFieldType smartNameFieldType(String name) {
        return mapperService().fullName(name);
    }

    @Override
    public ObjectMapper getObjectMapper(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Counter timeEstimateCounter() {
        throw new UnsupportedOperationException();
    }

    @Override
    public InnerHitsContext innerHits() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Class<?>, Collector> queryCollectors() {
        return queryCollectors;
    }

    @Override
    public Profilers getProfilers() {
        throw new UnsupportedOperationException();
    }
}
