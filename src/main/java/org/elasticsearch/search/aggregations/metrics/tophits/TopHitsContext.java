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
package org.elasticsearch.search.aggregations.metrics.tophits;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.docset.DocSetCache;
import org.elasticsearch.index.cache.filter.FilterCache;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldMappers;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.query.ParsedFilter;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.SearchContextAggregations;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.facet.SearchContextFacets;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.fielddata.FieldDataFieldsContext;
import org.elasticsearch.search.fetch.partial.PartialFieldsContext;
import org.elasticsearch.search.fetch.script.ScriptFieldsContext;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.search.highlight.SearchContextHighlight;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rescore.RescoreSearchContext;
import org.elasticsearch.search.scan.ScanContext;
import org.elasticsearch.search.suggest.SuggestionSearchContext;

import java.util.List;

/**
 */
public class TopHitsContext extends SearchContext {

    // By default return 3 hits per bucket. A higher default would make the response really large by default, since
    // the to hits are returned per bucket.
    private final static int DEFAULT_SIZE = 3;

    private int from;
    private int size = DEFAULT_SIZE;
    private Sort sort;

    private final FetchSearchResult fetchSearchResult;
    private final QuerySearchResult querySearchResult;

    private int[] docIdsToLoad;
    private int docsIdsToLoadFrom;
    private int docsIdsToLoadSize;

    private final SearchContext context;

    private List<String> fieldNames;
    private FieldDataFieldsContext fieldDataFields;
    private ScriptFieldsContext scriptFields;
    private PartialFieldsContext partialFields;
    private FetchSourceContext fetchSourceContext;
    private SearchContextHighlight highlight;

    private boolean explain;
    private boolean trackScores;
    private boolean version;

    public TopHitsContext(SearchContext context) {
        this.fetchSearchResult = new FetchSearchResult();
        this.querySearchResult = new QuerySearchResult();
        this.context = context;
    }

    @Override
    protected void doClose() {
    }

    @Override
    public void preProcess() {
    }

    @Override
    public Filter searchFilter(String[] types) {
        throw new UnsupportedOperationException("this context should be read only");
    }

    @Override
    public long id() {
        return context.id();
    }

    @Override
    public String source() {
        return context.source();
    }

    @Override
    public ShardSearchRequest request() {
        return context.request();
    }

    @Override
    public SearchType searchType() {
        return context.searchType();
    }

    @Override
    public SearchContext searchType(SearchType searchType) {
        throw new UnsupportedOperationException("this context should be read only");
    }

    @Override
    public SearchShardTarget shardTarget() {
        return context.shardTarget();
    }

    @Override
    public int numberOfShards() {
        return context.numberOfShards();
    }

    @Override
    public boolean hasTypes() {
        return context.hasTypes();
    }

    @Override
    public String[] types() {
        return context.types();
    }

    @Override
    public float queryBoost() {
        return context.queryBoost();
    }

    @Override
    public SearchContext queryBoost(float queryBoost) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public long nowInMillis() {
        return context.nowInMillis();
    }

    @Override
    public Scroll scroll() {
        return context.scroll();
    }

    @Override
    public SearchContext scroll(Scroll scroll) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public SearchContextAggregations aggregations() {
        return context.aggregations();
    }

    @Override
    public SearchContext aggregations(SearchContextAggregations aggregations) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public SearchContextFacets facets() {
        return context.facets();
    }

    @Override
    public SearchContext facets(SearchContextFacets facets) {
        throw new UnsupportedOperationException("Not supported");
    }

    public SearchContextHighlight highlight() {
        return highlight;
    }

    public void highlight(SearchContextHighlight highlight) {
        this.highlight = highlight;
    }

    @Override
    public SuggestionSearchContext suggest() {
        return context.suggest();
    }

    @Override
    public void suggest(SuggestionSearchContext suggest) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public List<RescoreSearchContext> rescore() {
        return context.rescore();
    }

    @Override
    public void addRescore(RescoreSearchContext rescore) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public boolean hasFieldDataFields() {
        return fieldDataFields != null;
    }

    @Override
    public FieldDataFieldsContext fieldDataFields() {
        if (fieldDataFields == null) {
            fieldDataFields = new FieldDataFieldsContext();
        }
        return this.fieldDataFields;
    }

    @Override
    public boolean hasScriptFields() {
        return scriptFields != null;
    }

    @Override
    public ScriptFieldsContext scriptFields() {
        if (scriptFields == null) {
            scriptFields = new ScriptFieldsContext();
        }
        return this.scriptFields;
    }

    @Override
    public boolean hasPartialFields() {
        return partialFields != null;
    }

    @Override
    public PartialFieldsContext partialFields() {
        if (partialFields == null) {
            partialFields = new PartialFieldsContext();
        }
        return this.partialFields;
    }

    @Override
    public boolean sourceRequested() {
        return fetchSourceContext != null && fetchSourceContext.fetchSource();
    }

    @Override
    public boolean hasFetchSourceContext() {
        return fetchSourceContext != null;
    }

    @Override
    public FetchSourceContext fetchSourceContext() {
        return fetchSourceContext;
    }

    @Override
    public SearchContext fetchSourceContext(FetchSourceContext fetchSourceContext) {
        this.fetchSourceContext = fetchSourceContext;
        return this;
    }

    @Override
    public ContextIndexSearcher searcher() {
        return context.searcher();
    }

    @Override
    public IndexShard indexShard() {
        return context.indexShard();
    }

    @Override
    public MapperService mapperService() {
        return context.mapperService();
    }

    @Override
    public AnalysisService analysisService() {
        return context.analysisService();
    }

    @Override
    public IndexQueryParserService queryParserService() {
        return context.queryParserService();
    }

    @Override
    public SimilarityService similarityService() {
        return context.similarityService();
    }

    @Override
    public ScriptService scriptService() {
        return context.scriptService();
    }

    @Override
    public CacheRecycler cacheRecycler() {
        return context.cacheRecycler();
    }

    @Override
    public PageCacheRecycler pageCacheRecycler() {
        return context.pageCacheRecycler();
    }

    @Override
    public BigArrays bigArrays() {
        return context.bigArrays();
    }

    @Override
    public FilterCache filterCache() {
        return context.filterCache();
    }

    @Override
    public DocSetCache docSetCache() {
        return context.docSetCache();
    }

    @Override
    public IndexFieldDataService fieldData() {
        return context.fieldData();
    }

    @Override
    public long timeoutInMillis() {
        return context.timeoutInMillis();
    }

    @Override
    public void timeoutInMillis(long timeoutInMillis) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public SearchContext minimumScore(float minimumScore) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public Float minimumScore() {
        return context.minimumScore();
    }

    @Override
    public SearchContext sort(Sort sort) {
        this.sort = sort;
        return null;
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
    public SearchContext parsedPostFilter(ParsedFilter postFilter) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public ParsedFilter parsedPostFilter() {
        return context.parsedPostFilter();
    }

    @Override
    public Filter aliasFilter() {
        return context.aliasFilter();
    }

    @Override
    public SearchContext parsedQuery(ParsedQuery query) {
        return context.parsedQuery(query);
    }

    @Override
    public ParsedQuery parsedQuery() {
        return context.parsedQuery();
    }

    @Override
    public Query query() {
        return context.query();
    }

    @Override
    public boolean queryRewritten() {
        return context.queryRewritten();
    }

    @Override
    public SearchContext updateRewriteQuery(Query rewriteQuery) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public int from() {
        return from;
    }

    @Override
    public SearchContext from(int from) {
        this.from = from;
        return this;
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
        return fieldNames != null;
    }

    @Override
    public List<String> fieldNames() {
        if (fieldNames == null) {
            fieldNames = Lists.newArrayList();
        }
        return fieldNames;
    }

    @Override
    public void emptyFieldNames() {
        this.fieldNames = ImmutableList.of();
    }

    @Override
    public boolean explain() {
        return explain;
    }

    @Override
    public void explain(boolean explain) {
        this.explain = explain;
    }

    @Override
    public List<String> groupStats() {
        return context.groupStats();
    }

    @Override
    public void groupStats(List<String> groupStats) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public boolean version() {
        return version;
    }

    @Override
    public void version(boolean version) {
        this.version = version;
    }

    @Override
    public int[] docIdsToLoad() {
        return docIdsToLoad;
    }

    @Override
    public int docIdsToLoadFrom() {
        return docsIdsToLoadFrom;
    }

    @Override
    public int docIdsToLoadSize() {
        return docsIdsToLoadSize;
    }

    @Override
    public SearchContext docIdsToLoad(int[] docIdsToLoad, int docsIdsToLoadFrom, int docsIdsToLoadSize) {
        this.docIdsToLoad = docIdsToLoad;
        this.docsIdsToLoadFrom = docsIdsToLoadFrom;
        this.docsIdsToLoadSize = docsIdsToLoadSize;
        return this;
    }

    @Override
    public void accessed(long accessTime) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public long lastAccessTime() {
        return context.lastAccessTime();
    }

    @Override
    public long keepAlive() {
        return context.keepAlive();
    }

    @Override
    public void keepAlive(long keepAlive) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void lastEmittedDoc(ScoreDoc doc) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public ScoreDoc lastEmittedDoc() {
        return context.lastEmittedDoc();
    }

    @Override
    public SearchLookup lookup() {
        return context.lookup();
    }

    @Override
    public DfsSearchResult dfsResult() {
        return context.dfsResult();
    }

    @Override
    public QuerySearchResult queryResult() {
        return querySearchResult;
    }

    @Override
    public FetchSearchResult fetchResult() {
        return fetchSearchResult;
    }

    @Override
    public ScanContext scanContext() {
        return context.scanContext();
    }

    @Override
    public MapperService.SmartNameFieldMappers smartFieldMappers(String name) {
        return context.smartFieldMappers(name);
    }

    @Override
    public FieldMappers smartNameFieldMappers(String name) {
        return context.smartNameFieldMappers(name);
    }

    @Override
    public FieldMapper smartNameFieldMapper(String name) {
        return context.smartNameFieldMapper(name);
    }

    @Override
    public MapperService.SmartNameObjectMapper smartNameObjectMapper(String name) {
        return context.smartNameObjectMapper(name);
    }

    @Override
    public boolean useSlowScroll() {
        return context.useSlowScroll();
    }

    @Override
    public SearchContext useSlowScroll(boolean useSlowScroll) {
        throw new UnsupportedOperationException("Not supported");
    }
}
