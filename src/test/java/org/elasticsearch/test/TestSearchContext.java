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

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.util.Counter;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.filter.FilterCache;
import org.elasticsearch.index.cache.fixedbitset.FixedBitSetFilterCache;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldMappers;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.query.ParsedFilter;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.SearchContextAggregations;
import org.elasticsearch.search.dfs.DfsSearchResult;
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
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;

public class TestSearchContext extends SearchContext {

    final PageCacheRecycler pageCacheRecycler;
    final BigArrays bigArrays;
    final IndexService indexService;
    final FilterCache filterCache;
    final IndexFieldDataService indexFieldDataService;
    final FixedBitSetFilterCache fixedBitSetFilterCache;
    final ThreadPool threadPool;

    ContextIndexSearcher searcher;
    int size;
    private int terminateAfter = DEFAULT_TERMINATE_AFTER;
    private String[] types;

    public TestSearchContext(ThreadPool threadPool,PageCacheRecycler pageCacheRecycler, BigArrays bigArrays, IndexService indexService, FilterCache filterCache, IndexFieldDataService indexFieldDataService) {
        this.pageCacheRecycler = pageCacheRecycler;
        this.bigArrays = bigArrays.withCircuitBreaking();
        this.indexService = indexService;
        this.filterCache = indexService.cache().filter();
        this.indexFieldDataService = indexService.fieldData();
        this.fixedBitSetFilterCache = indexService.fixedBitSetFilterCache();
        this.threadPool = threadPool;
    }

    public TestSearchContext() {
        this.pageCacheRecycler = null;
        this.bigArrays = null;
        this.indexService = null;
        this.filterCache = null;
        this.indexFieldDataService = null;
        this.threadPool = null;
        this.fixedBitSetFilterCache = null;
    }

    public void setTypes(String... types) {
        this.types = types;
    }

    @Override
    public void preProcess() {
    }

    @Override
    public Filter searchFilter(String[] types) {
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
        return 0;
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
    protected long nowInMillisImpl() {
        return 0;
    }

    @Override
    public Scroll scroll() {
        return null;
    }

    @Override
    public SearchContext scroll(Scroll scroll) {
        return null;
    }

    @Override
    public SearchContextAggregations aggregations() {
        return null;
    }

    @Override
    public SearchContext aggregations(SearchContextAggregations aggregations) {
        return null;
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
    public boolean hasFieldDataFields() {
        return false;
    }

    @Override
    public FieldDataFieldsContext fieldDataFields() {
        return null;
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
    public boolean hasPartialFields() {
        return false;
    }

    @Override
    public PartialFieldsContext partialFields() {
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

    public void setSearcher(ContextIndexSearcher searcher) {
        this.searcher = searcher;
    }

    @Override
    public IndexShard indexShard() {
        return null;
    }

    @Override
    public MapperService mapperService() {
        if (indexService != null) {
            return indexService.mapperService();
        }
        return null;
    }

    @Override
    public AnalysisService analysisService() {
        return indexService.analysisService();
    }

    @Override
    public IndexQueryParserService queryParserService() {
        return null;
    }

    @Override
    public SimilarityService similarityService() {
        return null;
    }

    @Override
    public ScriptService scriptService() {
        return null;
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
    public FilterCache filterCache() {
        return filterCache;
    }

    @Override
    public FixedBitSetFilterCache fixedBitSetFilterCache() {
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
        return null;
    }

    @Override
    public Float minimumScore() {
        return null;
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
    public SearchContext parsedPostFilter(ParsedFilter postFilter) {
        return null;
    }

    @Override
    public ParsedFilter parsedPostFilter() {
        return null;
    }

    @Override
    public Filter aliasFilter() {
        return null;
    }

    @Override
    public SearchContext parsedQuery(ParsedQuery query) {
        return null;
    }

    @Override
    public ParsedQuery parsedQuery() {
        return null;
    }

    @Override
    public Query query() {
        return null;
    }

    @Override
    public boolean queryRewritten() {
        return false;
    }

    @Override
    public SearchContext updateRewriteQuery(Query rewriteQuery) {
        return null;
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
    public void lastEmittedDoc(ScoreDoc doc) {
    }

    @Override
    public ScoreDoc lastEmittedDoc() {
        return null;
    }

    @Override
    public SearchLookup lookup() {
        return null;
    }

    @Override
    public DfsSearchResult dfsResult() {
        return null;
    }

    @Override
    public QuerySearchResult queryResult() {
        return null;
    }

    @Override
    public FetchSearchResult fetchResult() {
        return null;
    }

    @Override
    public ScanContext scanContext() {
        return null;
    }

    @Override
    public MapperService.SmartNameFieldMappers smartFieldMappers(String name) {
        return null;
    }

    @Override
    public FieldMappers smartNameFieldMappers(String name) {
        return null;
    }

    @Override
    public FieldMapper<?> smartNameFieldMapper(String name) {
        if (mapperService() != null) {
            return mapperService().smartNameFieldMapper(name, types());
        }
        return null;
    }

    @Override
    public MapperService.SmartNameObjectMapper smartNameObjectMapper(String name) {
        return null;
    }

    @Override
    public void doClose() throws ElasticsearchException {
    }

    @Override
    public boolean useSlowScroll() {
        return false;
    }

    @Override
    public SearchContext useSlowScroll(boolean useSlowScroll) {
        return null;
    }

    @Override
    public Counter timeEstimateCounter() {
        throw new UnsupportedOperationException();
    }
}
