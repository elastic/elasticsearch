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

import com.google.common.collect.ImmutableList;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.elasticsearch.action.percolate.PercolateShardRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.cache.filter.FilterCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldMappers;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.query.ParsedFilter;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.SearchContextAggregations;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.fielddata.FieldDataFieldsContext;
import org.elasticsearch.search.fetch.innerhits.InnerHitsContext;
import org.elasticsearch.search.fetch.script.ScriptFieldsContext;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.search.highlight.SearchContextHighlight;
import org.elasticsearch.search.internal.*;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rescore.RescoreSearchContext;
import org.elasticsearch.search.scan.ScanContext;
import org.elasticsearch.search.suggest.SuggestionSearchContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 */
public class PercolateContext extends SearchContext {

    public boolean limit;
    private int size;
    public boolean doSort;
    public byte percolatorTypeId;
    private boolean trackScores;

    private final SearchShardTarget searchShardTarget;
    private final IndexService indexService;
    private final IndexFieldDataService fieldDataService;
    private final IndexShard indexShard;
    private final PageCacheRecycler pageCacheRecycler;
    private final BigArrays bigArrays;
    private final ScriptService scriptService;
    private final ConcurrentMap<BytesRef, Query> percolateQueries;
    private final int numberOfShards;
    private final Filter aliasFilter;
    private String[] types;

    private Engine.Searcher docSearcher;
    private Engine.Searcher engineSearcher;
    private ContextIndexSearcher searcher;

    private SearchContextHighlight highlight;
    private SearchLookup searchLookup;
    private ParsedQuery parsedQuery;
    private Query query;
    private boolean queryRewritten;
    private Query percolateQuery;
    private FetchSubPhase.HitContext hitContext;
    private SearchContextAggregations aggregations;
    private QuerySearchResult querySearchResult;
    private Sort sort;

    public PercolateContext(PercolateShardRequest request, SearchShardTarget searchShardTarget, IndexShard indexShard,
                            IndexService indexService, PageCacheRecycler pageCacheRecycler,
                            BigArrays bigArrays, ScriptService scriptService, Filter aliasFilter) {
        this.indexShard = indexShard;
        this.indexService = indexService;
        this.fieldDataService = indexService.fieldData();
        this.searchShardTarget = searchShardTarget;
        this.percolateQueries = indexShard.percolateRegistry().percolateQueries();
        this.types = new String[]{request.documentType()};
        this.pageCacheRecycler = pageCacheRecycler;
        this.bigArrays = bigArrays.withCircuitBreaking();
        this.querySearchResult = new QuerySearchResult(0, searchShardTarget);
        this.engineSearcher = indexShard.acquireSearcher("percolate");
        this.searcher = new ContextIndexSearcher(this, engineSearcher);
        this.scriptService = scriptService;
        this.numberOfShards = request.getNumberOfShards();
        this.aliasFilter = aliasFilter;
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
            fields.put(field.name(), new InternalSearchHitField(field.name(), ImmutableList.of()));
        }
        hitContext().reset(
                new InternalSearchHit(0, "unknown", new StringText(parsedDocument.type()), fields),
                atomicReaderContext, 0, indexReader
        );
    }

    @Override
    public IndexShard indexShard() {
        return indexShard;
    }

    public IndexService indexService() {
        return indexService;
    }

    public ConcurrentMap<BytesRef, Query> percolateQueries() {
        return percolateQueries;
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
        return indexService.mapperService();
    }

    @Override
    public SearchContext parsedQuery(ParsedQuery query) {
        this.parsedQuery = query;
        this.query = query.query();
        this.queryRewritten = false;
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
    public boolean queryRewritten() {
        return queryRewritten;
    }

    @Override
    public SearchContext updateRewriteQuery(Query rewriteQuery) {
        queryRewritten = true;
        query = rewriteQuery;
        return this;
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

    // Unused:
    @Override
    public void preProcess() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Filter searchFilter(String[] types) {
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
    protected long nowInMillisImpl() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Scroll scroll() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SearchContext scroll(Scroll scroll) {
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
    public boolean hasFieldDataFields() {
        throw new UnsupportedOperationException();
    }

    @Override
    public FieldDataFieldsContext fieldDataFields() {
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
    public IndexQueryParserService queryParserService() {
        return indexService.queryParserService();
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
    public FilterCache filterCache() {
        return indexService.cache().filter();
    }

    @Override
    public BitsetFilterCache bitsetFilterCache() {
        return indexService.bitsetFilterCache();
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
    public SearchContext parsedPostFilter(ParsedFilter postFilter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ParsedFilter parsedPostFilter() {
        return null;
    }

    @Override
    public Filter aliasFilter() {
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
        this.limit = true;
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
    public void lastEmittedDoc(ScoreDoc doc) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ScoreDoc lastEmittedDoc() {
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
    public ScanContext scanContext() {
        throw new UnsupportedOperationException();
    }

    @Override
    public MapperService.SmartNameFieldMappers smartFieldMappers(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FieldMappers smartNameFieldMappers(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FieldMapper smartNameFieldMapper(String name) {
        return mapperService().smartNameFieldMapper(name, types);
    }

    @Override
    public FieldMapper smartNameFieldMapperFromAnyType(String name) {
        return mapperService().smartNameFieldMapper(name);
    }

    @Override
    public MapperService.SmartNameObjectMapper smartNameObjectMapper(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Counter timeEstimateCounter() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void innerHits(InnerHitsContext innerHitsContext) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InnerHitsContext innerHits() {
        throw new UnsupportedOperationException();
    }

}
