/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.elasticsearch.percolator;

import com.google.common.collect.ImmutableList;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.percolate.PercolateShardRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.docset.DocSetCache;
import org.elasticsearch.index.cache.filter.FilterCache;
import org.elasticsearch.index.cache.id.IdCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.fieldvisitor.JustSourceFieldsVisitor;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldMappers;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.query.ParsedFilter;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.facet.SearchContextFacets;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.partial.PartialFieldsContext;
import org.elasticsearch.search.fetch.script.ScriptFieldsContext;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.search.highlight.SearchContextHighlight;
import org.elasticsearch.search.internal.*;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rescore.RescoreSearchContext;
import org.elasticsearch.search.scan.ScanContext;
import org.elasticsearch.search.suggest.SuggestionSearchContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 */
public class PercolateContext extends SearchContext {

    public boolean limit;
    public int size;
    public boolean score;
    public boolean sort;
    public byte percolatorTypeId;

    private final PercolateShardRequest request;
    private final SearchShardTarget searchShardTarget;
    private final IndexService indexService;
    private final IndexFieldDataService fieldDataService;
    private final IndexShard indexShard;
    private final CacheRecycler cacheRecycler;
    private final ConcurrentMap<HashedBytesRef, Query> percolateQueries;
    private String[] types;

    private Engine.Searcher docEngineSearcher;
    private Engine.Searcher engineSearcher;
    private ContextIndexSearcher searcher;

    private SearchContextHighlight highlight;
    private SearchLookup searchLookup;
    private ParsedQuery parsedQuery;
    private Query query;
    private boolean queryRewritten;
    private Query percolateQuery;
    private FetchSubPhase.HitContext hitContext;
    private SearchContextFacets facets;
    private QuerySearchResult querySearchResult;

    public PercolateContext(PercolateShardRequest request, SearchShardTarget searchShardTarget, IndexShard indexShard, IndexService indexService, CacheRecycler cacheRecycler) {
        this.request = request;
        this.indexShard = indexShard;
        this.indexService = indexService;
        this.fieldDataService = indexService.fieldData();
        this.searchShardTarget = searchShardTarget;
        this.percolateQueries = indexShard.percolateRegistry().percolateQueries();
        this.types = new String[]{request.documentType()};
        this.cacheRecycler = cacheRecycler;
        this.querySearchResult = new QuerySearchResult(0, searchShardTarget);
        this.engineSearcher = indexShard.acquireSearcher("percolate");
        this.searcher = new ContextIndexSearcher(this, engineSearcher);
    }

    public void initialize(final MemoryIndex memoryIndex, ParsedDocument parsedDocument) {
        final IndexSearcher docSearcher = memoryIndex.createSearcher();
        final IndexReader topLevelReader = docSearcher.getIndexReader();
        AtomicReaderContext readerContext = topLevelReader.leaves().get(0);
        docEngineSearcher = new Engine.Searcher() {
            @Override
            public String source() {
                return "percolate";
            }

            @Override
            public IndexReader reader() {
                return topLevelReader;
            }

            @Override
            public IndexSearcher searcher() {
                return docSearcher;
            }

            @Override
            public boolean release() throws ElasticSearchException {
                try {
                    docSearcher.getIndexReader().close();
                    memoryIndex.reset();
                } catch (IOException e) {
                    throw new ElasticSearchException("failed to close percolator in-memory index", e);
                }
                return true;
            }
        };
        lookup().setNextReader(readerContext);
        lookup().setNextDocId(0);
        lookup().source().setNextSource(parsedDocument.source());

        Map<String, SearchHitField> fields = new HashMap<String, SearchHitField>();
        for (IndexableField field : parsedDocument.rootDoc().getFields()) {
            fields.put(field.name(), new InternalSearchHitField(field.name(), ImmutableList.of()));
        }
        hitContext = new FetchSubPhase.HitContext();
        hitContext.reset(new InternalSearchHit(0, "unknown", new StringText(request.documentType()), fields), readerContext, 0, topLevelReader, 0, new JustSourceFieldsVisitor());
    }

    public IndexSearcher docSearcher() {
        return docEngineSearcher.searcher();
    }

    public IndexShard indexShard() {
        return indexShard;
    }

    public IndexService indexService() {
        return indexService;
    }

    public ConcurrentMap<HashedBytesRef, Query> percolateQueries() {
        return percolateQueries;
    }

    public Query percolateQuery() {
        return percolateQuery;
    }

    public void percolateQuery(Query percolateQuery) {
        this.percolateQuery = percolateQuery;
    }

    public FetchSubPhase.HitContext hitContext() {
        return hitContext;
    }

    @Override
    public SearchContextHighlight highlight() {
        return highlight;
    }

    @Override
    public void highlight(SearchContextHighlight highlight) {
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
    public boolean release() throws ElasticSearchException {
        try {
            if (docEngineSearcher != null) {
                IndexReader indexReader = docEngineSearcher.reader();
                fieldDataService.clear(indexReader);
                indexService.cache().clear(indexReader);
                return docEngineSearcher.release();
            } else {
                return false;
            }
        } finally {
            engineSearcher.release();
        }
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
    public SearchContextFacets facets() {
        return facets;
    }

    @Override
    public SearchContext facets(SearchContextFacets facets) {
        this.facets = facets;
        return this;
    }

    // Unused:
    @Override
    public boolean clearAndRelease() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void preProcess() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Filter searchFilter(String[] types) {
        throw new UnsupportedOperationException();
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
        throw new UnsupportedOperationException();
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
    public long nowInMillis() {
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
    public RescoreSearchContext rescore() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rescore(RescoreSearchContext rescore) {
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
    public boolean hasPartialFields() {
        throw new UnsupportedOperationException();
    }

    @Override
    public PartialFieldsContext partialFields() {
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
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexQueryParserService queryParserService() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SimilarityService similarityService() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ScriptService scriptService() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CacheRecycler cacheRecycler() {
        return cacheRecycler;
    }

    @Override
    public FilterCache filterCache() {
        throw new UnsupportedOperationException();
    }

    @Override
    public DocSetCache docSetCache() {
        throw new UnsupportedOperationException();
    }

    @Override
    public IdCache idCache() {
        throw new UnsupportedOperationException();
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
    public SearchContext minimumScore(float minimumScore) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Float minimumScore() {
        return null;
    }

    @Override
    public SearchContext sort(Sort sort) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Sort sort() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SearchContext trackScores(boolean trackScores) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean trackScores() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SearchContext parsedFilter(ParsedFilter filter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ParsedFilter parsedFilter() {
        return null;
    }

    @Override
    public Filter aliasFilter() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int from() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SearchContext from(int from) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SearchContext size(int size) {
        throw new UnsupportedOperationException();
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
    public void addReleasable(Releasable releasable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clearReleasables() {
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
    public MapperService.SmartNameObjectMapper smartNameObjectMapper(String name) {
        throw new UnsupportedOperationException();
    }
}
