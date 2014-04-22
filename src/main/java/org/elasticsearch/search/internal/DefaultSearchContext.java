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

package org.elasticsearch.search.internal;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.search.AndFilter;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.lucene.search.XConstantScoreQuery;
import org.elasticsearch.common.lucene.search.XFilteredQuery;
import org.elasticsearch.common.lucene.search.function.BoostScoreFunction;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.docset.DocSetCache;
import org.elasticsearch.index.cache.filter.FilterCache;
import org.elasticsearch.index.engine.Engine;
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
import org.elasticsearch.search.facet.SearchContextFacets;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.fielddata.FieldDataFieldsContext;
import org.elasticsearch.search.fetch.partial.PartialFieldsContext;
import org.elasticsearch.search.fetch.script.ScriptFieldsContext;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.search.highlight.SearchContextHighlight;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rescore.RescoreSearchContext;
import org.elasticsearch.search.scan.ScanContext;
import org.elasticsearch.search.suggest.SuggestionSearchContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class DefaultSearchContext extends SearchContext {

    private final long id;

    private final ShardSearchRequest request;

    private final SearchShardTarget shardTarget;

    private SearchType searchType;

    private final Engine.Searcher engineSearcher;

    private final ScriptService scriptService;

    private final CacheRecycler cacheRecycler;

    private final PageCacheRecycler pageCacheRecycler;

    private final BigArrays bigArrays;

    private final IndexShard indexShard;

    private final IndexService indexService;

    private final ContextIndexSearcher searcher;

    private final DfsSearchResult dfsResult;

    private final QuerySearchResult queryResult;

    private final FetchSearchResult fetchResult;

    // lazy initialized only if needed
    private ScanContext scanContext;

    private float queryBoost = 1.0f;

    // timeout in millis
    private long timeoutInMillis = -1;


    private List<String> groupStats;

    private Scroll scroll;

    private boolean explain;

    private boolean version = false; // by default, we don't return versions

    private List<String> fieldNames;
    private FieldDataFieldsContext fieldDataFields;
    private ScriptFieldsContext scriptFields;
    private PartialFieldsContext partialFields;
    private FetchSourceContext fetchSourceContext;

    private int from = -1;

    private int size = -1;

    private Sort sort;

    private Float minimumScore;

    private boolean trackScores = false; // when sorting, track scores as well...

    private ParsedQuery originalQuery;

    private Query query;

    private ParsedFilter postFilter;

    private Filter aliasFilter;

    private int[] docIdsToLoad;

    private int docsIdsToLoadFrom;

    private int docsIdsToLoadSize;

    private SearchContextAggregations aggregations;

    private SearchContextFacets facets;

    private SearchContextHighlight highlight;

    private SuggestionSearchContext suggest;

    private List<RescoreSearchContext> rescore;

    private SearchLookup searchLookup;

    private boolean queryRewritten;

    private volatile long keepAlive;

    private ScoreDoc lastEmittedDoc;

    private volatile long lastAccessTime = -1;

    private volatile boolean useSlowScroll;

    public DefaultSearchContext(long id, ShardSearchRequest request, SearchShardTarget shardTarget,
                         Engine.Searcher engineSearcher, IndexService indexService, IndexShard indexShard,
                         ScriptService scriptService, CacheRecycler cacheRecycler, PageCacheRecycler pageCacheRecycler,
                         BigArrays bigArrays) {
        this.id = id;
        this.request = request;
        this.searchType = request.searchType();
        this.shardTarget = shardTarget;
        this.engineSearcher = engineSearcher;
        this.scriptService = scriptService;
        this.cacheRecycler = cacheRecycler;
        this.pageCacheRecycler = pageCacheRecycler;
        this.bigArrays = bigArrays;
        this.dfsResult = new DfsSearchResult(id, shardTarget);
        this.queryResult = new QuerySearchResult(id, shardTarget);
        this.fetchResult = new FetchSearchResult(id, shardTarget);
        this.indexShard = indexShard;
        this.indexService = indexService;

        this.searcher = new ContextIndexSearcher(this, engineSearcher);

        // initialize the filtering alias based on the provided filters
        aliasFilter = indexService.aliasesService().aliasFilter(request.filteringAliases());
    }

    @Override
    public void doClose() throws ElasticsearchException {
        if (scanContext != null) {
            scanContext.clear();
        }
        // clear and scope phase we  have
        Releasables.close(searcher, engineSearcher);
    }

    /**
     * Should be called before executing the main query and after all other parameters have been set.
     */
    public void preProcess() {
        if (query() == null) {
            parsedQuery(ParsedQuery.parsedMatchAllQuery());
        }
        if (queryBoost() != 1.0f) {
            parsedQuery(new ParsedQuery(new FunctionScoreQuery(query(), new BoostScoreFunction(queryBoost)), parsedQuery()));
        }
        Filter searchFilter = searchFilter(types());
        if (searchFilter != null) {
            if (Queries.isConstantMatchAllQuery(query())) {
                Query q = new XConstantScoreQuery(searchFilter);
                q.setBoost(query().getBoost());
                parsedQuery(new ParsedQuery(q, parsedQuery()));
            } else {
                parsedQuery(new ParsedQuery(new XFilteredQuery(query(), searchFilter), parsedQuery()));
            }
        }
    }

    public Filter searchFilter(String[] types) {
        Filter filter = mapperService().searchFilter(types);
        if (filter == null) {
            return aliasFilter;
        } else {
            filter = filterCache().cache(filter);
            if (aliasFilter != null) {
                return new AndFilter(ImmutableList.of(filter, aliasFilter));
            }
            return filter;
        }
    }

    public long id() {
        return this.id;
    }

    public String source() {
        return engineSearcher.source();
    }

    public ShardSearchRequest request() {
        return this.request;
    }

    public SearchType searchType() {
        return this.searchType;
    }

    public SearchContext searchType(SearchType searchType) {
        this.searchType = searchType;
        return this;
    }

    public SearchShardTarget shardTarget() {
        return this.shardTarget;
    }

    public int numberOfShards() {
        return request.numberOfShards();
    }

    public boolean hasTypes() {
        return request.types() != null && request.types().length > 0;
    }

    public String[] types() {
        return request.types();
    }

    public float queryBoost() {
        return queryBoost;
    }

    public SearchContext queryBoost(float queryBoost) {
        this.queryBoost = queryBoost;
        return this;
    }

    public long nowInMillis() {
        return request.nowInMillis();
    }

    public Scroll scroll() {
        return this.scroll;
    }

    public SearchContext scroll(Scroll scroll) {
        this.scroll = scroll;
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

    public SearchContextFacets facets() {
        return facets;
    }

    public SearchContext facets(SearchContextFacets facets) {
        this.facets = facets;
        return this;
    }

    public SearchContextHighlight highlight() {
        return highlight;
    }

    public void highlight(SearchContextHighlight highlight) {
        this.highlight = highlight;
    }

    public SuggestionSearchContext suggest() {
        return suggest;
    }

    public void suggest(SuggestionSearchContext suggest) {
        this.suggest = suggest;
    }

    public List<RescoreSearchContext> rescore() {
        if (rescore == null) {
            return Collections.emptyList();
        }
        return rescore;
    }

    public void addRescore(RescoreSearchContext rescore) {
        if (this.rescore == null) {
            this.rescore = new ArrayList<>();
        }
        this.rescore.add(rescore);
    }

    public boolean hasFieldDataFields() {
        return fieldDataFields != null;
    }

    public FieldDataFieldsContext fieldDataFields() {
        if (fieldDataFields == null) {
            fieldDataFields = new FieldDataFieldsContext();
        }
        return this.fieldDataFields;
    }

    public boolean hasScriptFields() {
        return scriptFields != null;
    }

    public ScriptFieldsContext scriptFields() {
        if (scriptFields == null) {
            scriptFields = new ScriptFieldsContext();
        }
        return this.scriptFields;
    }

    public boolean hasPartialFields() {
        return partialFields != null;
    }

    public PartialFieldsContext partialFields() {
        if (partialFields == null) {
            partialFields = new PartialFieldsContext();
        }
        return this.partialFields;
    }

    /**
     * A shortcut function to see whether there is a fetchSourceContext and it says the source is requested.
     *
     * @return
     */
    public boolean sourceRequested() {
        return fetchSourceContext != null && fetchSourceContext.fetchSource();
    }

    public boolean hasFetchSourceContext() {
        return fetchSourceContext != null;
    }

    public FetchSourceContext fetchSourceContext() {
        return this.fetchSourceContext;
    }

    public SearchContext fetchSourceContext(FetchSourceContext fetchSourceContext) {
        this.fetchSourceContext = fetchSourceContext;
        return this;
    }

    public ContextIndexSearcher searcher() {
        return this.searcher;
    }

    public IndexShard indexShard() {
        return this.indexShard;
    }

    public MapperService mapperService() {
        return indexService.mapperService();
    }

    public AnalysisService analysisService() {
        return indexService.analysisService();
    }

    public IndexQueryParserService queryParserService() {
        return indexService.queryParserService();
    }

    public SimilarityService similarityService() {
        return indexService.similarityService();
    }

    public ScriptService scriptService() {
        return scriptService;
    }

    public CacheRecycler cacheRecycler() {
        return cacheRecycler;
    }

    public PageCacheRecycler pageCacheRecycler() {
        return pageCacheRecycler;
    }

    public BigArrays bigArrays() {
        return bigArrays;
    }

    public FilterCache filterCache() {
        return indexService.cache().filter();
    }

    public DocSetCache docSetCache() {
        return indexService.cache().docSet();
    }

    public IndexFieldDataService fieldData() {
        return indexService.fieldData();
    }

    public long timeoutInMillis() {
        return timeoutInMillis;
    }

    public void timeoutInMillis(long timeoutInMillis) {
        this.timeoutInMillis = timeoutInMillis;
    }

    public SearchContext minimumScore(float minimumScore) {
        this.minimumScore = minimumScore;
        return this;
    }

    public Float minimumScore() {
        return this.minimumScore;
    }

    public SearchContext sort(Sort sort) {
        this.sort = sort;
        return this;
    }

    public Sort sort() {
        return this.sort;
    }

    public SearchContext trackScores(boolean trackScores) {
        this.trackScores = trackScores;
        return this;
    }

    public boolean trackScores() {
        return this.trackScores;
    }

    public SearchContext parsedPostFilter(ParsedFilter postFilter) {
        this.postFilter = postFilter;
        return this;
    }

    public ParsedFilter parsedPostFilter() {
        return this.postFilter;
    }

    public Filter aliasFilter() {
        return aliasFilter;
    }

    public SearchContext parsedQuery(ParsedQuery query) {
        queryRewritten = false;
        this.originalQuery = query;
        this.query = query.query();
        return this;
    }

    public ParsedQuery parsedQuery() {
        return this.originalQuery;
    }

    /**
     * The query to execute, might be rewritten.
     */
    public Query query() {
        return this.query;
    }

    /**
     * Has the query been rewritten already?
     */
    public boolean queryRewritten() {
        return queryRewritten;
    }

    /**
     * Rewrites the query and updates it. Only happens once.
     */
    public SearchContext updateRewriteQuery(Query rewriteQuery) {
        query = rewriteQuery;
        queryRewritten = true;
        return this;
    }

    public int from() {
        return from;
    }

    public SearchContext from(int from) {
        this.from = from;
        return this;
    }

    public int size() {
        return size;
    }

    public SearchContext size(int size) {
        this.size = size;
        return this;
    }

    public boolean hasFieldNames() {
        return fieldNames != null;
    }

    public List<String> fieldNames() {
        if (fieldNames == null) {
            fieldNames = Lists.newArrayList();
        }
        return fieldNames;
    }

    public void emptyFieldNames() {
        this.fieldNames = ImmutableList.of();
    }

    public boolean explain() {
        return explain;
    }

    public void explain(boolean explain) {
        this.explain = explain;
    }

    @Nullable
    public List<String> groupStats() {
        return this.groupStats;
    }

    public void groupStats(List<String> groupStats) {
        this.groupStats = groupStats;
    }

    public boolean version() {
        return version;
    }

    public void version(boolean version) {
        this.version = version;
    }

    public int[] docIdsToLoad() {
        return docIdsToLoad;
    }

    public int docIdsToLoadFrom() {
        return docsIdsToLoadFrom;
    }

    public int docIdsToLoadSize() {
        return docsIdsToLoadSize;
    }

    public SearchContext docIdsToLoad(int[] docIdsToLoad, int docsIdsToLoadFrom, int docsIdsToLoadSize) {
        this.docIdsToLoad = docIdsToLoad;
        this.docsIdsToLoadFrom = docsIdsToLoadFrom;
        this.docsIdsToLoadSize = docsIdsToLoadSize;
        return this;
    }

    public void accessed(long accessTime) {
        this.lastAccessTime = accessTime;
    }

    public long lastAccessTime() {
        return this.lastAccessTime;
    }

    public long keepAlive() {
        return this.keepAlive;
    }

    public void keepAlive(long keepAlive) {
        this.keepAlive = keepAlive;
    }

    @Override
    public void lastEmittedDoc(ScoreDoc doc) {
        this.lastEmittedDoc = doc;
    }

    @Override
    public ScoreDoc lastEmittedDoc() {
        return lastEmittedDoc;
    }

    public SearchLookup lookup() {
        // TODO: The types should take into account the parsing context in QueryParserContext...
        if (searchLookup == null) {
            searchLookup = new SearchLookup(mapperService(), fieldData(), request.types());
        }
        return searchLookup;
    }

    public DfsSearchResult dfsResult() {
        return dfsResult;
    }

    public QuerySearchResult queryResult() {
        return queryResult;
    }

    public FetchSearchResult fetchResult() {
        return fetchResult;
    }

    public ScanContext scanContext() {
        if (scanContext == null) {
            scanContext = new ScanContext();
        }
        return scanContext;
    }

    public MapperService.SmartNameFieldMappers smartFieldMappers(String name) {
        return mapperService().smartName(name, request.types());
    }

    public FieldMappers smartNameFieldMappers(String name) {
        return mapperService().smartNameFieldMappers(name, request.types());
    }

    public FieldMapper smartNameFieldMapper(String name) {
        return mapperService().smartNameFieldMapper(name, request.types());
    }

    public MapperService.SmartNameObjectMapper smartNameObjectMapper(String name) {
        return mapperService().smartNameObjectMapper(name, request.types());
    }

    @Override
    public boolean useSlowScroll() {
        return useSlowScroll;
    }

    public DefaultSearchContext useSlowScroll(boolean useSlowScroll) {
        this.useSlowScroll = useSlowScroll;
        return this;
    }
}
