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

import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Counter;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.WeightFactorFunction;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.query.IndexQueryParserService;
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
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.query.QueryPhaseExecutionException;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rescore.RescoreSearchContext;
import org.elasticsearch.search.suggest.SuggestionSearchContext;

import java.io.IOException;
import java.util.*;

/**
 *
 */
public class DefaultSearchContext extends SearchContext {
    /**
     * Index setting describing the maximum value of from + size on a query.
     */
    public static final String MAX_RESULT_WINDOW = "index.max_result_window";
    public static class Defaults {
        /**
         * Default maximum value of from + size on a query. 10,000 was chosen as
         * a conservative default as it is sure to not cause trouble. Users can
         * certainly profile their cluster and decide to set it to 100,000
         * safely. 1,000,000 is probably way to high for any cluster to set
         * safely.
         */
        public static final int MAX_RESULT_WINDOW = 10000;
    }

    private final long id;
    private final ShardSearchRequest request;
    private final SearchShardTarget shardTarget;
    private final Counter timeEstimateCounter;
    private SearchType searchType;
    private final Engine.Searcher engineSearcher;
    private final ScriptService scriptService;
    private final PageCacheRecycler pageCacheRecycler;
    private final BigArrays bigArrays;
    private final IndexShard indexShard;
    private final IndexService indexService;
    private final ContextIndexSearcher searcher;
    private final DfsSearchResult dfsResult;
    private final QuerySearchResult queryResult;
    private final FetchSearchResult fetchResult;
    private float queryBoost = 1.0f;
    // timeout in millis
    private long timeoutInMillis;
    // terminate after count
    private int terminateAfter = DEFAULT_TERMINATE_AFTER;
    private List<String> groupStats;
    private ScrollContext scrollContext;
    private boolean explain;
    private boolean version = false; // by default, we don't return versions
    private List<String> fieldNames;
    private ScriptFieldsContext scriptFields;
    private FetchSourceContext fetchSourceContext;
    private int from = -1;
    private int size = -1;
    private Sort sort;
    private Float minimumScore;
    private boolean trackScores = false; // when sorting, track scores as well...
    private ParsedQuery originalQuery;
    private Query query;
    private ParsedQuery postFilter;
    private Query aliasFilter;
    private int[] docIdsToLoad;
    private int docsIdsToLoadFrom;
    private int docsIdsToLoadSize;
    private SearchContextAggregations aggregations;
    private SearchContextHighlight highlight;
    private SuggestionSearchContext suggest;
    private List<RescoreSearchContext> rescore;
    private SearchLookup searchLookup;
    private volatile long keepAlive;
    private ScoreDoc lastEmittedDoc;
    private final long originNanoTime = System.nanoTime();
    private volatile long lastAccessTime = -1;
    private InnerHitsContext innerHitsContext;

    private final Map<String, FetchSubPhaseContext> subPhaseContexts = new HashMap<>();
    private final Map<Class<?>, Collector> queryCollectors = new HashMap<>();

    public DefaultSearchContext(long id, ShardSearchRequest request, SearchShardTarget shardTarget,
                                Engine.Searcher engineSearcher, IndexService indexService, IndexShard indexShard,
                                ScriptService scriptService, PageCacheRecycler pageCacheRecycler,
                                BigArrays bigArrays, Counter timeEstimateCounter, ParseFieldMatcher parseFieldMatcher,
                                TimeValue timeout
    ) {
        super(parseFieldMatcher, request);
        this.id = id;
        this.request = request;
        this.searchType = request.searchType();
        this.shardTarget = shardTarget;
        this.engineSearcher = engineSearcher;
        this.scriptService = scriptService;
        this.pageCacheRecycler = pageCacheRecycler;
        // SearchContexts use a BigArrays that can circuit break
        this.bigArrays = bigArrays.withCircuitBreaking();
        this.dfsResult = new DfsSearchResult(id, shardTarget);
        this.queryResult = new QuerySearchResult(id, shardTarget);
        this.fetchResult = new FetchSearchResult(id, shardTarget);
        this.indexShard = indexShard;
        this.indexService = indexService;
        this.searcher = new ContextIndexSearcher(this, engineSearcher);
        this.timeEstimateCounter = timeEstimateCounter;
        this.timeoutInMillis = timeout.millis();
    }

    @Override
    public void doClose() {
        // clear and scope phase we  have
        Releasables.close(searcher, engineSearcher);
    }

    /**
     * Should be called before executing the main query and after all other parameters have been set.
     */
    @Override
    public void preProcess() {
        if (scrollContext == null) {
            long from = from() == -1 ? 0 : from();
            long size = size() == -1 ? 10 : size();
            long resultWindow = from + size;
            // We need settingsService's view of the settings because its dynamic.
            // indexService's isn't.
            int maxResultWindow = indexService.settingsService().getSettings().getAsInt(MAX_RESULT_WINDOW, Defaults.MAX_RESULT_WINDOW);

            if (resultWindow > maxResultWindow) {
                throw new QueryPhaseExecutionException(this,
                        "Result window is too large, from + size must be less than or equal to: [" + maxResultWindow + "] but was ["
                                + resultWindow + "]. See the scroll api for a more efficient way to request large data sets. "
                                + "This limit can be set by changing the [" + DefaultSearchContext.MAX_RESULT_WINDOW
                                + "] index level parameter.");
            }
        }

        // initialize the filtering alias based on the provided filters
        aliasFilter = indexService.aliasesService().aliasFilter(request.filteringAliases());

        if (query() == null) {
            parsedQuery(ParsedQuery.parsedMatchAllQuery());
        }
        if (queryBoost() != 1.0f) {
            parsedQuery(new ParsedQuery(new FunctionScoreQuery(query(), new WeightFactorFunction(queryBoost)), parsedQuery()));
        }
        Query searchFilter = searchFilter(types());
        if (searchFilter != null) {
            if (Queries.isConstantMatchAllQuery(query())) {
                Query q = new ConstantScoreQuery(searchFilter);
                q.setBoost(query().getBoost());
                parsedQuery(new ParsedQuery(q, parsedQuery()));
            } else {
                BooleanQuery filtered = new BooleanQuery.Builder()
                    .add(query(), Occur.MUST)
                    .add(searchFilter, Occur.FILTER)
                    .build();
                parsedQuery(new ParsedQuery(filtered, parsedQuery()));
            }
        }
        try {
            this.query = searcher().rewrite(this.query);
        } catch (IOException e) {
            throw new QueryPhaseExecutionException(this, "Failed to rewrite main query", e);
        }
    }

    @Override
    public Query searchFilter(String[] types) {
        Query filter = mapperService().searchFilter(types);
        if (filter == null && aliasFilter == null) {
            return null;
        }
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        if (filter != null) {
            bq.add(filter, Occur.MUST);
        }
        if (aliasFilter != null) {
            bq.add(aliasFilter, Occur.MUST);
        }
        return new ConstantScoreQuery(bq.build());
    }

    @Override
    public long id() {
        return this.id;
    }

    @Override
    public String source() {
        return engineSearcher.source();
    }

    @Override
    public ShardSearchRequest request() {
        return this.request;
    }

    @Override
    public SearchType searchType() {
        return this.searchType;
    }

    @Override
    public SearchContext searchType(SearchType searchType) {
        this.searchType = searchType;
        return this;
    }

    @Override
    public SearchShardTarget shardTarget() {
        return this.shardTarget;
    }

    @Override
    public int numberOfShards() {
        return request.numberOfShards();
    }

    @Override
    public boolean hasTypes() {
        return request.types() != null && request.types().length > 0;
    }

    @Override
    public String[] types() {
        return request.types();
    }

    @Override
    public float queryBoost() {
        return queryBoost;
    }

    @Override
    public SearchContext queryBoost(float queryBoost) {
        this.queryBoost = queryBoost;
        return this;
    }

    @Override
    public long getOriginNanoTime() {
        return originNanoTime;
    }

    @Override
    protected long nowInMillisImpl() {
        return request.nowInMillis();
    }

    @Override
    public ScrollContext scrollContext() {
        return this.scrollContext;
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
    public <SubPhaseContext extends FetchSubPhaseContext> SubPhaseContext getFetchSubPhaseContext(FetchSubPhase.ContextFactory<SubPhaseContext> contextFactory) {
        String subPhaseName = contextFactory.getName();
        if (subPhaseContexts.get(subPhaseName) == null) {
            subPhaseContexts.put(subPhaseName, contextFactory.newContextInstance());
        }
        return (SubPhaseContext) subPhaseContexts.get(subPhaseName);
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
    public SuggestionSearchContext suggest() {
        return suggest;
    }

    @Override
    public void suggest(SuggestionSearchContext suggest) {
        this.suggest = suggest;
    }

    @Override
    public List<RescoreSearchContext> rescore() {
        if (rescore == null) {
            return Collections.emptyList();
        }
        return rescore;
    }

    @Override
    public void addRescore(RescoreSearchContext rescore) {
        if (this.rescore == null) {
            this.rescore = new ArrayList<>();
        }
        this.rescore.add(rescore);
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

    /**
     * A shortcut function to see whether there is a fetchSourceContext and it says the source is requested.
     */
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
        return this.fetchSourceContext;
    }

    @Override
    public SearchContext fetchSourceContext(FetchSourceContext fetchSourceContext) {
        this.fetchSourceContext = fetchSourceContext;
        return this;
    }

    @Override
    public ContextIndexSearcher searcher() {
        return this.searcher;
    }

    @Override
    public IndexShard indexShard() {
        return this.indexShard;
    }

    @Override
    public MapperService mapperService() {
        return indexService.mapperService();
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
    public BitsetFilterCache bitsetFilterCache() {
        return indexService.bitsetFilterCache();
    }

    @Override
    public IndexFieldDataService fieldData() {
        return indexService.fieldData();
    }

    @Override
    public long timeoutInMillis() {
        return timeoutInMillis;
    }

    @Override
    public void timeoutInMillis(long timeoutInMillis) {
        this.timeoutInMillis = timeoutInMillis;
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
        this.minimumScore = minimumScore;
        return this;
    }

    @Override
    public Float minimumScore() {
        return this.minimumScore;
    }

    @Override
    public SearchContext sort(Sort sort) {
        this.sort = sort;
        return this;
    }

    @Override
    public Sort sort() {
        return this.sort;
    }

    @Override
    public SearchContext trackScores(boolean trackScores) {
        this.trackScores = trackScores;
        return this;
    }

    @Override
    public boolean trackScores() {
        return this.trackScores;
    }

    @Override
    public SearchContext parsedPostFilter(ParsedQuery postFilter) {
        this.postFilter = postFilter;
        return this;
    }

    @Override
    public ParsedQuery parsedPostFilter() {
        return this.postFilter;
    }

    @Override
    public Query aliasFilter() {
        return aliasFilter;
    }

    @Override
    public SearchContext parsedQuery(ParsedQuery query) {
        this.originalQuery = query;
        this.query = query.query();
        return this;
    }

    @Override
    public ParsedQuery parsedQuery() {
        return this.originalQuery;
    }

    /**
     * The query to execute, in its rewritten form.
     */
    @Override
    public Query query() {
        return this.query;
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
            fieldNames = new ArrayList<>();
        }
        return fieldNames;
    }

    @Override
    public void emptyFieldNames() {
        this.fieldNames = Collections.emptyList();
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
    @Nullable
    public List<String> groupStats() {
        return this.groupStats;
    }

    @Override
    public void groupStats(List<String> groupStats) {
        this.groupStats = groupStats;
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
        this.lastAccessTime = accessTime;
    }

    @Override
    public long lastAccessTime() {
        return this.lastAccessTime;
    }

    @Override
    public long keepAlive() {
        return this.keepAlive;
    }

    @Override
    public void keepAlive(long keepAlive) {
        this.keepAlive = keepAlive;
    }

    @Override
    public SearchLookup lookup() {
        // TODO: The types should take into account the parsing context in QueryParserContext...
        if (searchLookup == null) {
            searchLookup = new SearchLookup(mapperService(), fieldData(), request.types());
        }
        return searchLookup;
    }

    @Override
    public DfsSearchResult dfsResult() {
        return dfsResult;
    }

    @Override
    public QuerySearchResult queryResult() {
        return queryResult;
    }

    @Override
    public FetchSearchResult fetchResult() {
        return fetchResult;
    }

    @Override
    public MappedFieldType smartNameFieldType(String name) {
        return mapperService().smartNameFieldType(name, request.types());
    }

    @Override
    public MappedFieldType smartNameFieldTypeFromAnyType(String name) {
        return mapperService().smartNameFieldType(name);
    }

    @Override
    public ObjectMapper getObjectMapper(String name) {
        return mapperService().getObjectMapper(name, request.types());
    }

    @Override
    public Counter timeEstimateCounter() {
        return timeEstimateCounter;
    }

    @Override
    public void innerHits(InnerHitsContext innerHitsContext) {
        this.innerHitsContext = innerHitsContext;
    }

    @Override
    public InnerHitsContext innerHits() {
        return innerHitsContext;
    }

    @Override
    public Map<Class<?>, Collector> queryCollectors() {
        return queryCollectors;
    }
}
