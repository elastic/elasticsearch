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

package org.elasticsearch.search;

import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.action.search.SearchShardTask;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.WeightFactorFunction;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.search.NestedHelper;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.similarity.SimilarityService;
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
import org.elasticsearch.search.query.QueryPhaseExecutionException;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.suggest.SuggestionSearchContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

final class DefaultSearchContext extends SearchContext {

    private final long id;
    private final ShardSearchRequest request;
    private final SearchShardTarget shardTarget;
    private final LongSupplier relativeTimeSupplier;
    private SearchType searchType;
    private final Engine.Searcher engineSearcher;
    private final BigArrays bigArrays;
    private final IndexShard indexShard;
    private final ClusterService clusterService;
    private final IndexService indexService;
    private final ContextIndexSearcher searcher;
    private final DfsSearchResult dfsResult;
    private final QuerySearchResult queryResult;
    private final FetchSearchResult fetchResult;
    private final float queryBoost;
    private TimeValue timeout;
    // terminate after count
    private int terminateAfter = DEFAULT_TERMINATE_AFTER;
    private List<String> groupStats;
    private ScrollContext scrollContext;
    private boolean explain;
    private boolean version = false; // by default, we don't return versions
    private boolean seqAndPrimaryTerm = false;
    private StoredFieldsContext storedFields;
    private ScriptFieldsContext scriptFields;
    private FetchSourceContext fetchSourceContext;
    private DocValueFieldsContext docValueFieldsContext;
    private int from = -1;
    private int size = -1;
    private SortAndFormats sort;
    private Float minimumScore;
    private boolean trackScores = false; // when sorting, track scores as well...
    private int trackTotalHitsUpTo = SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO;
    private FieldDoc searchAfter;
    private CollapseContext collapse;
    private boolean lowLevelCancellation;
    // filter for sliced scroll
    private SliceBuilder sliceBuilder;
    private SearchShardTask task;

    /**
     * The original query as sent by the user without the types and aliases
     * applied. Putting things in here leaks them into highlighting so don't add
     * things like the type filter or alias filters.
     */
    private ParsedQuery originalQuery;

    /**
     * The query to actually execute.
     */
    private Query query;
    private ParsedQuery postFilter;
    private Query aliasFilter;
    private int[] docIdsToLoad;
    private int docsIdsToLoadFrom;
    private int docsIdsToLoadSize;
    private SearchContextAggregations aggregations;
    private SearchContextHighlight highlight;
    private SuggestionSearchContext suggest;
    private List<RescoreContext> rescore;
    private volatile long keepAlive;
    private final long originNanoTime = System.nanoTime();
    private volatile long lastAccessTime = -1;
    private Profilers profilers;

    private final Map<String, SearchExtBuilder> searchExtBuilders = new HashMap<>();
    private final Map<Class<?>, Collector> queryCollectors = new HashMap<>();
    private final QueryShardContext queryShardContext;
    private final FetchPhase fetchPhase;

    DefaultSearchContext(long id, ShardSearchRequest request, SearchShardTarget shardTarget,
                         Engine.Searcher engineSearcher, ClusterService clusterService, IndexService indexService,
                         IndexShard indexShard, BigArrays bigArrays, LongSupplier relativeTimeSupplier, TimeValue timeout,
                         FetchPhase fetchPhase) {
        this.id = id;
        this.request = request;
        this.fetchPhase = fetchPhase;
        this.searchType = request.searchType();
        this.shardTarget = shardTarget;
        this.engineSearcher = engineSearcher;
        // SearchContexts use a BigArrays that can circuit break
        this.bigArrays = bigArrays.withCircuitBreaking();
        this.dfsResult = new DfsSearchResult(id, shardTarget);
        this.queryResult = new QuerySearchResult(id, shardTarget);
        this.fetchResult = new FetchSearchResult(id, shardTarget);
        this.indexShard = indexShard;
        this.indexService = indexService;
        this.clusterService = clusterService;
        this.searcher = new ContextIndexSearcher(engineSearcher.getIndexReader(), engineSearcher.getSimilarity(),
            engineSearcher.getQueryCache(), engineSearcher.getQueryCachingPolicy());
        this.relativeTimeSupplier = relativeTimeSupplier;
        this.timeout = timeout;
        queryShardContext = indexService.newQueryShardContext(request.shardId().id(), searcher,
            request::nowInMillis, shardTarget.getClusterAlias());
        queryBoost = request.indexBoost();
    }

    @Override
    public void doClose() {
        Releasables.close(engineSearcher);
    }

    /**
     * Should be called before executing the main query and after all other parameters have been set.
     */
    @Override
    public void preProcess(boolean rewrite) {
        if (hasOnlySuggest() ) {
            return;
        }
        long from = from() == -1 ? 0 : from();
        long size = size() == -1 ? 10 : size();
        long resultWindow = from + size;
        int maxResultWindow = indexService.getIndexSettings().getMaxResultWindow();

        if (resultWindow > maxResultWindow) {
            if (scrollContext == null) {
                throw new IllegalArgumentException(
                        "Result window is too large, from + size must be less than or equal to: [" + maxResultWindow + "] but was ["
                                + resultWindow + "]. See the scroll api for a more efficient way to request large data sets. "
                                + "This limit can be set by changing the [" + IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey()
                                + "] index level setting.");
            }
            throw new IllegalArgumentException(
                    "Batch size is too large, size must be less than or equal to: [" + maxResultWindow + "] but was [" + resultWindow
                            + "]. Scroll batch sizes cost as much memory as result windows so they are controlled by the ["
                            + IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey() + "] index level setting.");
        }
        if (rescore != null) {
            if (sort != null) {
                throw new IllegalArgumentException("Cannot use [sort] option in conjunction with [rescore].");
            }
            int maxWindow = indexService.getIndexSettings().getMaxRescoreWindow();
            for (RescoreContext rescoreContext: rescore) {
                if (rescoreContext.getWindowSize() > maxWindow) {
                    throw new IllegalArgumentException("Rescore window [" + rescoreContext.getWindowSize() + "] is too large. "
                            + "It must be less than [" + maxWindow + "]. This prevents allocating massive heaps for storing the results "
                            + "to be rescored. This limit can be set by changing the [" + IndexSettings.MAX_RESCORE_WINDOW_SETTING.getKey()
                            + "] index level setting.");
                }
            }
        }

        if (sliceBuilder != null) {
            int sliceLimit = indexService.getIndexSettings().getMaxSlicesPerScroll();
            int numSlices = sliceBuilder.getMax();
            if (numSlices > sliceLimit) {
                throw new IllegalArgumentException("The number of slices [" + numSlices + "] is too large. It must "
                    + "be less than [" + sliceLimit + "]. This limit can be set by changing the [" +
                    IndexSettings.MAX_SLICES_PER_SCROLL.getKey() + "] index level setting.");
            }
        }

        // initialize the filtering alias based on the provided filters
        try {
            final QueryBuilder queryBuilder = request.getAliasFilter().getQueryBuilder();
            aliasFilter = queryBuilder == null ? null : queryBuilder.toQuery(queryShardContext);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        if (query() == null) {
            parsedQuery(ParsedQuery.parsedMatchAllQuery());
        }
        if (queryBoost() != AbstractQueryBuilder.DEFAULT_BOOST) {
            parsedQuery(new ParsedQuery(new FunctionScoreQuery(query(), new WeightFactorFunction(queryBoost)), parsedQuery()));
        }
        this.query = buildFilteredQuery(query);
        if (rewrite) {
            try {
                this.query = searcher.rewrite(query);
            } catch (IOException e) {
                throw new QueryPhaseExecutionException(shardTarget, "Failed to rewrite main query", e);
            }
        }
    }

    @Override
    public Query buildFilteredQuery(Query query) {
        List<Query> filters = new ArrayList<>();

        if (mapperService().hasNested()
                && new NestedHelper(mapperService()).mightMatchNestedDocs(query)
                && (aliasFilter == null || new NestedHelper(mapperService()).mightMatchNestedDocs(aliasFilter))) {
            filters.add(Queries.newNonNestedFilter());
        }

        if (aliasFilter != null) {
            filters.add(aliasFilter);
        }

        if (sliceBuilder != null) {
            Query slicedQuery = sliceBuilder.toFilter(clusterService, request, queryShardContext);
            if (slicedQuery instanceof MatchNoDocsQuery) {
                return slicedQuery;
            } else {
                filters.add(slicedQuery);
            }
        }

        if (filters.isEmpty()) {
            return query;
        } else {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(query, Occur.MUST);
            for (Query filter : filters) {
                builder.add(filter, Occur.FILTER);
            }
            return builder.build();
        }
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
    public SearchShardTarget shardTarget() {
        return this.shardTarget;
    }

    @Override
    public int numberOfShards() {
        return request.numberOfShards();
    }

    @Override
    public float queryBoost() {
        return queryBoost;
    }

    @Override
    public long getOriginNanoTime() {
        return originNanoTime;
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
    public void addSearchExt(SearchExtBuilder searchExtBuilder) {
        //it's ok to use the writeable name here given that we enforce it to be the same as the name of the element that gets
        //parsed by the corresponding parser. There is one single name and one single way to retrieve the parsed object from the context.
        searchExtBuilders.put(searchExtBuilder.getWriteableName(), searchExtBuilder);
    }

    @Override
    public SearchExtBuilder getSearchExt(String name) {
        return searchExtBuilders.get(name);
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
    public List<RescoreContext> rescore() {
        if (rescore == null) {
            return Collections.emptyList();
        }
        return rescore;
    }

    @Override
    public void addRescore(RescoreContext rescore) {
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
    public DocValueFieldsContext docValueFieldsContext() {
        return docValueFieldsContext;
    }

    @Override
    public SearchContext docValueFieldsContext(DocValueFieldsContext docValueFieldsContext) {
        this.docValueFieldsContext = docValueFieldsContext;
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
    public SimilarityService similarityService() {
        return indexService.similarityService();
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
    public <IFD extends IndexFieldData<?>> IFD getForField(MappedFieldType fieldType) {
        return queryShardContext.getForField(fieldType);
    }

    @Override
    public TimeValue timeout() {
        return timeout;
    }

    @Override
    public void timeout(TimeValue timeout) {
        this.timeout = timeout;
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
    public SearchContext sort(SortAndFormats sort) {
        this.sort = sort;
        return this;
    }

    @Override
    public SortAndFormats sort() {
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
    public SearchContext trackTotalHitsUpTo(int trackTotalHitsUpTo) {
        this.trackTotalHitsUpTo = trackTotalHitsUpTo;
        return this;
    }

    @Override
    public int trackTotalHitsUpTo() {
        return trackTotalHitsUpTo;
    }

    @Override
    public SearchContext searchAfter(FieldDoc searchAfter) {
        this.searchAfter = searchAfter;
        return this;
    }

    @Override
    public boolean lowLevelCancellation() {
        return lowLevelCancellation;
    }

    public void lowLevelCancellation(boolean lowLevelCancellation) {
        this.lowLevelCancellation = lowLevelCancellation;
    }

    @Override
    public FieldDoc searchAfter() {
        return searchAfter;
    }

    @Override
    public SearchContext collapse(CollapseContext collapse) {
        this.collapse = collapse;
        return this;
    }

    @Override
    public CollapseContext collapse() {
        return collapse;
    }

    public SearchContext sliceBuilder(SliceBuilder sliceBuilder) {
        this.sliceBuilder = sliceBuilder;
        return this;
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
    public boolean hasStoredFields() {
        return storedFields != null && storedFields.fieldNames() != null;
    }

    @Override
    public boolean hasStoredFieldsContext() {
        return storedFields != null;
    }

    @Override
    public StoredFieldsContext storedFieldsContext() {
        return storedFields;
    }

    @Override
    public SearchContext storedFieldsContext(StoredFieldsContext storedFieldsContext) {
        this.storedFields = storedFieldsContext;
        return this;
    }

    @Override
    public boolean storedFieldsRequested() {
        return storedFields == null || storedFields.fetchFields();
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
    public boolean seqNoAndPrimaryTerm() {
        return seqAndPrimaryTerm;
    }

    @Override
    public void seqNoAndPrimaryTerm(boolean seqNoAndPrimaryTerm) {
        this.seqAndPrimaryTerm = seqNoAndPrimaryTerm;
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
    public DfsSearchResult dfsResult() {
        return dfsResult;
    }

    @Override
    public QuerySearchResult queryResult() {
        return queryResult;
    }

    @Override
    public FetchPhase fetchPhase() {
        return fetchPhase;
    }

    @Override
    public FetchSearchResult fetchResult() {
        return fetchResult;
    }

    @Override
    public MappedFieldType smartNameFieldType(String name) {
        return mapperService().fieldType(name);
    }

    @Override
    public ObjectMapper getObjectMapper(String name) {
        return mapperService().getObjectMapper(name);
    }

    @Override
    public long getRelativeTimeInMillis() {
        return relativeTimeSupplier.getAsLong();
    }

    @Override
    public Map<Class<?>, Collector> queryCollectors() {
        return queryCollectors;
    }

    @Override
    public QueryShardContext getQueryShardContext() {
        return queryShardContext;
    }

    @Override
    public Profilers getProfilers() {
        return profilers;
    }

    public void setProfilers(Profilers profilers) {
        this.profilers = profilers;
    }

    @Override
    public void setTask(SearchShardTask task) {
        this.task = task;
    }

    @Override
    public SearchShardTask getTask() {
        return task;
    }

    @Override
    public boolean isCancelled() {
        return task.isCancelled();
    }
}
