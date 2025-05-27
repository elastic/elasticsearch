/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.mapper.IdLoader;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.NestedLookup;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.search.NestedHelper;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.search.aggregations.SearchContextAggregations;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseContext;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.fetch.subphase.FetchDocValuesContext;
import org.elasticsearch.search.fetch.subphase.FetchFieldsContext;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.ScriptFieldsContext;
import org.elasticsearch.search.fetch.subphase.highlight.SearchHighlightContext;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.search.internal.ScrollContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.profile.Profilers;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rank.context.QueryPhaseRankShardContext;
import org.elasticsearch.search.rank.feature.RankFeatureResult;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.rescore.RescorePhase;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.suggest.SuggestionSearchContext;
import org.elasticsearch.tasks.CancellableTask;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.LongSupplier;
import java.util.function.ToLongFunction;

import static org.elasticsearch.search.SearchService.DEFAULT_SIZE;

final class DefaultSearchContext extends SearchContext {

    private final ReaderContext readerContext;
    private final ShardSearchRequest request;
    private final SearchShardTarget shardTarget;
    private final LongSupplier relativeTimeSupplier;
    private final SearchType searchType;
    private final IndexShard indexShard;
    private final IndexService indexService;
    private final ContextIndexSearcher searcher;
    private final long memoryAccountingBufferSize;
    private DfsSearchResult dfsResult;
    private QuerySearchResult queryResult;
    private RankFeatureResult rankFeatureResult;
    private FetchSearchResult fetchResult;
    private final float queryBoost;
    private final boolean lowLevelCancellation;
    private TimeValue timeout;
    // terminate after count
    private int terminateAfter = DEFAULT_TERMINATE_AFTER;
    private List<String> groupStats;
    private boolean explain;
    private boolean version = false; // by default, we don't return versions
    private boolean seqAndPrimaryTerm = false;
    private StoredFieldsContext storedFields;
    private ScriptFieldsContext scriptFields;
    private FetchSourceContext fetchSourceContext;
    private FetchDocValuesContext docValuesContext;
    private FetchFieldsContext fetchFieldsContext;
    private int from = -1;
    private int size = -1;
    private SortAndFormats sort;
    private Float minimumScore;
    private boolean trackScores = false; // when sorting, track scores as well...
    private int trackTotalHitsUpTo = SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO;
    private FieldDoc searchAfter;
    private CollapseContext collapse;
    // filter for sliced scroll
    private SliceBuilder sliceBuilder;
    private CancellableTask task;
    private QueryPhaseRankShardContext queryPhaseRankShardContext;

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
    private SearchContextAggregations aggregations;
    private SearchHighlightContext highlight;
    private SuggestionSearchContext suggest;
    private List<RescoreContext> rescore;
    private Profilers profilers;

    private final Map<String, SearchExtBuilder> searchExtBuilders = new HashMap<>();
    private final SearchExecutionContext searchExecutionContext;
    private final FetchPhase fetchPhase;

    DefaultSearchContext(
        ReaderContext readerContext,
        ShardSearchRequest request,
        SearchShardTarget shardTarget,
        LongSupplier relativeTimeSupplier,
        TimeValue timeout,
        FetchPhase fetchPhase,
        boolean lowLevelCancellation,
        Executor executor,
        SearchService.ResultsType resultsType,
        boolean enableQueryPhaseParallelCollection,
        int minimumDocsPerSlice,
        long memoryAccountingBufferSize
    ) throws IOException {
        this.readerContext = readerContext;
        this.request = request;
        this.fetchPhase = fetchPhase;
        boolean success = false;
        try {
            this.searchType = request.searchType();
            this.shardTarget = shardTarget;
            this.indexService = readerContext.indexService();
            this.indexShard = readerContext.indexShard();
            this.memoryAccountingBufferSize = memoryAccountingBufferSize;

            Engine.Searcher engineSearcher = readerContext.acquireSearcher("search");
            int maximumNumberOfSlices = determineMaximumNumberOfSlices(
                executor,
                request,
                resultsType,
                enableQueryPhaseParallelCollection,
                field -> getFieldCardinality(field, readerContext.indexService(), engineSearcher.getDirectoryReader())
            );
            if (executor == null || maximumNumberOfSlices <= 1) {
                this.searcher = new ContextIndexSearcher(
                    engineSearcher.getIndexReader(),
                    engineSearcher.getSimilarity(),
                    engineSearcher.getQueryCache(),
                    engineSearcher.getQueryCachingPolicy(),
                    lowLevelCancellation
                );
            } else {
                this.searcher = new ContextIndexSearcher(
                    engineSearcher.getIndexReader(),
                    engineSearcher.getSimilarity(),
                    engineSearcher.getQueryCache(),
                    engineSearcher.getQueryCachingPolicy(),
                    lowLevelCancellation,
                    executor,
                    maximumNumberOfSlices,
                    minimumDocsPerSlice
                );
            }
            closeFuture.addListener(ActionListener.releasing(Releasables.wrap(engineSearcher, searcher)));
            this.relativeTimeSupplier = relativeTimeSupplier;
            this.timeout = timeout;
            searchExecutionContext = indexService.newSearchExecutionContext(
                request.shardId().id(),
                request.shardRequestIndex(),
                searcher,
                request::nowInMillis,
                shardTarget.getClusterAlias(),
                request.getRuntimeMappings(),
                request.source() == null ? null : request.source().size()
            );
            queryBoost = request.indexBoost();
            this.lowLevelCancellation = lowLevelCancellation;
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    static long getFieldCardinality(String field, IndexService indexService, DirectoryReader directoryReader) {
        MappedFieldType mappedFieldType = indexService.mapperService().fieldType(field);
        if (mappedFieldType == null) {
            return -1;
        }
        IndexFieldData<?> indexFieldData;
        try {
            indexFieldData = indexService.loadFielddata(mappedFieldType, FieldDataContext.noRuntimeFields("field cardinality"));
        } catch (Exception e) {
            // loading fielddata for runtime fields will fail, that's ok
            return -1;
        }
        return getFieldCardinality(indexFieldData, directoryReader);
    }

    static long getFieldCardinality(IndexFieldData<?> indexFieldData, DirectoryReader directoryReader) {
        if (indexFieldData instanceof IndexOrdinalsFieldData indexOrdinalsFieldData) {
            if (indexOrdinalsFieldData.supportsGlobalOrdinalsMapping()) {
                IndexOrdinalsFieldData global = indexOrdinalsFieldData.loadGlobal(directoryReader);
                OrdinalMap ordinalMap = global.getOrdinalMap();
                if (ordinalMap != null) {
                    return ordinalMap.getValueCount();
                }
                if (directoryReader.leaves().isEmpty()) {
                    return 0;
                }
                return global.load(directoryReader.leaves().get(0)).getOrdinalsValues().getValueCount();
            }
        } else if (indexFieldData instanceof IndexNumericFieldData indexNumericFieldData) {
            final IndexNumericFieldData.NumericType type = indexNumericFieldData.getNumericType();
            try {
                if (type == IndexNumericFieldData.NumericType.INT || type == IndexNumericFieldData.NumericType.SHORT) {
                    final IndexReader reader = directoryReader.getContext().reader();
                    final byte[] min = PointValues.getMinPackedValue(reader, indexFieldData.getFieldName());
                    final byte[] max = PointValues.getMaxPackedValue(reader, indexFieldData.getFieldName());
                    if (min != null && max != null) {
                        return NumericUtils.sortableBytesToInt(max, 0) - NumericUtils.sortableBytesToInt(min, 0) + 1;
                    }
                } else if (type == IndexNumericFieldData.NumericType.LONG) {
                    final IndexReader reader = directoryReader.getContext().reader();
                    final byte[] min = PointValues.getMinPackedValue(reader, indexFieldData.getFieldName());
                    final byte[] max = PointValues.getMaxPackedValue(reader, indexFieldData.getFieldName());
                    if (min != null && max != null) {
                        return NumericUtils.sortableBytesToLong(max, 0) - NumericUtils.sortableBytesToLong(min, 0) + 1;
                    }
                }
            } catch (IOException ioe) {
                return -1L;
            }
        }
        //
        return -1L;
    }

    static int determineMaximumNumberOfSlices(
        Executor executor,
        ShardSearchRequest request,
        SearchService.ResultsType resultsType,
        boolean enableQueryPhaseParallelCollection,
        ToLongFunction<String> fieldCardinality
    ) {
        return executor instanceof ThreadPoolExecutor tpe
            && tpe.getQueue().size() <= tpe.getMaximumPoolSize()
            && isParallelCollectionSupportedForResults(resultsType, request.source(), fieldCardinality, enableQueryPhaseParallelCollection)
                ? tpe.getMaximumPoolSize()
                : 1;
    }

    static boolean isParallelCollectionSupportedForResults(
        SearchService.ResultsType resultsType,
        SearchSourceBuilder source,
        ToLongFunction<String> fieldCardinality,
        boolean isQueryPhaseParallelismEnabled
    ) {
        if (resultsType == SearchService.ResultsType.DFS) {
            return true;
        }
        if (resultsType == SearchService.ResultsType.QUERY && isQueryPhaseParallelismEnabled) {
            return source == null || source.supportsParallelCollection(fieldCardinality);
        }
        return false;
    }

    @Override
    public void addRankFeatureResult() {
        this.rankFeatureResult = new RankFeatureResult(this.readerContext.id(), this.shardTarget, this.request);
        addReleasable(rankFeatureResult::decRef);
    }

    @Override
    public RankFeatureResult rankFeatureResult() {
        return rankFeatureResult;
    }

    @Override
    public void addFetchResult() {
        this.fetchResult = new FetchSearchResult(this.readerContext.id(), this.shardTarget);
        addReleasable(fetchResult::decRef);
    }

    @Override
    public void addQueryResult() {
        this.queryResult = new QuerySearchResult(this.readerContext.id(), this.shardTarget, this.request);
        addReleasable(queryResult::decRef);
    }

    @Override
    public void addDfsResult() {
        this.dfsResult = new DfsSearchResult(this.readerContext.id(), this.shardTarget, this.request);
    }

    /**
     * Should be called before executing the main query and after all other parameters have been set.
     */
    @Override
    public void preProcess() {
        if (hasOnlySuggest()) {
            return;
        }
        long from = from() == -1 ? 0 : from();
        long size = size() == -1 ? DEFAULT_SIZE : size();
        long resultWindow = from + size;
        int maxResultWindow = indexService.getIndexSettings().getMaxResultWindow();

        if (resultWindow > maxResultWindow) {
            if (scrollContext() == null) {
                throw new IllegalArgumentException(
                    "Result window is too large, from + size must be less than or equal to: ["
                        + maxResultWindow
                        + "] but was ["
                        + resultWindow
                        + "]. See the scroll api for a more efficient way to request large data sets. "
                        + "This limit can be set by changing the ["
                        + IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey()
                        + "] index level setting."
                );
            }
            throw new IllegalArgumentException(
                "Batch size is too large, size must be less than or equal to: ["
                    + maxResultWindow
                    + "] but was ["
                    + resultWindow
                    + "]. Scroll batch sizes cost as much memory as result windows so they are controlled by the ["
                    + IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey()
                    + "] index level setting."
            );
        }
        if (rescore != null) {
            if (RescorePhase.validateSort(sort) == false) {
                throw new IllegalArgumentException("Cannot use [sort] option in conjunction with [rescore].");
            }
            int maxWindow = indexService.getIndexSettings().getMaxRescoreWindow();
            for (RescoreContext rescoreContext : rescore()) {
                if (rescoreContext.getWindowSize() > maxWindow) {
                    throw new IllegalArgumentException(
                        "Rescore window ["
                            + rescoreContext.getWindowSize()
                            + "] is too large. "
                            + "It must be less than ["
                            + maxWindow
                            + "]. This prevents allocating massive heaps for storing the results "
                            + "to be rescored. This limit can be set by changing the ["
                            + IndexSettings.MAX_RESCORE_WINDOW_SETTING.getKey()
                            + "] index level setting."
                    );
                }
            }
        }

        if (sliceBuilder != null && scrollContext() != null) {
            int sliceLimit = indexService.getIndexSettings().getMaxSlicesPerScroll();
            int numSlices = sliceBuilder.getMax();
            if (numSlices > sliceLimit) {
                throw new IllegalArgumentException(
                    "The number of slices ["
                        + numSlices
                        + "] is too large. It must "
                        + "be less than ["
                        + sliceLimit
                        + "]. This limit can be set by changing the ["
                        + IndexSettings.MAX_SLICES_PER_SCROLL.getKey()
                        + "] index level setting."
                );
            }
        }

        // initialize the filtering alias based on the provided filters
        try {
            final QueryBuilder queryBuilder = request.getAliasFilter().getQueryBuilder();
            aliasFilter = queryBuilder == null ? null : queryBuilder.toQuery(searchExecutionContext);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        if (query == null) {
            parsedQuery(ParsedQuery.parsedMatchAllQuery());
        }
        if (queryBoost != AbstractQueryBuilder.DEFAULT_BOOST) {
            parsedQuery(new ParsedQuery(new BoostQuery(query, queryBoost), parsedQuery()));
        }
        this.query = buildFilteredQuery(query);
        if (lowLevelCancellation) {
            searcher().addQueryCancellation(() -> {
                final CancellableTask task = getTask();
                if (task != null) {
                    task.ensureNotCancelled();
                }
            });
        }
    }

    @Override
    public Query buildFilteredQuery(Query query) {
        List<Query> filters = new ArrayList<>();
        NestedLookup nestedLookup = searchExecutionContext.nestedLookup();
        if (nestedLookup != NestedLookup.EMPTY
            && NestedHelper.mightMatchNestedDocs(query, searchExecutionContext)
            && (aliasFilter == null || NestedHelper.mightMatchNestedDocs(aliasFilter, searchExecutionContext))) {
            filters.add(Queries.newNonNestedFilter(searchExecutionContext.indexVersionCreated()));
        }

        if (aliasFilter != null) {
            filters.add(aliasFilter);
        }

        if (sliceBuilder != null) {
            Query slicedQuery = sliceBuilder.toFilter(request, searchExecutionContext);
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
    public ShardSearchContextId id() {
        return readerContext.id();
    }

    @Override
    public String source() {
        return "search";
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
    public ScrollContext scrollContext() {
        return readerContext.scrollContext();
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

    public void addSearchExt(SearchExtBuilder searchExtBuilder) {
        // it's ok to use the writeable name here given that we enforce it to be the same as the name of the element that gets
        // parsed by the corresponding parser. There is one single name and one single way to retrieve the parsed object from the context.
        searchExtBuilders.put(searchExtBuilder.getWriteableName(), searchExtBuilder);
    }

    @Override
    public SearchExtBuilder getSearchExt(String name) {
        return searchExtBuilders.get(name);
    }

    @Override
    public SearchHighlightContext highlight() {
        return highlight;
    }

    @Override
    public void highlight(SearchHighlightContext highlight) {
        this.highlight = highlight;
    }

    @Override
    public SuggestionSearchContext suggest() {
        return suggest;
    }

    public void suggest(SuggestionSearchContext suggest) {
        this.suggest = suggest;
    }

    @Override
    public QueryPhaseRankShardContext queryPhaseRankShardContext() {
        return queryPhaseRankShardContext;
    }

    @Override
    public void queryPhaseRankShardContext(QueryPhaseRankShardContext queryPhaseRankShardContext) {
        this.queryPhaseRankShardContext = queryPhaseRankShardContext;
    }

    @Override
    public List<RescoreContext> rescore() {
        if (rescore == null) {
            return List.of();
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
        return scriptFields != null && scriptFields.fields().isEmpty() == false;
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
    public FetchSourceContext fetchSourceContext() {
        return this.fetchSourceContext;
    }

    @Override
    public SearchContext fetchSourceContext(FetchSourceContext fetchSourceContext) {
        this.fetchSourceContext = fetchSourceContext;
        return this;
    }

    @Override
    public FetchDocValuesContext docValuesContext() {
        return docValuesContext;
    }

    @Override
    public SearchContext docValuesContext(FetchDocValuesContext docValuesContext) {
        this.docValuesContext = docValuesContext;
        return this;
    }

    @Override
    public FetchFieldsContext fetchFieldsContext() {
        return fetchFieldsContext;
    }

    @Override
    public SearchContext fetchFieldsContext(FetchFieldsContext fetchFieldsContext) {
        this.fetchFieldsContext = fetchFieldsContext;
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
    public BitsetFilterCache bitsetFilterCache() {
        return indexService.cache().bitsetFilterCache();
    }

    @Override
    public TimeValue timeout() {
        return timeout;
    }

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

    @Override
    public FieldDoc searchAfter() {
        return searchAfter;
    }

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
    public SearchContext parsedQuery(ParsedQuery query) {
        this.originalQuery = query;
        this.query = query.query();
        return this;
    }

    @Override
    public ParsedQuery parsedQuery() {
        return this.originalQuery;
    }

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
    public StoredFieldsContext storedFieldsContext() {
        return storedFields;
    }

    @Override
    public SearchContext storedFieldsContext(StoredFieldsContext storedFieldsContext) {
        this.storedFields = storedFieldsContext;
        return this;
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
    public DfsSearchResult dfsResult() {
        return dfsResult;
    }

    @Override
    public QuerySearchResult queryResult() {
        return queryResult;
    }

    public void addAggregationContext(AggregationContext aggregationContext) {
        queryResult.addAggregationContext(aggregationContext);
    }

    @Override
    public TotalHits getTotalHits() {
        if (queryResult != null) {
            return queryResult.getTotalHits();
        }
        return null;
    }

    @Override
    public float getMaxScore() {
        if (queryResult != null) {
            return queryResult.getMaxScore();
        }
        return Float.NaN;
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
    public long getRelativeTimeInMillis() {
        return relativeTimeSupplier.getAsLong();
    }

    @Override
    public SearchExecutionContext getSearchExecutionContext() {
        return searchExecutionContext;
    }

    @Override
    public Profilers getProfilers() {
        return profilers;
    }

    @Override
    public CircuitBreaker circuitBreaker() {
        return indexService.breakerService().getBreaker(CircuitBreaker.REQUEST);
    }

    @Override
    public long memAccountingBufferSize() {
        return memoryAccountingBufferSize;
    }

    public void setProfilers(Profilers profilers) {
        this.profilers = profilers;
    }

    @Override
    public void setTask(CancellableTask task) {
        this.task = task;
    }

    @Override
    public CancellableTask getTask() {
        return task;
    }

    @Override
    public boolean isCancelled() {
        return task.isCancelled();
    }

    @Override
    public ReaderContext readerContext() {
        return readerContext;
    }

    @Override
    public SourceLoader newSourceLoader() {
        return searchExecutionContext.newSourceLoader(request.isForceSyntheticSource());
    }

    @Override
    public IdLoader newIdLoader() {
        if (indexService.getIndexSettings().getMode() == IndexMode.TIME_SERIES) {
            IndexRouting.ExtractFromSource indexRouting = null;
            List<String> routingPaths = null;
            if (indexService.getIndexSettings().getIndexVersionCreated().before(IndexVersions.TIME_SERIES_ROUTING_HASH_IN_ID)) {
                indexRouting = (IndexRouting.ExtractFromSource) indexService.getIndexSettings().getIndexRouting();
                routingPaths = indexService.getMetadata().getRoutingPaths();
                for (String routingField : routingPaths) {
                    if (routingField.contains("*")) {
                        // In case the routing fields include path matches, find any matches and add them as distinct fields
                        // to the routing path.
                        Set<String> matchingRoutingPaths = new TreeSet<>(routingPaths);
                        for (Mapper mapper : indexService.mapperService().mappingLookup().fieldMappers()) {
                            if (mapper instanceof KeywordFieldMapper && indexRouting.matchesField(mapper.fullPath())) {
                                matchingRoutingPaths.add(mapper.fullPath());
                            }
                        }
                        routingPaths = new ArrayList<>(matchingRoutingPaths);
                        break;
                    }
                }
            }
            return IdLoader.createTsIdLoader(
                indexRouting,
                routingPaths,
                indexService.getIndexSettings().getIndexVersionCreated().onOrAfter(IndexVersions.TIME_SERIES_ID_LONG)
            );
        } else {
            return IdLoader.fromLeafStoredFieldLoader();
        }
    }
}
