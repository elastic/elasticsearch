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

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.elasticsearch.Version;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.common.util.concurrent.RefCounted;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.query.InnerHitContextBuilder;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.search.NestedHelper;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.script.FieldScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchException;
import org.elasticsearch.search.SearchExtBuilder;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.AggregationInitializationException;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService.MultiBucketConsumer;
import org.elasticsearch.search.aggregations.SearchContextAggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseContext;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.fetch.subphase.DocValueFieldsContext;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.ScriptFieldsContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.SearchContextHighlight;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.profile.Profilers;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.rescore.RescorerBuilder;
import org.elasticsearch.search.searchafter.SearchAfterBuilder;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.suggest.SuggestionSearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

import static org.elasticsearch.index.query.AbstractQueryBuilder.DEFAULT_BOOST;
import static org.elasticsearch.search.fetch.subphase.FetchSourceContext.FETCH_SOURCE;

/**
 * This class encapsulates the state needed to execute a search. It holds a reference to the
 * shards point in time snapshot (IndexReader / ContextIndexSearcher) and allows passing on
 * state from one query / fetch phase to another.
 *
 * This class also implements {@link RefCounted} since in some situations like in {@link SearchService}
 * a SearchContext can be closed concurrently due to independent events ie. when an index gets removed. To prevent accessing closed
 * IndexReader / IndexSearcher instances the SearchContext can be guarded by a reference count and fail if it's been closed by
 * an external event.
 *
 * NOTE: For reference why we use RefCounted here see #20095
 */
public class SearchContext extends AbstractRefCounted implements Releasable {
    public static final int DEFAULT_TERMINATE_AFTER = 0;
    public static final int TRACK_TOTAL_HITS_ACCURATE = Integer.MAX_VALUE;
    public static final int TRACK_TOTAL_HITS_DISABLED = -1;
    public static final int DEFAULT_TRACK_TOTAL_HITS_UP_TO = 10000;

    private final long id;
    private final String nodeId;
    private SearchTask task;
    private final SearchSourceBuilder source;
    private final IndexShard indexShard;
    private final SearchShardTarget shardTarget;
    private final LongSupplier relativeTimeSupplier;
    private final QueryShardContext queryShardContext;
    private final ContextIndexSearcher searcher;
    private final FetchPhase fetchPhase;
    private final int numberOfShards;
    private final boolean allowPartialResults;
    private final Query aliasFilter;
    private final Query sliceQuery;
    private final ParsedQuery originalQuery;
    private Query query;
    private boolean isQueryRewritten;
    private final ParsedQuery postFilter;
    // non-final because scrolls need to increment this value
    private int from;
    private final int size;
    private final SortAndFormats sort;
    private final Float minimumScore;
    private final boolean trackScores;
    private final int trackTotalHitsUpTo;
    private final FieldDoc searchAfter;
    private final boolean lowLevelCancellation;
    private final SearchType searchType;
    private final float queryBoost;
    private final TimeValue timeout;
    private final int terminateAfter;
    private final boolean explain;
    private final boolean version;
    private final boolean seqAndPrimaryTerm;
    private final CollapseContext collapse;
    private final ScrollContext scrollContext;
    private final StoredFieldsContext storedFields;
    private final ScriptFieldsContext scriptFields;
    private final FetchSourceContext fetchSourceContext;
    private final DocValueFieldsContext docValueFieldsContext;
    private final Map<String, InnerHitContextBuilder> innerHits;
    private final List<String> groupStats;
    // non-final because scrolls need to unset this value
    private SearchContextAggregations aggregations;
    private final SearchContextHighlight highlight;
    private final SuggestionSearchContext suggest;
    private final List<RescoreContext> rescorers;
    private final Map<String, SearchExtBuilder> searchExtBuilders;
    private final Profilers profilers;

    private Uid docUid;

    private final Map<Class<?>, Collector> queryCollectors = new HashMap<>();
    private final DfsSearchResult dfsResult;
    private final QuerySearchResult queryResult;
    private final FetchSearchResult fetchResult;

    private int[] docIdsToLoad;
    private int docsIdsToLoadFrom;
    private int docsIdsToLoadSize;

    private volatile long keepAlive;
    private final long originNanoTime = System.nanoTime();
    private volatile long lastAccessTime = -1;
    private final Runnable onClose;

    private Map<Lifetime, List<Releasable>> clearables = null;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private SearchContext(long id,
                          SearchTask task,
                          String nodeId,
                          SearchSourceBuilder source,
                          IndexShard indexShard,
                          SearchShardTarget shardTarget,
                          LongSupplier relativeTimeSupplier,
                          QueryShardContext queryShardContext,
                          ContextIndexSearcher searcher,
                          FetchPhase fetchPhase,
                          int numberOfShards,
                          boolean allowPartialResults,
                          Query aliasFilter,
                          Query sliceQuery,
                          ParsedQuery originalQuery,
                          ParsedQuery postFilter,
                          int from,
                          int size,
                          SortAndFormats sort,
                          Float minimumScore,
                          boolean trackScores,
                          int trackTotalHitsUpTo,
                          FieldDoc searchAfter,
                          boolean lowLevelCancellation,
                          SearchType searchType,
                          float queryBoost,
                          TimeValue timeout,
                          int terminateAfter,
                          boolean explain,
                          boolean version,
                          boolean seqAndPrimaryTerm,
                          boolean profile,
                          CollapseContext collapse,
                          ScrollContext scrollContext,
                          StoredFieldsContext storedFields,
                          ScriptFieldsContext scriptFields,
                          FetchSourceContext fetchSourceContext,
                          DocValueFieldsContext docValueFieldsContext,
                          Map<String, InnerHitContextBuilder> innerHits,
                          List<String> groupStats,
                          SearchContextAggregations aggregations,
                          SearchContextHighlight highlight,
                          SuggestionSearchContext suggest,
                          List<RescoreContext> rescorers,
                          Map<String, SearchExtBuilder> searchExtBuilders,
                          Runnable onClose) {
        super("search_context");
        this.id = id;
        this.task = task;
        this.nodeId = nodeId;
        this.source = source;
        this.indexShard = indexShard;
        this.shardTarget = shardTarget;
        this.relativeTimeSupplier = relativeTimeSupplier;
        this.queryShardContext = queryShardContext;
        this.searcher = searcher;
        this.fetchPhase = fetchPhase;
        this.numberOfShards = numberOfShards;
        this.allowPartialResults = allowPartialResults;
        this.aliasFilter = aliasFilter;
        this.sliceQuery = sliceQuery;
        this.originalQuery = originalQuery;
        this.postFilter = postFilter;
        this.from = from;
        this.size = size;
        this.sort = sort;
        this.minimumScore = minimumScore;
        this.trackScores = trackScores;
        this.trackTotalHitsUpTo = trackTotalHitsUpTo;
        this.searchAfter = searchAfter;
        this.lowLevelCancellation = lowLevelCancellation;
        this.searchType = searchType;
        this.queryBoost = queryBoost;
        this.timeout = timeout;
        this.terminateAfter = terminateAfter;
        this.explain = explain;
        this.version = version;
        this.seqAndPrimaryTerm = seqAndPrimaryTerm;
        this.collapse = collapse;
        this.scrollContext = scrollContext;
        this.storedFields = storedFields;
        this.scriptFields = scriptFields;
        this.fetchSourceContext = fetchSourceContext;
        this.docValueFieldsContext = docValueFieldsContext;
        this.innerHits = innerHits;
        this.groupStats = groupStats;
        this.aggregations = aggregations;
        this.highlight = highlight;
        this.suggest = suggest;
        this.rescorers = rescorers;
        this.searchExtBuilders = searchExtBuilders;
        this.profilers = profile ? new Profilers(searcher) : null;

        this.dfsResult = new DfsSearchResult(id, shardTarget);
        this.queryResult = new QuerySearchResult(id, shardTarget);
        this.fetchResult = new FetchSearchResult(id, shardTarget);
        this.onClose = onClose;

        this.query = buildFilteredQuery(originalQuery.query());
    }

    @Override
    public final void close() {
        if (closed.compareAndSet(false, true)) { // prevent double closing
            decRef();
        }
    }

    @Override
    protected final void closeInternal() {
        try {
            clearReleasables(Lifetime.CONTEXT);
        } finally {
            onClose.run();
        }
    }

    @Override
    protected void alreadyClosed() {
        throw new IllegalStateException("search context is already closed can't increment refCount current count [" + refCount() + "]");
    }

    /**
     * Automatically apply all required filters to the provided query such as
     * alias filters, types filters, etc.
     **/
    public Query buildFilteredQuery(Query query) {
        assert query != null;
        List<Query> filters = new ArrayList<>();

        if (queryShardContext.nestedScope().getObjectMapper() == null
                && mapperService().hasNested()
                && new NestedHelper(mapperService()).mightMatchNestedDocs(query)
                && (aliasFilter == null || new NestedHelper(mapperService()).mightMatchNestedDocs(aliasFilter))) {
            filters.add(Queries.newNonNestedFilter());
        }

        if (aliasFilter != null) {
            filters.add(aliasFilter);
        }

        if (sliceQuery != null) {
            filters.add(sliceQuery);
        }

        if (filters.isEmpty()) {
            return query;
        } else {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(query, BooleanClause.Occur.MUST);
            for (Query filter : filters) {
                builder.add(filter, BooleanClause.Occur.FILTER);
            }
            return builder.build();
        }
    }

    /**
     * Rewrites the main query.
     */
    public void rewriteQuery() {
        if (isQueryRewritten == false) {
            try {
                query = searcher.rewrite(query);
                isQueryRewritten = true;
            } catch (IOException e) {
                throw new SearchException(shardTarget, "failed to rewrite query");
            }
        }
    }

    public long id() {
        return id;
    }

    public String nodeId() {
        return nodeId;
    }

    public SearchSourceBuilder source() {
        return source;
    }

    public SearchType searchType() {
        return searchType;
    }

    public SearchShardTarget shardTarget() {
        return shardTarget;
    }

    public int numberOfShards() {
        return numberOfShards;
    }

    public float queryBoost() {
        return queryBoost;
    }

    public long getOriginNanoTime() {
        return originNanoTime;
    }

    @Nullable
    public ScrollContext scrollContext() {
        return this.scrollContext;
    }

    @Nullable
    public SearchContextAggregations aggregations() {
        return aggregations;
    }

    public void clearAggregations() {
        this.aggregations = null;
    }

    @Nullable
    public SearchExtBuilder getSearchExt(String name) {
        return searchExtBuilders.get(name);
    }

    @Nullable
    public SearchContextHighlight highlight() {
        return highlight;
    }

    public Map<String, InnerHitContextBuilder> innerHits() {
        return innerHits;
    }

    @Nullable
    public SuggestionSearchContext suggest() {
        return suggest;
    }

    public List<RescoreContext> rescore() {
        return rescorers;
    }

    public boolean hasScriptFields() {
        return scriptFields != null && scriptFields.fields().isEmpty() == false;
    }

    public ScriptFieldsContext scriptFields() {
        return scriptFields;
    }

    /**
     * A shortcut function to see whether there is a fetchSourceContext and it says the source is requested.
     */
    public boolean sourceRequested() {
        return fetchSourceContext != null && fetchSourceContext.fetchSource();
    }

    @Nullable
    public FetchSourceContext fetchSourceContext() {
        return fetchSourceContext;
    }

    @Nullable
    public DocValueFieldsContext docValueFieldsContext() {
        return docValueFieldsContext;
    }

    public ContextIndexSearcher searcher() {
        return this.searcher;
    }

    public IndexShard indexShard() {
        return this.indexShard;
    }

    public TimeValue timeout() {
        return timeout;
    }

    public int terminateAfter() {
        return terminateAfter;
    }

    public Float minimumScore() {
        return minimumScore;
    }

    @Nullable
    public SortAndFormats sort() {
        return this.sort;
    }

    public boolean trackScores() {
        return this.trackScores;
    }

    public int trackTotalHitsUpTo() {
        return trackTotalHitsUpTo;
    }

    public boolean lowLevelCancellation() {
        return lowLevelCancellation;
    }

    @Nullable
    public FieldDoc searchAfter() {
        return searchAfter;
    }

    @Nullable
    public CollapseContext collapse() {
        return collapse;
    }

    @Nullable
    public ParsedQuery parsedPostFilter() {
        return this.postFilter;
    }

    public Query aliasFilter() {
        return aliasFilter;
    }

    /**
     * The original query as sent by the user without the types and aliases
     * applied. Putting things in here leaks them into highlighting so don't add
     * things like the type filter or alias filters.
     */
    public ParsedQuery parsedQuery() {
        return this.originalQuery;
    }

    /**
     * The query to execute.
     */
    public Query query() {
        return query;
    }

    public void setFrom(int from) {
        this.from = from;
    }

    public int from() {
        return from;
    }

    public int size() {
        return size;
    }

    public boolean hasStoredFields() {
        return storedFields != null && storedFields.fieldNames() != null;
    }

    @Nullable
    public StoredFieldsContext storedFieldsContext() {
        return storedFields;
    }

    public boolean explain() {
        return explain;
    }

    @Nullable
    public List<String> groupStats() {
        return groupStats;
    }

    public boolean version() {
        return version;
    }

    public boolean seqNoAndPrimaryTerm() {
        return seqAndPrimaryTerm;
    }

    public int[] docIdToLoad() {
        return docIdsToLoad;
    }

    public int docIdsToLoadFrom() {
        return docsIdsToLoadFrom;
    }

    public int docIdsToLoadSize() {
        return docsIdsToLoadSize;
    }

    public void setDocIdsToLoad(int[] docIdsToLoad, int docsIdsToLoadFrom, int docsIdsToLoadSize) {
        this.docIdsToLoad = docIdsToLoad;
        this.docsIdsToLoadFrom = docsIdsToLoadFrom;
        this.docsIdsToLoadSize = docsIdsToLoadSize;
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

    public DfsSearchResult dfsResult() {
        return dfsResult;
    }

    public QuerySearchResult queryResult() {
        return queryResult;
    }

    public FetchPhase fetchPhase() {
        return fetchPhase;
    }

    public FetchSearchResult fetchResult() {
        return fetchResult;
    }

    public MapperService mapperService() {
        return queryShardContext.getMapperService();
    }

    public SimilarityService similarityService() {
        return queryShardContext.getSimilarityService();
    }

    public BigArrays bigArrays() {
        return queryShardContext.bigArrays();
    }

    public BitsetFilterCache bitsetFilterCache() {
        return queryShardContext.bitsetFilterCache();
    }

    public <IFD extends IndexFieldData<?>> IFD getForField(MappedFieldType fieldType) {
        return queryShardContext.getForField(fieldType);
    }

    public MappedFieldType smartNameFieldType(String name) {
        return mapperService().fullName(name);
    }

    public ObjectMapper getObjectMapper(String name) {
        return mapperService().getObjectMapper(name);
    }

    public long getRelativeTimeInMillis() {
        return relativeTimeSupplier.getAsLong();
    }

    public Map<Class<?>, Collector> queryCollectors() {
        return queryCollectors;
    }

    public QueryShardContext getQueryShardContext() {
        return queryShardContext;
    }

    @Nullable
    public Profilers getProfilers() {
        return profilers;
    }

    public void setTask(SearchTask task) {
        this.task = task;
    }

    public SearchTask getTask() {
        return task;
    }

    public boolean isCancelled() {
        return task != null && task.isCancelled();
    }

    /**
     * @return true if the request contains only suggest
     */
    public final boolean hasOnlySuggest() {
        return source != null && source.isSuggestOnly();
    }

    public SearchLookup lookup() {
        return queryShardContext.lookup();
    }

    /**
     * Returns the document {@link Uid} associated with this context.
     * See {@link FetchPhase}.
     */
    public Uid docUid() {
        return docUid;
    }

    /**
     * Associates a document {@link Uid} with this context.
     */
    public void setDocUid(Uid uid) {
        this.docUid = uid;
    }

    public ShardId shardId() {
        return shardTarget.getShardId();
    }

    public IndexSettings indexSettings() {
        return queryShardContext.getIndexSettings();
    }

    public boolean allowPartialSearchResults() {
        return allowPartialResults;
    }

    /**
     * The life time of an object that is used during search execution.
     */
    public enum Lifetime {
        /**
         * This life time is for objects that only live during collection time.
         */
        COLLECTION,
        /**
         * This life time is for objects that need to live until the end of the current search phase.
         */
        PHASE,
        /**
         * This life time is for objects that need to live until the search context they are attached to is destroyed.
         */
        CONTEXT
    }

    /**
     * Schedule the release of a resource. The time when {@link Releasable#close()} will be called on this object
     * is function of the provided {@link SearchContext.Lifetime}.
     */
    public void addReleasable(Releasable releasable, Lifetime lifetime) {
        if (clearables == null) {
            clearables = new EnumMap<>(Lifetime.class);
        }
        List<Releasable> releasables = clearables.get(lifetime);
        if (releasables == null) {
            releasables = new ArrayList<>();
            clearables.put(lifetime, releasables);
        }
        releasables.add(releasable);
    }

    public void clearReleasables(Lifetime lifetime) {
        if (clearables != null) {
            List<List<Releasable>>releasables = new ArrayList<>();
            for (Lifetime lc : Lifetime.values()) {
                if (lc.compareTo(lifetime) > 0) {
                    break;
                }
                List<Releasable> remove = clearables.remove(lc);
                if (remove != null) {
                    releasables.add(remove);
                }
            }
            Releasables.close(Iterables.flatten(releasables));
        }
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder().append(shardTarget());
        if (searchType() != SearchType.DEFAULT) {
            result.append("searchType=[").append(searchType()).append("]");
        }
        if (scrollContext() != null) {
            if (scrollContext().scroll != null) {
                result.append("scroll=[").append(scrollContext().scroll.keepAlive()).append("]");
            } else {
                result.append("scroll=[null]");
            }
        }
        result.append(" query=[").append(query()).append("]");
        return result.toString();
    }

    /**
     * A builder to create final {@link SearchContext}.
     */
    public static class Builder {
        private final long id;
        private final SearchTask task;
        private final String nodeId;
        private final QueryShardContext queryShardContext;
        private final SearchSourceBuilder source;
        private final IndexShard indexShard;
        private final SearchShardTarget shardTarget;
        private final ContextIndexSearcher searcher;
        private final FetchPhase fetchPhase;
        private final int numberOfShards;
        private final LongSupplier relativeTimeSupplier;

        private boolean allowPartialResults;

        private Query aliasFilter;
        private Query sliceQuery;
        private ParsedQuery query;
        private ParsedQuery postFilter;
        private int from = SearchService.DEFAULT_FROM;
        private int size = SearchService.DEFAULT_SIZE;
        private SortAndFormats sort;
        private Float minimumScore;
        private boolean trackScores;
        private Integer trackTotalHitsUpTo;
        private FieldDoc searchAfter;
        private boolean lowLevelCancellation;
        private SearchType searchType = SearchType.DEFAULT;
        private float queryBoost = DEFAULT_BOOST;
        private TimeValue timeout;
        private int terminateAfter = DEFAULT_TERMINATE_AFTER;
        private boolean explain;
        private boolean version;
        private boolean seqAndPrimaryTerm;
        private CollapseContext collapse;
        private ScrollContext scrollContext;
        private StoredFieldsContext storedFields;
        private ScriptFieldsContext scriptFields = new ScriptFieldsContext();
        private FetchSourceContext fetchSourceContext;
        private DocValueFieldsContext docValueFieldsContext;
        private Map<String, InnerHitContextBuilder> innerHits = Collections.emptyMap();
        private List<String> groupStats;
        private SearchContextAggregations aggregations;
        private SearchContextHighlight highlight;
        private SuggestionSearchContext suggest;
        private List<RescoreContext> rescorers = Collections.emptyList();
        private Map<String, SearchExtBuilder> searchExts = Collections.emptyMap();
        private boolean profile;

        /**
         * Returns a builder to create a {@link SearchContext}.
         *
         * @param id The id of the context.
         * @param task The task associated with the context.
         * @param nodeId The local node id.
         * @param indexShard The {@link IndexShard} for the search.
         * @param queryShardContext The {@link QueryShardContext} to use to parse the request.
         * @param searcher The {@link ContextIndexSearcher} for the search.
         * @param fetchPhase The {@link FetchPhase} to use for the search.
         * @param clusterAlias The cluster alias if the search is controlled by a remote cluster or null.
         * @param numberOfShards The number of shards that participate in the search.
         * @param relativeTimeSupplier The relative time supplier to use for timers.
         * @param source The original source builder.
         *
         * @warning This constructor does not parse the provided {@link SearchSourceBuilder}, it is only used
         * as the original source for logging/debug purpose.
         */
        public Builder(long id,
                       SearchTask task,
                       String nodeId,
                       IndexShard indexShard,
                       QueryShardContext queryShardContext,
                       ContextIndexSearcher searcher,
                       FetchPhase fetchPhase,
                       @Nullable String clusterAlias,
                       int numberOfShards,
                       LongSupplier relativeTimeSupplier,
                       SearchSourceBuilder source) {
            this.id = id;
            this.nodeId = nodeId;
            this.task = task;
            this.queryShardContext = queryShardContext;
            this.source = source;
            this.indexShard = indexShard;
            this.searcher = searcher;
            this.fetchPhase = fetchPhase;
            this.numberOfShards = numberOfShards;
            this.relativeTimeSupplier = relativeTimeSupplier;
            this.shardTarget = new SearchShardTarget(nodeId, indexShard.shardId(), clusterAlias, OriginalIndices.NONE);
        }

        public Builder setAllowPartialResults(boolean value) {
            this.allowPartialResults = value;
            return this;
        }


        public Builder setAliasFilter(Query aliasFilter) {
            this.aliasFilter = aliasFilter;
            return this;
        }

        public Builder setSliceQuery(Query sliceQuery) {
            this.sliceQuery = sliceQuery;
            return this;
        }

        public Builder setQuery(ParsedQuery query) {
            this.query = query;
            return this;
        }

        public Builder setPostFilter(ParsedQuery postFilter) {
            this.postFilter = postFilter;
            return this;
        }

        public Builder setFrom(int from) {
            this.from = from;
            return this;
        }

        public Builder setSize(int size) {
            this.size = size;
            return this;
        }

        public Builder setSort(SortAndFormats sort) {
            this.sort = sort;
            return this;
        }

        public Builder setMinimumScore(float minimumScore) {
            this.minimumScore = minimumScore;
            return this;
        }

        public Builder setTrackScores(boolean trackScores) {
            this.trackScores = trackScores;
            return this;
        }

        public Builder setTrackTotalHitsUpTo(Integer trackTotalHitsUpTo) {
            this.trackTotalHitsUpTo = trackTotalHitsUpTo;
            return this;
        }

        public Builder setSearchAfter(FieldDoc searchAfter) {
            this.searchAfter = searchAfter;
            return this;
        }

        public Builder setLowLevelCancellation(boolean lowLevelCancellation) {
            this.lowLevelCancellation = lowLevelCancellation;
            return this;
        }

        public Builder setSearchType(SearchType searchType) {
            this.searchType = searchType;
            return this;
        }

        public Builder setQueryBoost(float queryBoost) {
            this.queryBoost = queryBoost;
            return this;
        }

        public Builder setTimeout(TimeValue timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder setTerminateAfter(int terminateAfter) {
            this.terminateAfter = terminateAfter;
            return this;
        }

        public Builder setExplain(boolean explain) {
            this.explain = explain;
            return this;
        }

        public Builder setVersion(boolean version) {
            this.version = version;
            return this;
        }

        public Builder setSeqAndPrimaryTerm(boolean seqAndPrimaryTerm) {
            this.seqAndPrimaryTerm = seqAndPrimaryTerm;
            return this;
        }

        public Builder setCollapse(CollapseContext collapse) {
            this.collapse = collapse;
            return this;
        }

        public Builder setScroll(ScrollContext scrollContext) {
            this.scrollContext = scrollContext;
            return this;
        }

        public Builder setStoredFields(StoredFieldsContext storedFields) {
            this.storedFields = storedFields;
            return this;
        }

        public Builder setScriptFields(ScriptFieldsContext scriptFields) {
            this.scriptFields = scriptFields;
            return this;
        }

        public Builder setFetchSource(FetchSourceContext fetchSourceContext) {
            this.fetchSourceContext = fetchSourceContext;
            return this;
        }

        public Builder setDocValueFields(DocValueFieldsContext docValueFieldsContext) {
            this.docValueFieldsContext = docValueFieldsContext;
            return this;
        }

        public Builder setInnerHits(Map<String, InnerHitContextBuilder> innerHits) {
            this.innerHits = innerHits;
            return this;
        }

        public Builder setGroupStats(List<String> groupStats) {
            this.groupStats = groupStats;
            return this;
        }

        public Builder setHighlight(SearchContextHighlight highlight) {
            this.highlight = highlight;
            return this;
        }

        public Builder setSuggest(SuggestionSearchContext suggest) {
            this.suggest = suggest;
            return this;
        }

        public Builder setRescorers(List<RescoreContext> rescorers) {
            this.rescorers = rescorers;
            return this;
        }

        public Builder setSearchExt(Map<String, SearchExtBuilder> builders) {
            this.searchExts = builders;
            return this;
        }

        public Builder setProfile(boolean profile) {
            this.profile = profile;
            return this;
        }

        /**
         * Builds the highlight context from the provided builder.
         */
        public Builder buildHighlight(HighlightBuilder builder) throws SearchException {
            try {
                highlight = builder.build(queryShardContext);
            } catch (IOException e) {
                throw new SearchException(shardTarget, "failed to create SearchContextHighlighter", e);
            }
            return this;
        }

        /**
         * Builds the aggregations context from the provided builder.
         */
        public Builder buildAggregations(AggregatorFactories.Builder aggs,
                                         MultiBucketConsumer multiBucketConsumer) throws AggregationInitializationException {
            try {
                AggregatorFactories factories = aggs.build(queryShardContext, null);
                aggregations = new SearchContextAggregations(factories, multiBucketConsumer);
            } catch (IOException e) {
                throw new AggregationInitializationException("Failed to create aggregators", e);
            }
            return this;
        }

        /**
         * Builds the script fields context from the provided builder.
         */
        public Builder buildScriptFields(ScriptService scriptService,
                                         Collection<SearchSourceBuilder.ScriptField> fields) {
            int maxAllowedScriptFields = queryShardContext.getIndexSettings().getMaxScriptFields();
            if (fields.size() > maxAllowedScriptFields) {
                throw new IllegalArgumentException(
                    "Trying to retrieve too many script_fields. Must be less than or equal to: [" + maxAllowedScriptFields
                        + "] but was [" + fields.size() + "]. This limit can be set by changing the ["
                        + IndexSettings.MAX_SCRIPT_FIELDS_SETTING.getKey() + "] index level setting.");
            }
            List<ScriptFieldsContext.ScriptField> result = new ArrayList<>();
            for (SearchSourceBuilder.ScriptField field : fields) {
                FieldScript.Factory factory = scriptService.compile(field.script(), FieldScript.CONTEXT);
                FieldScript.LeafFactory searchScript = factory.newFactory(field.script().getParams(), queryShardContext.lookup());
                result.add(new ScriptFieldsContext.ScriptField(field.fieldName(), searchScript, field.ignoreFailure()));
            }
            this.scriptFields = new ScriptFieldsContext(result);
            return this;
        }

        /**
         * Builds the doc values context from the provided list of fields.
         */
        public Builder buildDocValues(List<DocValueFieldsContext.FieldAndFormat> fields) {
            List<DocValueFieldsContext.FieldAndFormat> docValueFields = new ArrayList<>();
            for (DocValueFieldsContext.FieldAndFormat format : fields) {
                Collection<String> fieldNames = queryShardContext.getMapperService().simpleMatchToFullName(format.field);
                for (String fieldName : fieldNames) {
                    docValueFields.add(new DocValueFieldsContext.FieldAndFormat(fieldName, format.format));
                }
            }
            this.docValueFieldsContext = new DocValueFieldsContext(docValueFields);
            return this;
        }

        /**
         * Creates the final {@link SearchContext} and set the provided
         * {@link Runnable} to be executed when the context is closed.
         */
        public SearchContext build(Runnable onClose) throws SearchException {
            validate();
            trackTotalHitsUpTo = trackTotalHitsUpTo != null ? trackTotalHitsUpTo : TRACK_TOTAL_HITS_ACCURATE;
            if (storedFields == null
                    && fetchSourceContext == null
                    && (scriptFields == null || scriptFields.fields().isEmpty())) {
                // no fields specified, default to return source if no explicit indication
                fetchSourceContext = new FetchSourceContext(true);
            } else if (storedFields != null && storedFields.fetchFields()) {
                for (String fieldNameOrPattern : storedFields.fieldNames()) {
                    if (fieldNameOrPattern.equals(SourceFieldMapper.NAME)) {
                        FetchSourceContext fetch = fetchSourceContext != null ? fetchSourceContext : FETCH_SOURCE;
                        fetchSourceContext = new FetchSourceContext(true, fetch.includes(), fetch.excludes());
                        break;
                    }
                }
            }
            if (collapse != null) {
                // retrieve the `doc_value` associated with the collapse field
                String name = collapse.getFieldName();
                if (docValueFieldsContext == null) {
                    docValueFieldsContext = new DocValueFieldsContext(
                        Collections.singletonList(new DocValueFieldsContext.FieldAndFormat(name, null))
                    );
                } else if (docValueFieldsContext.fields().stream().map(ff -> ff.field).anyMatch(name::equals) == false) {
                    docValueFieldsContext.fields().add(new DocValueFieldsContext.FieldAndFormat(name, null));
                }
            }

            if (query == null) {
                query = ParsedQuery.parsedMatchAllQuery();
            }
            if (queryBoost != DEFAULT_BOOST) {
                query = new ParsedQuery(new BoostQuery(query.query(), queryBoost), query.namedFilters());
            }

            return new SearchContext(id,
                task,
                nodeId,
                source,
                indexShard,
                shardTarget,
                relativeTimeSupplier,
                queryShardContext,
                searcher,
                fetchPhase,
                numberOfShards,
                allowPartialResults,
                aliasFilter,
                sliceQuery,
                query,
                postFilter,
                from,
                size,
                sort,
                minimumScore,
                trackScores,
                trackTotalHitsUpTo,
                searchAfter,
                lowLevelCancellation,
                searchType,
                queryBoost,
                timeout,
                terminateAfter,
                explain,
                version,
                seqAndPrimaryTerm,
                profile,
                collapse,
                scrollContext,
                storedFields,
                scriptFields,
                fetchSourceContext,
                docValueFieldsContext,
                innerHits,
                groupStats,
                aggregations,
                highlight,
                suggest,
                rescorers,
                searchExts,
                onClose);
        }

        private void validate() {
            long resultWindow = (long) from + size;
            int maxResultWindow = queryShardContext.getIndexSettings().getMaxResultWindow();

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
            if (rescorers.isEmpty() == false) {
                if (sort != null && Sort.RELEVANCE.equals(sort.sort) == false) {
                    throw new IllegalArgumentException("Cannot use [sort] option in conjunction with [rescore].");
                }
                int maxWindow = queryShardContext.getIndexSettings().getMaxRescoreWindow();
                for (RescoreContext rescoreContext: rescorers) {
                    if (rescoreContext.getWindowSize() > maxWindow) {
                        throw new IllegalArgumentException("Rescore window [" + rescoreContext.getWindowSize() + "] is too large. "
                            + "It must be less than [" + maxWindow + "]. This prevents allocating massive heaps for storing the results "
                            + "to be rescored. This limit can be set by changing the [" + IndexSettings.MAX_RESCORE_WINDOW_SETTING.getKey()
                            + "] index level setting.");
                    }
                }
            }
            if (trackTotalHitsUpTo != null
                    && trackTotalHitsUpTo != TRACK_TOTAL_HITS_ACCURATE
                    && scrollContext != null) {
                throw new SearchException(shardTarget, "disabling [track_total_hits] is not allowed in a scroll context");
            }
            int maxAllowedDocvalueFields = queryShardContext.getIndexSettings().getMaxDocvalueFields();
            if (docValueFieldsContext != null && docValueFieldsContext.fields().size() > maxAllowedDocvalueFields) {
                throw new IllegalArgumentException(
                    "Trying to retrieve too many docvalue_fields. Must be less than or equal to: [" + maxAllowedDocvalueFields
                        + "] but was [" + docValueFieldsContext.fields().size() + "]. This limit can be set by changing the ["
                        + IndexSettings.MAX_DOCVALUE_FIELDS_SEARCH_SETTING.getKey() + "] index level setting.");
            }

            if (searchAfter != null && searchAfter.fields.length > 0) {
                if (scrollContext != null) {
                    throw new SearchException(shardTarget, "`search_after` cannot be used in a scroll context.");
                }
                if (from > 0) {
                    throw new SearchException(shardTarget, "`from` parameter must be set to 0 when `search_after` is used.");
                }
            }
            if (sliceQuery != null && scrollContext == null) {
                throw new SearchException(shardTarget, "`slice` cannot be used outside of a scroll context");
            }

            if (storedFields != null && storedFields.fetchFields() == false) {
                if (version) {
                    throw new SearchException(shardTarget, "`stored_fields` cannot be disabled if version is requested");
                }
                if (fetchSourceContext != null && fetchSourceContext.fetchSource()) {
                    throw new SearchException(shardTarget, "`stored_fields` cannot be disabled if _source is requested");
                }
            }

            if (collapse != null) {
                if (scrollContext != null) {
                    throw new SearchException(shardTarget, "cannot use `collapse` in a scroll context");
                }
                if (searchAfter != null) {
                    throw new SearchException(shardTarget, "cannot use `collapse` in conjunction with `search_after`");
                }
                if (rescorers != null && rescorers.isEmpty() == false) {
                    throw new SearchException(shardTarget, "cannot use `collapse` in conjunction with `rescore`");
                }
            }
        }
    }

    /**
     * Parses the provided {@link ShardSearchRequest}.
     *
     * @param builder The search context {@link Builder}.
     * @param request The resuest to parse.
     * @param clusterService The cluster service to build the slice query (if any).
     * @param multiBucketConsumer The bucket consumer for aggregations.
     */
    public static Builder parseShardSearchRequest(Builder builder,
                                                  ShardSearchRequest request,
                                                  ClusterService clusterService,
                                                  MultiBucketConsumer multiBucketConsumer) throws SearchException {
        final SearchShardTarget shardTarget = builder.shardTarget;
        final QueryShardContext queryShardContext = builder.queryShardContext;
        builder.setSearchType(request.searchType());
        builder.setAllowPartialResults(request.allowPartialSearchResults());
        builder.setQueryBoost(request.indexBoost());
        if (request.scroll() != null) {
            ScrollContext scrollContext = new ScrollContext();
            scrollContext.scroll = request.scroll();
            builder.setScroll(scrollContext);
        }
        if (request.getAliasFilter().getQueryBuilder() != null) {
            QueryBuilder aliasBuilder = request.getAliasFilter().getQueryBuilder();
            try {
                builder.setAliasFilter(aliasBuilder.toQuery(queryShardContext));
            } catch (IOException e) {
                throw new SearchException(builder.shardTarget, "failed to create alias filter", e);
            }
        }

        SearchSourceBuilder source = request.source();
        // nothing to parse...
        if (source == null) {
            return builder;
        }
        if (source.from() != -1) {
            builder.setFrom(source.from());
        }
        if (source.size() != -1) {
            builder.setSize(source.size());
        }
        Map<String, InnerHitContextBuilder> innerHits = new HashMap<>();
        if (source.query() != null) {
            InnerHitContextBuilder.extractInnerHits(source.query(), innerHits);
            builder.setQuery(queryShardContext.toQuery(source.query()));
        }
        if (source.postFilter() != null) {
            InnerHitContextBuilder.extractInnerHits(source.postFilter(), innerHits);
            builder.setPostFilter(queryShardContext.toQuery(source.postFilter()));
        }
        if (innerHits.size() > 0) {
            for (Map.Entry<String, InnerHitContextBuilder> entry : innerHits.entrySet()) {
                entry.getValue().validate(queryShardContext);
            }
        }
        builder.setInnerHits(innerHits);
        if (source.sorts() != null) {
            try {
                Optional<SortAndFormats> optionalSort = SortBuilder.buildSort(source.sorts(), queryShardContext);
                if (optionalSort.isPresent()) {
                    builder.setSort(optionalSort.get());
                }
            } catch (IOException e) {
                throw new SearchException(shardTarget, "failed to create sort elements", e);
            }
        }
        builder.setTrackScores(source.trackScores());
        if (source.trackTotalHitsUpTo() != null) {
            builder.setTrackTotalHitsUpTo(source.trackTotalHitsUpTo());
        }
        if (source.minScore() != null) {
            builder.setMinimumScore(source.minScore());
        }
        builder.setProfile(source.profile());
        if (source.timeout() != null) {
            builder.setTimeout(source.timeout());
        }
        builder.setTerminateAfter(source.terminateAfter());

        if (source.aggregations() != null) {
            builder.buildAggregations(source.aggregations(), multiBucketConsumer);
        }
        if (source.suggest() != null) {
            try {
                builder.setSuggest(source.suggest().build(queryShardContext));
            } catch (IOException e) {
                throw new SearchException(shardTarget, "failed to create SuggestionSearchContext", e);
            }
        }
        if (source.rescores() != null) {
            try {
                List<RescoreContext> rescorers = new ArrayList<>();
                for (RescorerBuilder<?> rescore : source.rescores()) {
                    rescorers.add(rescore.buildContext(queryShardContext));
                }
                builder.setRescorers(rescorers);
            } catch (IOException e) {
                throw new SearchException(shardTarget, "failed to create RescoreSearchContext", e);
            }
        }
        if (source.explain() != null) {
            builder.setExplain(source.explain());
        }
        if (source.fetchSource() != null) {
            builder.setFetchSource(source.fetchSource());
        }
        if (source.docValueFields() != null) {
            builder.buildDocValues(source.docValueFields());
        }
        if (source.highlighter() != null) {
            builder.buildHighlight(source.highlighter());
        }
        if (source.scriptFields() != null && source.size() != 0) {
            builder.buildScriptFields(queryShardContext.getScriptService(), source.scriptFields());
        }
        if (source.ext() != null) {
            Map<String, SearchExtBuilder>  searchExtBuilders = new HashMap<>();
            for (SearchExtBuilder searchExtBuilder : source.ext()) {
                searchExtBuilders.put(searchExtBuilder.getWriteableName(), searchExtBuilder);
            }
            builder.setSearchExt(searchExtBuilders);
        }
        if (source.version() != null) {
            builder.setVersion(source.version());
        }

        if (source.seqNoAndPrimaryTerm() != null) {
            builder.setSeqAndPrimaryTerm(source.seqNoAndPrimaryTerm());
        }

        if (source.stats() != null) {
            builder.setGroupStats(source.stats());
        }
        if (source.searchAfter() != null && source.searchAfter().length > 0) {
            FieldDoc fieldDoc = SearchAfterBuilder.buildFieldDoc(builder.sort, source.searchAfter());
            builder.setSearchAfter(fieldDoc);
        }

        if (source.slice() != null) {
            if (source.slice() != null) {
                int sliceLimit = queryShardContext.getIndexSettings().getMaxSlicesPerScroll();
                int numSlices = source.slice().getMax();
                if (numSlices > sliceLimit) {
                    throw new IllegalArgumentException("The number of slices [" + numSlices + "] is too large. It must "
                        + "be less than [" + sliceLimit + "]. This limit can be set by changing the [" +
                        IndexSettings.MAX_SLICES_PER_SCROLL.getKey() + "] index level setting.");
                }
            }
            builder.setSliceQuery(source.slice().toFilter(clusterService, request, queryShardContext, Version.CURRENT));
        }

        if (source.storedFields() != null) {
            builder.setStoredFields(source.storedFields());
        }

        if (source.collapse() != null) {
            builder.setCollapse(source.collapse().build(queryShardContext));
        }
        return builder;
    }
}
