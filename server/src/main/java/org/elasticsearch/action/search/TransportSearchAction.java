/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.RemoteClusterActionType;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsAction;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.ExecutorSelector;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.rest.action.search.SearchResponseMetrics;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.profile.SearchProfileResults;
import org.elasticsearch.search.profile.SearchProfileShardResult;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.action.search.SearchType.DFS_QUERY_THEN_FETCH;
import static org.elasticsearch.action.search.SearchType.QUERY_THEN_FETCH;
import static org.elasticsearch.action.search.TransportSearchHelper.checkCCSVersionCompatibility;
import static org.elasticsearch.search.sort.FieldSortBuilder.hasPrimaryFieldSort;
import static org.elasticsearch.threadpool.ThreadPool.Names.SYSTEM_CRITICAL_READ;
import static org.elasticsearch.threadpool.ThreadPool.Names.SYSTEM_READ;

public class TransportSearchAction extends HandledTransportAction<SearchRequest, SearchResponse> {

    public static final String NAME = "indices:data/read/search";
    public static final ActionType<SearchResponse> TYPE = new ActionType<>(NAME);
    public static final RemoteClusterActionType<SearchResponse> REMOTE_TYPE = new RemoteClusterActionType<>(NAME, SearchResponse::new);
    private static final Logger logger = LogManager.getLogger(TransportSearchAction.class);
    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(TransportSearchAction.class);
    public static final String FROZEN_INDICES_DEPRECATION_MESSAGE = "Searching frozen indices [{}] is deprecated."
        + " Consider cold or frozen tiers in place of frozen indices. The frozen feature will be removed in a feature release.";

    /** The maximum number of shards for a single search request. */
    public static final Setting<Long> SHARD_COUNT_LIMIT_SETTING = Setting.longSetting(
        "action.search.shard_count.limit",
        Long.MAX_VALUE,
        1L,
        Property.Dynamic,
        Property.NodeScope
    );

    public static final Setting<Integer> DEFAULT_PRE_FILTER_SHARD_SIZE = Setting.intSetting(
        "action.search.pre_filter_shard_size.default",
        SearchRequest.DEFAULT_PRE_FILTER_SHARD_SIZE,
        1,
        Property.NodeScope
    );

    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final TransportService transportService;
    private final SearchTransportService searchTransportService;
    private final RemoteClusterService remoteClusterService;
    private final SearchPhaseController searchPhaseController;
    private final SearchService searchService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final CircuitBreaker circuitBreaker;
    private final ExecutorSelector executorSelector;
    private final int defaultPreFilterShardSize;
    private final boolean ccsCheckCompatibility;
    private final SearchResponseMetrics searchResponseMetrics;

    @Inject
    public TransportSearchAction(
        ThreadPool threadPool,
        CircuitBreakerService circuitBreakerService,
        TransportService transportService,
        SearchService searchService,
        SearchTransportService searchTransportService,
        SearchPhaseController searchPhaseController,
        ClusterService clusterService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        NamedWriteableRegistry namedWriteableRegistry,
        ExecutorSelector executorSelector,
        SearchTransportAPMMetrics searchTransportMetrics,
        SearchResponseMetrics searchResponseMetrics
    ) {
        super(TYPE.name(), transportService, actionFilters, SearchRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.threadPool = threadPool;
        this.circuitBreaker = circuitBreakerService.getBreaker(CircuitBreaker.REQUEST);
        this.searchPhaseController = searchPhaseController;
        this.searchTransportService = searchTransportService;
        this.remoteClusterService = searchTransportService.getRemoteClusterService();
        SearchTransportService.registerRequestHandler(transportService, searchService, searchTransportMetrics);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.searchService = searchService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.executorSelector = executorSelector;
        this.defaultPreFilterShardSize = DEFAULT_PRE_FILTER_SHARD_SIZE.get(clusterService.getSettings());
        this.ccsCheckCompatibility = SearchService.CCS_VERSION_CHECK_SETTING.get(clusterService.getSettings());
        this.searchResponseMetrics = searchResponseMetrics;
    }

    private Map<String, OriginalIndices> buildPerIndexOriginalIndices(
        ClusterState clusterState,
        Set<String> indicesAndAliases,
        String[] indices,
        IndicesOptions indicesOptions
    ) {
        Map<String, OriginalIndices> res = new HashMap<>();
        for (String index : indices) {
            clusterState.blocks().indexBlockedRaiseException(ClusterBlockLevel.READ, index);

            String[] aliases = indexNameExpressionResolver.indexAliases(
                clusterState,
                index,
                Predicates.always(),
                Predicates.always(),
                true,
                indicesAndAliases
            );
            BooleanSupplier hasDataStreamRef = () -> {
                IndexAbstraction ret = clusterState.getMetadata().getIndicesLookup().get(index);
                if (ret == null || ret.getParentDataStream() == null) {
                    return false;
                }
                return indicesAndAliases.contains(ret.getParentDataStream().getName());
            };
            List<String> finalIndices = new ArrayList<>();
            if (aliases == null || aliases.length == 0 || indicesAndAliases.contains(index) || hasDataStreamRef.getAsBoolean()) {
                finalIndices.add(index);
            }
            if (aliases != null) {
                finalIndices.addAll(Arrays.asList(aliases));
            }
            res.put(index, new OriginalIndices(finalIndices.toArray(String[]::new), indicesOptions));
        }
        return Collections.unmodifiableMap(res);
    }

    Map<String, AliasFilter> buildIndexAliasFilters(ClusterState clusterState, Set<String> indicesAndAliases, Index[] concreteIndices) {
        final Map<String, AliasFilter> aliasFilterMap = new HashMap<>();
        for (Index index : concreteIndices) {
            clusterState.blocks().indexBlockedRaiseException(ClusterBlockLevel.READ, index.getName());
            AliasFilter aliasFilter = searchService.buildAliasFilter(clusterState, index.getName(), indicesAndAliases);
            assert aliasFilter != null;
            aliasFilterMap.put(index.getUUID(), aliasFilter);
        }
        return aliasFilterMap;
    }

    private Map<String, Float> resolveIndexBoosts(SearchRequest searchRequest, ClusterState clusterState) {
        if (searchRequest.source() == null) {
            return Collections.emptyMap();
        }

        SearchSourceBuilder source = searchRequest.source();
        if (source.indexBoosts() == null) {
            return Collections.emptyMap();
        }

        Map<String, Float> concreteIndexBoosts = new HashMap<>();
        for (SearchSourceBuilder.IndexBoost ib : source.indexBoosts()) {
            Index[] concreteIndices = indexNameExpressionResolver.concreteIndices(
                clusterState,
                searchRequest.indicesOptions(),
                ib.getIndex()
            );

            for (Index concreteIndex : concreteIndices) {
                concreteIndexBoosts.putIfAbsent(concreteIndex.getUUID(), ib.getBoost());
            }
        }
        return Collections.unmodifiableMap(concreteIndexBoosts);
    }

    /**
     * Search operations need two clocks. One clock is to fulfill real clock needs (e.g., resolving
     * "now" to an index name). Another clock is needed for measuring how long a search operation
     * took. These two uses are at odds with each other. There are many issues with using a real
     * clock for measuring how long an operation took (they often lack precision, they are subject
     * to moving backwards due to NTP and other such complexities, etc.). There are also issues with
     * using a relative clock for reporting real time. Thus, we simply separate these two uses.
     */
    public record SearchTimeProvider(long absoluteStartMillis, long relativeStartNanos, LongSupplier relativeCurrentNanosProvider) {

        /**
         * Instantiates a new search time provider. The absolute start time is the real clock time
         * used for resolving index expressions that include dates. The relative start time is the
         * start of the search operation according to a relative clock. The total time the search
         * operation took can be measured against the provided relative clock and the relative start
         * time.
         *
         * @param absoluteStartMillis          the absolute start time in milliseconds since the epoch
         * @param relativeStartNanos           the relative start time in nanoseconds
         * @param relativeCurrentNanosProvider provides the current relative time
         */
        public SearchTimeProvider {}

        public long buildTookInMillis() {
            return TimeUnit.NANOSECONDS.toMillis(relativeCurrentNanosProvider.getAsLong() - relativeStartNanos);
        }
    }

    @Override
    protected void doExecute(Task task, SearchRequest searchRequest, ActionListener<SearchResponse> listener) {
        ActionListener<SearchResponse> loggingAndMetrics = listener.delegateFailureAndWrap((l, searchResponse) -> {
            searchResponseMetrics.recordTookTime(searchResponse.getTookInMillis());
            if (searchResponse.getShardFailures() != null && searchResponse.getShardFailures().length > 0) {
                // Deduplicate failures by exception message and index
                ShardOperationFailedException[] groupedFailures = ExceptionsHelper.groupBy(searchResponse.getShardFailures());
                for (ShardOperationFailedException f : groupedFailures) {
                    boolean causeHas500Status = false;
                    if (f.getCause() != null) {
                        causeHas500Status = ExceptionsHelper.status(f.getCause()).getStatus() >= 500;
                    }
                    if ((f.status().getStatus() >= 500 || causeHas500Status)
                        && ExceptionsHelper.isNodeOrShardUnavailableTypeException(f.getCause()) == false) {
                        logger.warn("TransportSearchAction shard failure (partial results response)", f);
                    }
                }
            }
            l.onResponse(searchResponse);
        });
        executeRequest((SearchTask) task, searchRequest, loggingAndMetrics, AsyncSearchActionProvider::new);
    }

    void executeRequest(
        SearchTask task,
        SearchRequest original,
        ActionListener<SearchResponse> listener,
        Function<ActionListener<SearchResponse>, SearchPhaseProvider> searchPhaseProvider
    ) {
        final long relativeStartNanos = System.nanoTime();
        final SearchTimeProvider timeProvider = new SearchTimeProvider(
            original.getOrCreateAbsoluteStartMillis(),
            relativeStartNanos,
            System::nanoTime
        );
        ActionListener<SearchRequest> rewriteListener = listener.delegateFailureAndWrap((delegate, rewritten) -> {
            final SearchContextId searchContext;
            // key to map is clusterAlias
            final Map<String, OriginalIndices> remoteClusterIndices;
            if (ccsCheckCompatibility) {
                checkCCSVersionCompatibility(rewritten);
            }

            if (rewritten.pointInTimeBuilder() != null) {
                searchContext = rewritten.pointInTimeBuilder().getSearchContextId(namedWriteableRegistry);
                remoteClusterIndices = getIndicesFromSearchContexts(searchContext, rewritten.indicesOptions());
            } else {
                searchContext = null;
                remoteClusterIndices = remoteClusterService.groupIndices(rewritten.indicesOptions(), rewritten.indices());
            }
            OriginalIndices localIndices = remoteClusterIndices.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            final ClusterState clusterState = clusterService.state();
            if (remoteClusterIndices.isEmpty()) {
                executeLocalSearch(
                    task,
                    timeProvider,
                    rewritten,
                    localIndices,
                    clusterState,
                    SearchResponse.Clusters.EMPTY,
                    searchContext,
                    searchPhaseProvider.apply(delegate)
                );
            } else {
                final TaskId parentTaskId = task.taskInfo(clusterService.localNode().getId(), false).taskId();
                if (shouldMinimizeRoundtrips(rewritten)) {
                    final AggregationReduceContext.Builder aggregationReduceContextBuilder = rewritten.source() != null
                        && rewritten.source().aggregations() != null
                            ? searchService.aggReduceContextBuilder(task::isCancelled, rewritten.source().aggregations())
                            : null;
                    SearchResponse.Clusters clusters = new SearchResponse.Clusters(
                        localIndices,
                        remoteClusterIndices,
                        true,
                        remoteClusterService::isSkipUnavailable
                    );
                    if (localIndices == null) {
                        // Notify the progress listener that a CCS with minimize_roundtrips is happening remote-only (no local shards)
                        task.getProgressListener()
                            .notifyListShards(Collections.emptyList(), Collections.emptyList(), clusters, false, timeProvider);
                    }
                    ccsRemoteReduce(
                        task,
                        parentTaskId,
                        rewritten,
                        localIndices,
                        remoteClusterIndices,
                        clusters,
                        timeProvider,
                        aggregationReduceContextBuilder,
                        remoteClusterService,
                        threadPool,
                        delegate,
                        (r, l) -> executeLocalSearch(
                            task,
                            timeProvider,
                            r,
                            localIndices,
                            clusterState,
                            clusters,
                            searchContext,
                            searchPhaseProvider.apply(l)
                        )
                    );
                } else {
                    SearchResponse.Clusters clusters = new SearchResponse.Clusters(
                        localIndices,
                        remoteClusterIndices,
                        false,
                        remoteClusterService::isSkipUnavailable
                    );
                    // TODO: pass parentTaskId
                    collectSearchShards(
                        rewritten.indicesOptions(),
                        rewritten.preference(),
                        rewritten.routing(),
                        rewritten.source() != null ? rewritten.source().query() : null,
                        Objects.requireNonNullElse(rewritten.allowPartialSearchResults(), searchService.defaultAllowPartialSearchResults()),
                        searchContext,
                        remoteClusterIndices,
                        clusters,
                        timeProvider,
                        transportService,
                        delegate.delegateFailureAndWrap((finalDelegate, searchShardsResponses) -> {
                            final BiFunction<String, String, DiscoveryNode> clusterNodeLookup = getRemoteClusterNodeLookup(
                                searchShardsResponses
                            );
                            final Map<String, AliasFilter> remoteAliasFilters;
                            final List<SearchShardIterator> remoteShardIterators;
                            if (searchContext != null) {
                                remoteAliasFilters = searchContext.aliasFilter();
                                remoteShardIterators = getRemoteShardsIteratorFromPointInTime(
                                    searchShardsResponses,
                                    searchContext,
                                    rewritten.pointInTimeBuilder().getKeepAlive(),
                                    remoteClusterIndices
                                );
                            } else {
                                remoteAliasFilters = new HashMap<>();
                                for (SearchShardsResponse searchShardsResponse : searchShardsResponses.values()) {
                                    remoteAliasFilters.putAll(searchShardsResponse.getAliasFilters());
                                }
                                remoteShardIterators = getRemoteShardsIterator(
                                    searchShardsResponses,
                                    remoteClusterIndices,
                                    remoteAliasFilters
                                );
                            }
                            executeSearch(
                                task,
                                timeProvider,
                                rewritten,
                                localIndices,
                                remoteShardIterators,
                                clusterNodeLookup,
                                clusterState,
                                remoteAliasFilters,
                                clusters,
                                searchContext,
                                searchPhaseProvider.apply(finalDelegate)
                            );
                        })
                    );
                }
            }
        });
        Rewriteable.rewriteAndFetch(original, searchService.getRewriteContext(timeProvider::absoluteStartMillis), rewriteListener);
    }

    static void adjustSearchType(SearchRequest searchRequest, boolean singleShard) {
        // if there's a kNN search, always use DFS_QUERY_THEN_FETCH
        if (searchRequest.hasKnnSearch()) {
            searchRequest.searchType(DFS_QUERY_THEN_FETCH);
            return;
        }

        // if there's only suggest, disable request cache and always use QUERY_THEN_FETCH
        if (searchRequest.isSuggestOnly()) {
            searchRequest.requestCache(false);
            searchRequest.searchType(QUERY_THEN_FETCH);
            return;
        }

        // optimize search type for cases where there is only one shard group to search on
        if (singleShard) {
            // if we only have one group, then we always want Q_T_F, no need for DFS, and no need to do THEN since we hit one shard
            searchRequest.searchType(QUERY_THEN_FETCH);
        }
    }

    public static boolean shouldMinimizeRoundtrips(SearchRequest searchRequest) {
        if (searchRequest.isCcsMinimizeRoundtrips() == false) {
            return false;
        }
        if (searchRequest.scroll() != null) {
            return false;
        }
        if (searchRequest.pointInTimeBuilder() != null) {
            return false;
        }
        if (searchRequest.searchType() == DFS_QUERY_THEN_FETCH) {
            return false;
        }
        if (searchRequest.hasKnnSearch()) {
            return false;
        }
        SearchSourceBuilder source = searchRequest.source();
        return source == null
            || source.collapse() == null
            || source.collapse().getInnerHits() == null
            || source.collapse().getInnerHits().isEmpty();
    }

    /**
     * Handles ccs_minimize_roundtrips=true
     */
    static void ccsRemoteReduce(
        SearchTask task,
        TaskId parentTaskId,
        SearchRequest searchRequest,
        OriginalIndices localIndices,
        Map<String, OriginalIndices> remoteIndices,
        SearchResponse.Clusters clusters,
        SearchTimeProvider timeProvider,
        AggregationReduceContext.Builder aggReduceContextBuilder,
        RemoteClusterService remoteClusterService,
        ThreadPool threadPool,
        ActionListener<SearchResponse> listener,
        BiConsumer<SearchRequest, ActionListener<SearchResponse>> localSearchConsumer
    ) {
        final var remoteClientResponseExecutor = threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION);
        if (localIndices == null && remoteIndices.size() == 1) {
            // if we are searching against a single remote cluster, we simply forward the original search request to such cluster
            // and we directly perform final reduction in the remote cluster
            Map.Entry<String, OriginalIndices> entry = remoteIndices.entrySet().iterator().next();
            String clusterAlias = entry.getKey();
            boolean skipUnavailable = remoteClusterService.isSkipUnavailable(clusterAlias);
            OriginalIndices indices = entry.getValue();
            SearchRequest ccsSearchRequest = SearchRequest.subSearchRequest(
                parentTaskId,
                searchRequest,
                indices.indices(),
                clusterAlias,
                timeProvider.absoluteStartMillis(),
                true
            );
            var remoteClusterClient = remoteClusterService.getRemoteClusterClient(clusterAlias, remoteClientResponseExecutor);
            remoteClusterClient.execute(TransportSearchAction.REMOTE_TYPE, ccsSearchRequest, new ActionListener<>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    // overwrite the existing cluster entry with the updated one
                    ccsClusterInfoUpdate(searchResponse, clusters, clusterAlias, skipUnavailable);
                    Map<String, SearchProfileShardResult> profileResults = searchResponse.getProfileResults();
                    SearchProfileResults profile = profileResults == null || profileResults.isEmpty()
                        ? null
                        : new SearchProfileResults(profileResults);

                    ActionListener.respondAndRelease(
                        listener,
                        new SearchResponse(
                            searchResponse.getHits(),
                            searchResponse.getAggregations(),
                            searchResponse.getSuggest(),
                            searchResponse.isTimedOut(),
                            searchResponse.isTerminatedEarly(),
                            profile,
                            searchResponse.getNumReducePhases(),
                            searchResponse.getScrollId(),
                            searchResponse.getTotalShards(),
                            searchResponse.getSuccessfulShards(),
                            searchResponse.getSkippedShards(),
                            timeProvider.buildTookInMillis(),
                            searchResponse.getShardFailures(),
                            clusters,
                            searchResponse.pointInTimeId()
                        )
                    );
                }

                @Override
                public void onFailure(Exception e) {
                    ShardSearchFailure failure = new ShardSearchFailure(e);
                    logCCSError(failure, clusterAlias, skipUnavailable);
                    ccsClusterInfoUpdate(failure, clusters, clusterAlias, skipUnavailable);
                    if (skipUnavailable) {
                        ActionListener.respondAndRelease(listener, SearchResponse.empty(timeProvider::buildTookInMillis, clusters));
                    } else {
                        listener.onFailure(wrapRemoteClusterFailure(clusterAlias, e));
                    }
                }
            });
        } else {
            SearchResponseMerger searchResponseMerger = createSearchResponseMerger(
                searchRequest.source(),
                timeProvider,
                aggReduceContextBuilder
            );
            task.setSearchResponseMergerSupplier(
                () -> createSearchResponseMerger(searchRequest.source(), timeProvider, aggReduceContextBuilder)
            );
            final AtomicReference<Exception> exceptions = new AtomicReference<>();
            int totalClusters = remoteIndices.size() + (localIndices == null ? 0 : 1);
            final CountDown countDown = new CountDown(totalClusters);
            for (Map.Entry<String, OriginalIndices> entry : remoteIndices.entrySet()) {
                String clusterAlias = entry.getKey();
                boolean skipUnavailable = remoteClusterService.isSkipUnavailable(clusterAlias);
                OriginalIndices indices = entry.getValue();
                SearchRequest ccsSearchRequest = SearchRequest.subSearchRequest(
                    parentTaskId,
                    searchRequest,
                    indices.indices(),
                    clusterAlias,
                    timeProvider.absoluteStartMillis(),
                    false
                );
                ActionListener<SearchResponse> ccsListener = createCCSListener(
                    clusterAlias,
                    skipUnavailable,
                    countDown,
                    exceptions,
                    searchResponseMerger,
                    clusters,
                    task.getProgressListener(),
                    listener
                );
                final var remoteClusterClient = remoteClusterService.getRemoteClusterClient(clusterAlias, remoteClientResponseExecutor);
                remoteClusterClient.execute(TransportSearchAction.REMOTE_TYPE, ccsSearchRequest, ccsListener);
            }
            if (localIndices != null) {
                ActionListener<SearchResponse> ccsListener = createCCSListener(
                    RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
                    false,
                    countDown,
                    exceptions,
                    searchResponseMerger,
                    clusters,
                    task.getProgressListener(),
                    listener
                );
                SearchRequest ccsLocalSearchRequest = SearchRequest.subSearchRequest(
                    parentTaskId,
                    searchRequest,
                    localIndices.indices(),
                    RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
                    timeProvider.absoluteStartMillis(),
                    false
                );
                localSearchConsumer.accept(ccsLocalSearchRequest, ccsListener);
            }
        }
    }

    static SearchResponseMerger createSearchResponseMerger(
        SearchSourceBuilder source,
        SearchTimeProvider timeProvider,
        AggregationReduceContext.Builder aggReduceContextBuilder
    ) {
        final int from;
        final int size;
        final int trackTotalHitsUpTo;
        if (source == null) {
            from = SearchService.DEFAULT_FROM;
            size = SearchService.DEFAULT_SIZE;
            trackTotalHitsUpTo = SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO;
        } else {
            from = source.from() == -1 ? SearchService.DEFAULT_FROM : source.from();
            size = source.size() == -1 ? SearchService.DEFAULT_SIZE : source.size();
            trackTotalHitsUpTo = source.trackTotalHitsUpTo() == null
                ? SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO
                : source.trackTotalHitsUpTo();
            // here we modify the original source so we can re-use it by setting it to each outgoing search request
            source.from(0);
            source.size(from + size);
        }
        return new SearchResponseMerger(from, size, trackTotalHitsUpTo, timeProvider, aggReduceContextBuilder);
    }

    /**
     * Used for ccs_minimize_roundtrips=false
     */
    static void collectSearchShards(
        IndicesOptions indicesOptions,
        String preference,
        String routing,
        QueryBuilder query,
        boolean allowPartialResults,
        SearchContextId searchContext,
        Map<String, OriginalIndices> remoteIndicesByCluster,
        SearchResponse.Clusters clusters,
        SearchTimeProvider timeProvider,
        TransportService transportService,
        ActionListener<Map<String, SearchShardsResponse>> listener
    ) {
        RemoteClusterService remoteClusterService = transportService.getRemoteClusterService();
        final CountDown responsesCountDown = new CountDown(remoteIndicesByCluster.size());
        final Map<String, SearchShardsResponse> searchShardsResponses = new ConcurrentHashMap<>();
        final AtomicReference<Exception> exceptions = new AtomicReference<>();
        for (Map.Entry<String, OriginalIndices> entry : remoteIndicesByCluster.entrySet()) {
            final String clusterAlias = entry.getKey();
            boolean skipUnavailable = remoteClusterService.isSkipUnavailable(clusterAlias);
            TransportSearchAction.CCSActionListener<SearchShardsResponse, Map<String, SearchShardsResponse>> singleListener =
                new TransportSearchAction.CCSActionListener<>(
                    clusterAlias,
                    skipUnavailable,
                    responsesCountDown,
                    exceptions,
                    clusters,
                    listener
                ) {
                    @Override
                    void innerOnResponse(SearchShardsResponse searchShardsResponse) {
                        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.SEARCH_COORDINATION);
                        ccsClusterInfoUpdate(searchShardsResponse, clusters, clusterAlias, timeProvider);
                        searchShardsResponses.put(clusterAlias, searchShardsResponse);
                    }

                    @Override
                    Map<String, SearchShardsResponse> createFinalResponse() {
                        return searchShardsResponses;
                    }
                };
            remoteClusterService.maybeEnsureConnectedAndGetConnection(
                clusterAlias,
                skipUnavailable == false,
                singleListener.delegateFailureAndWrap((delegate, connection) -> {
                    final String[] indices = entry.getValue().indices();
                    final Executor responseExecutor = transportService.getThreadPool().executor(ThreadPool.Names.SEARCH_COORDINATION);
                    // TODO: support point-in-time
                    if (searchContext == null && connection.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
                        SearchShardsRequest searchShardsRequest = new SearchShardsRequest(
                            indices,
                            indicesOptions,
                            query,
                            routing,
                            preference,
                            allowPartialResults,
                            clusterAlias
                        );
                        transportService.sendRequest(
                            connection,
                            TransportSearchShardsAction.TYPE.name(),
                            searchShardsRequest,
                            TransportRequestOptions.EMPTY,
                            new ActionListenerResponseHandler<>(delegate, SearchShardsResponse::new, responseExecutor)
                        );
                    } else {
                        // does not do a can-match
                        ClusterSearchShardsRequest searchShardsRequest = new ClusterSearchShardsRequest(indices).indicesOptions(
                            indicesOptions
                        ).local(true).preference(preference).routing(routing);
                        transportService.sendRequest(
                            connection,
                            ClusterSearchShardsAction.NAME,
                            searchShardsRequest,
                            TransportRequestOptions.EMPTY,
                            new ActionListenerResponseHandler<>(
                                delegate.map(SearchShardsResponse::fromLegacyResponse),
                                ClusterSearchShardsResponse::new,
                                responseExecutor
                            )
                        );
                    }
                })
            );
        }
    }

    /**
     * Only used for ccs_minimize_roundtrips=true pathway
     */
    private static ActionListener<SearchResponse> createCCSListener(
        String clusterAlias,
        boolean skipUnavailable,
        CountDown countDown,
        AtomicReference<Exception> exceptions,
        SearchResponseMerger searchResponseMerger,
        SearchResponse.Clusters clusters,
        SearchProgressListener progressListener,
        ActionListener<SearchResponse> originalListener
    ) {
        return new CCSActionListener<>(
            clusterAlias,
            skipUnavailable,
            countDown,
            exceptions,
            clusters,
            ActionListener.releaseAfter(originalListener, searchResponseMerger)
        ) {
            @Override
            void innerOnResponse(SearchResponse searchResponse) {
                ccsClusterInfoUpdate(searchResponse, clusters, clusterAlias, skipUnavailable);
                searchResponseMerger.add(searchResponse);
                progressListener.notifyClusterResponseMinimizeRoundtrips(clusterAlias, searchResponse);
            }

            @Override
            SearchResponse createFinalResponse() {
                return searchResponseMerger.getMergedResponse(clusters);
            }

            @Override
            protected void releaseResponse(SearchResponse searchResponse) {
                searchResponse.decRef();
            }
        };
    }

    /**
     * Creates a new Cluster object using the {@link ShardSearchFailure} info and skip_unavailable
     * flag to set Status. Then it swaps it in the clusters CHM at key clusterAlias
     */
    static void ccsClusterInfoUpdate(
        ShardSearchFailure failure,
        SearchResponse.Clusters clusters,
        String clusterAlias,
        boolean skipUnavailable
    ) {
        clusters.swapCluster(clusterAlias, (k, v) -> {
            SearchResponse.Cluster.Status status;
            if (skipUnavailable) {
                status = SearchResponse.Cluster.Status.SKIPPED;
            } else {
                status = SearchResponse.Cluster.Status.FAILED;
            }
            return new SearchResponse.Cluster.Builder(v).setStatus(status)
                .setFailures(CollectionUtils.appendToCopy(v.getFailures(), failure))
                .build();
        });
    }

    /**
     * Helper method common to multiple ccs_minimize_roundtrips=true code paths.
     * Used to update a specific SearchResponse.Cluster object state based upon
     * the SearchResponse coming from the cluster coordinator the search was performed on.
     * @param searchResponse SearchResponse from cluster sub-search
     * @param clusters Clusters that the search was executed on
     * @param clusterAlias Alias of the cluster to be updated
     */
    private static void ccsClusterInfoUpdate(
        SearchResponse searchResponse,
        SearchResponse.Clusters clusters,
        String clusterAlias,
        boolean skipUnavailable
    ) {
        /*
         * Cluster Status logic:
         * 1) FAILED if total_shards > 0 && all shards failed && skip_unavailable=false
         * 2) SKIPPED if total_shards > 0 && all shards failed && skip_unavailable=true
         * 3) PARTIAL if it timed out
         * 4) PARTIAL if it at least one of the shards succeeded but not all
         * 5) SUCCESSFUL if no shards failed (and did not time out)
         */
        clusters.swapCluster(clusterAlias, (k, v) -> {
            SearchResponse.Cluster.Status status;
            int totalShards = searchResponse.getTotalShards();
            if (totalShards > 0 && searchResponse.getFailedShards() >= totalShards) {
                if (skipUnavailable) {
                    status = SearchResponse.Cluster.Status.SKIPPED;
                } else {
                    status = SearchResponse.Cluster.Status.FAILED;
                }
            } else if (searchResponse.isTimedOut()) {
                status = SearchResponse.Cluster.Status.PARTIAL;
            } else if (searchResponse.getFailedShards() > 0) {
                status = SearchResponse.Cluster.Status.PARTIAL;
            } else {
                status = SearchResponse.Cluster.Status.SUCCESSFUL;
            }
            return new SearchResponse.Cluster.Builder(v).setStatus(status)
                .setTotalShards(totalShards)
                .setSuccessfulShards(searchResponse.getSuccessfulShards())
                .setSkippedShards(searchResponse.getSkippedShards())
                .setFailedShards(searchResponse.getFailedShards())
                .setFailures(Arrays.asList(searchResponse.getShardFailures()))
                .setTook(searchResponse.getTook())
                .setTimedOut(searchResponse.isTimedOut())
                .build();
        });
    }

    /**
     * Edge case ---
     * Typically we don't need to update a Cluster object after the SearchShards API call, since the
     * skipped shards will be passed into SearchProgressListener.onListShards.
     * However, there is an edge case where the remote SearchShards API call returns no shards at all.
     * So in that case, nothing for this cluster will be passed to onListShards, so we need to update
     * the Cluster object to SUCCESSFUL status with shard counts of 0 and a filled in 'took' value.
     *
     * @param response from SearchShards API call to remote cluster
     * @param clusters Clusters that the search was executed on
     * @param clusterAlias Alias of the cluster to be updated
     * @param timeProvider search time provider (for setting took value)
     */
    private static void ccsClusterInfoUpdate(
        SearchShardsResponse response,
        SearchResponse.Clusters clusters,
        String clusterAlias,
        SearchTimeProvider timeProvider
    ) {
        if (response.getGroups().isEmpty()) {
            clusters.swapCluster(
                clusterAlias,
                (k, v) -> new SearchResponse.Cluster.Builder(v).setStatus(SearchResponse.Cluster.Status.SUCCESSFUL)
                    .setTotalShards(0)
                    .setSuccessfulShards(0)
                    .setSkippedShards(0)
                    .setFailedShards(0)
                    .setFailures(Collections.emptyList())
                    .setTook(new TimeValue(timeProvider.buildTookInMillis()))
                    .setTimedOut(false)
                    .build()
            );
        }
    }

    void executeLocalSearch(
        Task task,
        SearchTimeProvider timeProvider,
        SearchRequest searchRequest,
        OriginalIndices localIndices,
        ClusterState clusterState,
        SearchResponse.Clusters clusterInfo,
        SearchContextId searchContext,
        SearchPhaseProvider searchPhaseProvider
    ) {
        executeSearch(
            (SearchTask) task,
            timeProvider,
            searchRequest,
            localIndices,
            Collections.emptyList(),
            (clusterName, nodeId) -> null,
            clusterState,
            Collections.emptyMap(),
            clusterInfo,
            searchContext,
            searchPhaseProvider
        );
    }

    static BiFunction<String, String, DiscoveryNode> getRemoteClusterNodeLookup(Map<String, SearchShardsResponse> searchShardsResp) {
        Map<String, Map<String, DiscoveryNode>> clusterToNode = new HashMap<>();
        for (Map.Entry<String, SearchShardsResponse> entry : searchShardsResp.entrySet()) {
            String clusterAlias = entry.getKey();
            for (DiscoveryNode remoteNode : entry.getValue().getNodes()) {
                clusterToNode.computeIfAbsent(clusterAlias, k -> new HashMap<>()).put(remoteNode.getId(), remoteNode);
            }
        }
        return (clusterAlias, nodeId) -> {
            Map<String, DiscoveryNode> clusterNodes = clusterToNode.get(clusterAlias);
            if (clusterNodes == null) {
                throw new IllegalArgumentException("unknown remote cluster: " + clusterAlias);
            }
            return clusterNodes.get(nodeId);
        };
    }

    static List<SearchShardIterator> getRemoteShardsIterator(
        Map<String, SearchShardsResponse> searchShardsResponses,
        Map<String, OriginalIndices> remoteIndicesByCluster,
        Map<String, AliasFilter> aliasFilterMap
    ) {
        final List<SearchShardIterator> remoteShardIterators = new ArrayList<>();
        for (Map.Entry<String, SearchShardsResponse> entry : searchShardsResponses.entrySet()) {
            for (SearchShardsGroup searchShardsGroup : entry.getValue().getGroups()) {
                // add the cluster name to the remote index names for indices disambiguation
                // this ends up in the hits returned with the search response
                ShardId shardId = searchShardsGroup.shardId();
                AliasFilter aliasFilter = aliasFilterMap.get(shardId.getIndex().getUUID());
                String[] aliases = aliasFilter.getAliases();
                String clusterAlias = entry.getKey();
                String[] finalIndices = aliases.length == 0 ? new String[] { shardId.getIndexName() } : aliases;
                final OriginalIndices originalIndices = remoteIndicesByCluster.get(clusterAlias);
                assert originalIndices != null : "original indices are null for clusterAlias: " + clusterAlias;
                SearchShardIterator shardIterator = new SearchShardIterator(
                    clusterAlias,
                    shardId,
                    searchShardsGroup.allocatedNodes(),
                    new OriginalIndices(finalIndices, originalIndices.indicesOptions()),
                    null,
                    null,
                    searchShardsGroup.preFiltered(),
                    searchShardsGroup.skipped()
                );
                remoteShardIterators.add(shardIterator);
            }
        }
        return remoteShardIterators;
    }

    static List<SearchShardIterator> getRemoteShardsIteratorFromPointInTime(
        Map<String, SearchShardsResponse> searchShardsResponses,
        SearchContextId searchContextId,
        TimeValue searchContextKeepAlive,
        Map<String, OriginalIndices> remoteClusterIndices
    ) {
        final List<SearchShardIterator> remoteShardIterators = new ArrayList<>();
        for (Map.Entry<String, SearchShardsResponse> entry : searchShardsResponses.entrySet()) {
            for (SearchShardsGroup group : entry.getValue().getGroups()) {
                final ShardId shardId = group.shardId();
                final SearchContextIdForNode perNode = searchContextId.shards().get(shardId);
                if (perNode == null) {
                    // the shard was skipped after can match, hence it is not even part of the pit id
                    continue;
                }
                final String clusterAlias = entry.getKey();
                assert clusterAlias.equals(perNode.getClusterAlias()) : clusterAlias + " != " + perNode.getClusterAlias();
                final List<String> targetNodes = new ArrayList<>(group.allocatedNodes().size());
                targetNodes.add(perNode.getNode());
                if (perNode.getSearchContextId().getSearcherId() != null) {
                    for (String node : group.allocatedNodes()) {
                        if (node.equals(perNode.getNode()) == false) {
                            targetNodes.add(node);
                        }
                    }
                }
                assert remoteClusterIndices.get(clusterAlias) != null : "original indices are null for clusterAlias: " + clusterAlias;
                final OriginalIndices finalIndices = new OriginalIndices(
                    new String[] { shardId.getIndexName() },
                    remoteClusterIndices.get(clusterAlias).indicesOptions()
                );
                SearchShardIterator shardIterator = new SearchShardIterator(
                    clusterAlias,
                    shardId,
                    targetNodes,
                    finalIndices,
                    perNode.getSearchContextId(),
                    searchContextKeepAlive,
                    false,
                    false
                );
                remoteShardIterators.add(shardIterator);
            }
        }
        assert checkAllRemotePITShardsWereReturnedBySearchShards(searchContextId.shards(), searchShardsResponses)
            : "search shards did not return remote shards that PIT included: " + searchContextId.shards();
        return remoteShardIterators;
    }

    private static boolean checkAllRemotePITShardsWereReturnedBySearchShards(
        Map<ShardId, SearchContextIdForNode> searchContextIdShards,
        Map<String, SearchShardsResponse> searchShardsResponses
    ) {
        Map<ShardId, SearchContextIdForNode> searchContextIdForNodeMap = new HashMap<>(searchContextIdShards);
        for (SearchShardsResponse searchShardsResponse : searchShardsResponses.values()) {
            for (SearchShardsGroup group : searchShardsResponse.getGroups()) {
                searchContextIdForNodeMap.remove(group.shardId());
            }
        }
        return searchContextIdForNodeMap.values()
            .stream()
            .allMatch(searchContextIdForNode -> searchContextIdForNode.getClusterAlias() == null);
    }

    Index[] resolveLocalIndices(OriginalIndices localIndices, ClusterState clusterState, SearchTimeProvider timeProvider) {
        if (localIndices == null) {
            return Index.EMPTY_ARRAY; // don't search on any local index (happens when only remote indices were specified)
        }

        List<String> frozenIndices = null;
        Index[] indices = indexNameExpressionResolver.concreteIndices(clusterState, localIndices, timeProvider.absoluteStartMillis());
        for (Index index : indices) {
            IndexMetadata indexMetadata = clusterState.metadata().index(index);
            if (indexMetadata.getSettings().getAsBoolean("index.frozen", false)) {
                if (frozenIndices == null) {
                    frozenIndices = new ArrayList<>();
                }
                frozenIndices.add(index.getName());
            }
        }
        if (frozenIndices != null) {
            DEPRECATION_LOGGER.warn(
                DeprecationCategory.INDICES,
                "search-frozen-indices",
                FROZEN_INDICES_DEPRECATION_MESSAGE,
                String.join(",", frozenIndices)
            );
        }
        return indices;
    }

    private void executeSearch(
        SearchTask task,
        SearchTimeProvider timeProvider,
        SearchRequest searchRequest,
        OriginalIndices localIndices,
        List<SearchShardIterator> remoteShardIterators,
        BiFunction<String, String, DiscoveryNode> remoteConnections,
        ClusterState clusterState,
        Map<String, AliasFilter> remoteAliasMap,
        SearchResponse.Clusters clusters,
        @Nullable SearchContextId searchContext,
        SearchPhaseProvider searchPhaseProvider
    ) {
        clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);
        if (searchRequest.allowPartialSearchResults() == null) {
            // No user preference defined in search request - apply cluster service default
            searchRequest.allowPartialSearchResults(searchService.defaultAllowPartialSearchResults());
        }

        // TODO: I think startTime() should become part of ActionRequest and that should be used both for index name
        // date math expressions and $now in scripts. This way all apis will deal with now in the same way instead
        // of just for the _search api
        final List<SearchShardIterator> localShardIterators;
        final Map<String, AliasFilter> aliasFilter;

        final String[] concreteLocalIndices;
        if (searchContext != null) {
            assert searchRequest.pointInTimeBuilder() != null;
            aliasFilter = searchContext.aliasFilter();
            concreteLocalIndices = localIndices == null ? new String[0] : localIndices.indices();
            localShardIterators = getLocalLocalShardsIteratorFromPointInTime(
                clusterState,
                localIndices,
                searchRequest.getLocalClusterAlias(),
                searchContext,
                searchRequest.pointInTimeBuilder().getKeepAlive(),
                searchRequest.allowPartialSearchResults()
            );
        } else {
            final Index[] indices = resolveLocalIndices(localIndices, clusterState, timeProvider);
            concreteLocalIndices = Arrays.stream(indices).map(Index::getName).toArray(String[]::new);
            final Set<String> indicesAndAliases = indexNameExpressionResolver.resolveExpressions(clusterState, searchRequest.indices());
            aliasFilter = buildIndexAliasFilters(clusterState, indicesAndAliases, indices);
            aliasFilter.putAll(remoteAliasMap);
            localShardIterators = getLocalShardsIterator(
                clusterState,
                searchRequest,
                searchRequest.getLocalClusterAlias(),
                indicesAndAliases,
                concreteLocalIndices
            );
        }
        final GroupShardsIterator<SearchShardIterator> shardIterators = mergeShardsIterators(localShardIterators, remoteShardIterators);

        failIfOverShardCountLimit(clusterService, shardIterators.size());

        if (searchRequest.getWaitForCheckpoints().isEmpty() == false) {
            if (remoteShardIterators.isEmpty() == false) {
                throw new IllegalArgumentException("Cannot use wait_for_checkpoints parameter with cross-cluster searches.");
            } else {
                validateAndResolveWaitForCheckpoint(clusterState, indexNameExpressionResolver, searchRequest, concreteLocalIndices);
            }
        }

        Map<String, Float> concreteIndexBoosts = resolveIndexBoosts(searchRequest, clusterState);

        adjustSearchType(searchRequest, shardIterators.size() == 1);

        final DiscoveryNodes nodes = clusterState.nodes();
        BiFunction<String, String, Transport.Connection> connectionLookup = buildConnectionLookup(
            searchRequest.getLocalClusterAlias(),
            nodes::get,
            remoteConnections,
            searchTransportService::getConnection
        );
        final Executor asyncSearchExecutor = asyncSearchExecutor(concreteLocalIndices);
        final boolean preFilterSearchShards = shouldPreFilterSearchShards(
            clusterState,
            searchRequest,
            concreteLocalIndices,
            localShardIterators.size() + remoteShardIterators.size(),
            defaultPreFilterShardSize
        );
        searchPhaseProvider.newSearchPhase(
            task,
            searchRequest,
            asyncSearchExecutor,
            shardIterators,
            timeProvider,
            connectionLookup,
            clusterState,
            Collections.unmodifiableMap(aliasFilter),
            concreteIndexBoosts,
            preFilterSearchShards,
            threadPool,
            clusters
        ).start();
    }

    Executor asyncSearchExecutor(final String[] indices) {
        final List<String> executorsForIndices = Arrays.stream(indices).map(executorSelector::executorForSearch).toList();
        if (executorsForIndices.size() == 1) { // all indices have same executor
            return threadPool.executor(executorsForIndices.get(0));
        }
        if (executorsForIndices.size() == 2
            && executorsForIndices.contains(SYSTEM_READ)
            && executorsForIndices.contains(SYSTEM_CRITICAL_READ)) { // mix of critical and non critical system indices
            return threadPool.executor(SYSTEM_READ);
        }
        return threadPool.executor(ThreadPool.Names.SEARCH);
    }

    static BiFunction<String, String, Transport.Connection> buildConnectionLookup(
        String requestClusterAlias,
        Function<String, DiscoveryNode> localNodes,
        BiFunction<String, String, DiscoveryNode> remoteNodes,
        BiFunction<String, DiscoveryNode, Transport.Connection> nodeToConnection
    ) {
        return (clusterAlias, nodeId) -> {
            final DiscoveryNode discoveryNode;
            final boolean remoteCluster;
            if (clusterAlias == null || requestClusterAlias != null) {
                assert requestClusterAlias == null || requestClusterAlias.equals(clusterAlias);
                discoveryNode = localNodes.apply(nodeId);
                remoteCluster = false;
            } else {
                discoveryNode = remoteNodes.apply(clusterAlias, nodeId);
                remoteCluster = true;
            }
            if (discoveryNode == null) {
                throw new IllegalStateException("no node found for id: " + nodeId);
            }
            return nodeToConnection.apply(remoteCluster ? clusterAlias : null, discoveryNode);
        };
    }

    static boolean shouldPreFilterSearchShards(
        ClusterState clusterState,
        SearchRequest searchRequest,
        String[] indices,
        int numShards,
        int defaultPreFilterShardSize
    ) {
        SearchSourceBuilder source = searchRequest.source();
        Integer preFilterShardSize = searchRequest.getPreFilterShardSize();
        if (preFilterShardSize == null && (hasReadOnlyIndices(indices, clusterState) || hasPrimaryFieldSort(source))) {
            preFilterShardSize = 1;
        } else if (preFilterShardSize == null) {
            preFilterShardSize = defaultPreFilterShardSize;
        }
        return searchRequest.searchType() == QUERY_THEN_FETCH // we can't do this for DFS it needs to fan out to all shards all the time
            && (SearchService.canRewriteToMatchNone(source) || hasPrimaryFieldSort(source))
            && preFilterShardSize < numShards;
    }

    private static boolean hasReadOnlyIndices(String[] indices, ClusterState clusterState) {
        for (String index : indices) {
            ClusterBlockException writeBlock = clusterState.blocks().indexBlockedException(ClusterBlockLevel.WRITE, index);
            if (writeBlock != null) {
                return true;
            }
        }
        return false;
    }

    static GroupShardsIterator<SearchShardIterator> mergeShardsIterators(
        List<SearchShardIterator> localShardIterators,
        List<SearchShardIterator> remoteShardIterators
    ) {
        List<SearchShardIterator> shards = new ArrayList<>(remoteShardIterators);
        shards.addAll(localShardIterators);
        return GroupShardsIterator.sortAndCreate(shards);
    }

    interface SearchPhaseProvider {
        SearchPhase newSearchPhase(
            SearchTask task,
            SearchRequest searchRequest,
            Executor executor,
            GroupShardsIterator<SearchShardIterator> shardIterators,
            SearchTimeProvider timeProvider,
            BiFunction<String, String, Transport.Connection> connectionLookup,
            ClusterState clusterState,
            Map<String, AliasFilter> aliasFilter,
            Map<String, Float> concreteIndexBoosts,
            boolean preFilter,
            ThreadPool threadPool,
            SearchResponse.Clusters clusters
        );
    }

    private class AsyncSearchActionProvider implements SearchPhaseProvider {
        private final ActionListener<SearchResponse> listener;

        AsyncSearchActionProvider(ActionListener<SearchResponse> listener) {
            this.listener = listener;
        }

        @Override
        public SearchPhase newSearchPhase(
            SearchTask task,
            SearchRequest searchRequest,
            Executor executor,
            GroupShardsIterator<SearchShardIterator> shardIterators,
            SearchTimeProvider timeProvider,
            BiFunction<String, String, Transport.Connection> connectionLookup,
            ClusterState clusterState,
            Map<String, AliasFilter> aliasFilter,
            Map<String, Float> concreteIndexBoosts,
            boolean preFilter,
            ThreadPool threadPool,
            SearchResponse.Clusters clusters
        ) {
            if (preFilter) {
                return new CanMatchPreFilterSearchPhase(
                    logger,
                    searchTransportService,
                    connectionLookup,
                    aliasFilter,
                    concreteIndexBoosts,
                    threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION),
                    searchRequest,
                    shardIterators,
                    timeProvider,
                    task,
                    true,
                    searchService.getCoordinatorRewriteContextProvider(timeProvider::absoluteStartMillis),
                    listener.delegateFailureAndWrap((l, iters) -> {
                        SearchPhase action = newSearchPhase(
                            task,
                            searchRequest,
                            executor,
                            iters,
                            timeProvider,
                            connectionLookup,
                            clusterState,
                            aliasFilter,
                            concreteIndexBoosts,
                            false,
                            threadPool,
                            clusters
                        );
                        action.start();
                    })
                );
            } else {
                // for synchronous CCS minimize_roundtrips=false, use the CCSSingleCoordinatorSearchProgressListener
                // (AsyncSearchTask will not return SearchProgressListener.NOOP, since it uses its own progress listener
                // which delegates to CCSSingleCoordinatorSearchProgressListener when minimizing roundtrips)
                if (clusters.isCcsMinimizeRoundtrips() == false
                    && clusters.hasRemoteClusters()
                    && task.getProgressListener() == SearchProgressListener.NOOP) {
                    task.setProgressListener(new CCSSingleCoordinatorSearchProgressListener());
                }
                final SearchPhaseResults<SearchPhaseResult> queryResultConsumer = searchPhaseController.newSearchPhaseResults(
                    executor,
                    circuitBreaker,
                    task::isCancelled,
                    task.getProgressListener(),
                    searchRequest,
                    shardIterators.size(),
                    exc -> searchTransportService.cancelSearchTask(task, "failed to merge result [" + exc.getMessage() + "]")
                );
                if (searchRequest.searchType() == DFS_QUERY_THEN_FETCH) {
                    return new SearchDfsQueryThenFetchAsyncAction(
                        logger,
                        namedWriteableRegistry,
                        searchTransportService,
                        connectionLookup,
                        aliasFilter,
                        concreteIndexBoosts,
                        executor,
                        queryResultConsumer,
                        searchRequest,
                        listener,
                        shardIterators,
                        timeProvider,
                        clusterState,
                        task,
                        clusters
                    );
                } else {
                    assert searchRequest.searchType() == QUERY_THEN_FETCH : searchRequest.searchType();
                    return new SearchQueryThenFetchAsyncAction(
                        logger,
                        namedWriteableRegistry,
                        searchTransportService,
                        connectionLookup,
                        aliasFilter,
                        concreteIndexBoosts,
                        executor,
                        queryResultConsumer,
                        searchRequest,
                        listener,
                        shardIterators,
                        timeProvider,
                        clusterState,
                        task,
                        clusters
                    );
                }
            }
        }
    }

    private static void validateAndResolveWaitForCheckpoint(
        ClusterState clusterState,
        IndexNameExpressionResolver resolver,
        SearchRequest searchRequest,
        String[] concreteLocalIndices
    ) {
        HashSet<String> searchedIndices = new HashSet<>(Arrays.asList(concreteLocalIndices));
        Map<String, long[]> newWaitForCheckpoints = Maps.newMapWithExpectedSize(searchRequest.getWaitForCheckpoints().size());
        for (Map.Entry<String, long[]> waitForCheckpointIndex : searchRequest.getWaitForCheckpoints().entrySet()) {
            long[] checkpoints = waitForCheckpointIndex.getValue();
            int checkpointsProvided = checkpoints.length;
            String target = waitForCheckpointIndex.getKey();
            Index resolved;
            try {
                resolved = resolver.concreteSingleIndex(clusterState, new IndicesRequest() {
                    @Override
                    public String[] indices() {
                        return new String[] { target };
                    }

                    @Override
                    public IndicesOptions indicesOptions() {
                        return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
                    }
                });
            } catch (Exception e) {
                throw new IllegalArgumentException(
                    "Failed to resolve wait_for_checkpoints target ["
                        + target
                        + "]. Configured target "
                        + "must resolve to a single open index.",
                    e
                );
            }
            String index = resolved.getName();
            IndexMetadata indexMetadata = clusterState.metadata().index(index);
            if (searchedIndices.contains(index) == false) {
                throw new IllegalArgumentException(
                    "Target configured with wait_for_checkpoints must be a concrete index resolved in "
                        + "this search. Target ["
                        + target
                        + "] is not a concrete index resolved in this search."
                );
            } else if (indexMetadata == null) {
                throw new IllegalArgumentException("Cannot find index configured for wait_for_checkpoints parameter [" + index + "].");
            } else if (indexMetadata.getNumberOfShards() != checkpointsProvided) {
                throw new IllegalArgumentException(
                    "Target configured with wait_for_checkpoints must search the same number of shards as "
                        + "checkpoints provided. ["
                        + checkpointsProvided
                        + "] checkpoints provided. Target ["
                        + target
                        + "] which resolved to "
                        + "index ["
                        + index
                        + "] has "
                        + "["
                        + indexMetadata.getNumberOfShards()
                        + "] shards."
                );
            }
            newWaitForCheckpoints.put(index, checkpoints);
        }
        searchRequest.setWaitForCheckpoints(Collections.unmodifiableMap(newWaitForCheckpoints));
    }

    private static void failIfOverShardCountLimit(ClusterService clusterService, int shardCount) {
        final long shardCountLimit = clusterService.getClusterSettings().get(SHARD_COUNT_LIMIT_SETTING);
        if (shardCount > shardCountLimit) {
            throw new IllegalArgumentException(
                "Trying to query "
                    + shardCount
                    + " shards, which is over the limit of "
                    + shardCountLimit
                    + ". This limit exists because querying many shards at the same time can make the "
                    + "job of the coordinating node very CPU and/or memory intensive. It is usually a better idea to "
                    + "have a smaller number of larger shards. Update ["
                    + SHARD_COUNT_LIMIT_SETTING.getKey()
                    + "] to a greater value if you really want to query that many shards at the same time."
            );
        }
    }

    abstract static class CCSActionListener<Response, FinalResponse> implements ActionListener<Response> {
        protected final String clusterAlias;
        protected final boolean skipUnavailable;
        private final CountDown countDown;
        private final AtomicReference<Exception> exceptions;
        protected final SearchResponse.Clusters clusters;
        private final ActionListener<FinalResponse> originalListener;

        /**
         * Used by both minimize_roundtrips true and false
         */
        CCSActionListener(
            String clusterAlias,
            boolean skipUnavailable,
            CountDown countDown,
            AtomicReference<Exception> exceptions,
            SearchResponse.Clusters clusters,
            ActionListener<FinalResponse> originalListener
        ) {
            this.clusterAlias = clusterAlias;
            this.skipUnavailable = skipUnavailable;
            this.countDown = countDown;
            this.exceptions = exceptions;
            this.clusters = clusters;
            this.originalListener = originalListener;
        }

        @Override
        public final void onResponse(Response response) {
            innerOnResponse(response);
            maybeFinish();
        }

        abstract void innerOnResponse(Response response);

        @Override
        public final void onFailure(Exception e) {
            ShardSearchFailure f = new ShardSearchFailure(e);
            logCCSError(f, clusterAlias, skipUnavailable);
            SearchResponse.Cluster cluster = clusters.getCluster(clusterAlias);
            if (skipUnavailable) {
                if (cluster != null) {
                    ccsClusterInfoUpdate(f, clusters, clusterAlias, true);
                }
            } else {
                if (cluster != null) {
                    ccsClusterInfoUpdate(f, clusters, clusterAlias, false);
                }
                Exception exception = e;
                if (RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY.equals(clusterAlias) == false) {
                    exception = wrapRemoteClusterFailure(clusterAlias, e);
                }
                if (exceptions.compareAndSet(null, exception) == false) {
                    exceptions.accumulateAndGet(exception, (previous, current) -> {
                        current.addSuppressed(previous);
                        return current;
                    });
                }
            }
            maybeFinish();
        }

        private void maybeFinish() {
            if (countDown.countDown()) {
                Exception exception = exceptions.get();
                if (exception == null) {
                    FinalResponse response;
                    try {
                        response = createFinalResponse();
                    } catch (Exception e) {
                        originalListener.onFailure(e);
                        return;
                    }
                    try {
                        originalListener.onResponse(response);
                    } finally {
                        releaseResponse(response);
                    }
                } else {
                    originalListener.onFailure(exceptions.get());
                }
            }
        }

        protected void releaseResponse(FinalResponse response) {}

        abstract FinalResponse createFinalResponse();
    }

    /**
     * In order to gather data on what types of CCS errors happen in the field, we will log
     * them using the ShardSearchFailure XContent (JSON), which supplies information about underlying
     * causes of shard failures.
     * @param f ShardSearchFailure to log
     * @param clusterAlias cluster on which the failure occurred
     * @param skipUnavailable the skip_unavailable setting of the cluster with the search error
     */
    private static void logCCSError(ShardSearchFailure f, String clusterAlias, boolean skipUnavailable) {
        String errorInfo;
        try {
            errorInfo = Strings.toString(f.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS));
        } catch (IOException ex) {
            // use the toString as a fallback if for some reason the XContent conversion to JSON fails
            errorInfo = f.toString();
        }
        logger.debug(
            "CCS remote cluster failure. Cluster [{}]. skip_unavailable: [{}]. Error: {}",
            clusterAlias,
            skipUnavailable,
            errorInfo
        );
    }

    private static RemoteTransportException wrapRemoteClusterFailure(String clusterAlias, Exception e) {
        return new RemoteTransportException("error while communicating with remote cluster [" + clusterAlias + "]", e);
    }

    static Map<String, OriginalIndices> getIndicesFromSearchContexts(SearchContextId searchContext, IndicesOptions indicesOptions) {
        final Map<String, Set<String>> indices = new HashMap<>();
        for (Map.Entry<ShardId, SearchContextIdForNode> entry : searchContext.shards().entrySet()) {
            String clusterAlias = entry.getValue().getClusterAlias() == null
                ? RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY
                : entry.getValue().getClusterAlias();
            indices.computeIfAbsent(clusterAlias, k -> new HashSet<>()).add(entry.getKey().getIndexName());
        }
        return indices.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> new OriginalIndices(e.getValue().toArray(String[]::new), indicesOptions)));
    }

    static List<SearchShardIterator> getLocalLocalShardsIteratorFromPointInTime(
        ClusterState clusterState,
        OriginalIndices originalIndices,
        String localClusterAlias,
        SearchContextId searchContext,
        TimeValue keepAlive,
        boolean allowPartialSearchResults
    ) {
        final List<SearchShardIterator> iterators = new ArrayList<>(searchContext.shards().size());
        for (Map.Entry<ShardId, SearchContextIdForNode> entry : searchContext.shards().entrySet()) {
            final SearchContextIdForNode perNode = entry.getValue();
            if (Strings.isEmpty(perNode.getClusterAlias())) {
                final ShardId shardId = entry.getKey();
                final List<String> targetNodes = new ArrayList<>(2);
                try {
                    final ShardIterator shards = OperationRouting.getShards(clusterState, shardId);
                    // Prefer executing shard requests on nodes that are part of PIT first.
                    if (clusterState.nodes().nodeExists(perNode.getNode())) {
                        targetNodes.add(perNode.getNode());
                    }
                    if (perNode.getSearchContextId().getSearcherId() != null) {
                        for (ShardRouting shard : shards) {
                            if (shard.currentNodeId().equals(perNode.getNode()) == false) {
                                targetNodes.add(shard.currentNodeId());
                            }
                        }
                    }
                } catch (IndexNotFoundException | ShardNotFoundException e) {
                    // We can hit these exceptions if the index was deleted after creating PIT or the cluster state on
                    // this coordinating node is outdated. It's fine to ignore these extra "retry-able" target shards
                    // when allowPartialSearchResults is false
                    if (allowPartialSearchResults == false) {
                        throw e;
                    }
                }
                OriginalIndices finalIndices = new OriginalIndices(
                    new String[] { shardId.getIndexName() },
                    originalIndices.indicesOptions()
                );
                iterators.add(
                    new SearchShardIterator(
                        localClusterAlias,
                        shardId,
                        targetNodes,
                        finalIndices,
                        perNode.getSearchContextId(),
                        keepAlive,
                        false,
                        false
                    )
                );
            }
        }
        return iterators;
    }

    List<SearchShardIterator> getLocalShardsIterator(
        ClusterState clusterState,
        SearchRequest searchRequest,
        String clusterAlias,
        Set<String> indicesAndAliases,
        String[] concreteIndices
    ) {
        var routingMap = indexNameExpressionResolver.resolveSearchRouting(clusterState, searchRequest.routing(), searchRequest.indices());
        GroupShardsIterator<ShardIterator> shardRoutings = clusterService.operationRouting()
            .searchShards(
                clusterState,
                concreteIndices,
                routingMap,
                searchRequest.preference(),
                searchService.getResponseCollectorService(),
                searchTransportService.getPendingSearchRequests()
            );
        final Map<String, OriginalIndices> originalIndices = buildPerIndexOriginalIndices(
            clusterState,
            indicesAndAliases,
            concreteIndices,
            searchRequest.indicesOptions()
        );
        return StreamSupport.stream(shardRoutings.spliterator(), false).map(it -> {
            OriginalIndices finalIndices = originalIndices.get(it.shardId().getIndex().getName());
            assert finalIndices != null;
            return new SearchShardIterator(clusterAlias, it.shardId(), it.getShardRoutings(), finalIndices);
        }).toList();
    }
}
