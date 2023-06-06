/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsAction;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.ExecutorSelector;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.InternalSearchResponse;
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
import java.util.concurrent.atomic.AtomicInteger;
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
        ExecutorSelector executorSelector
    ) {
        super(SearchAction.NAME, transportService, actionFilters, (Writeable.Reader<SearchRequest>) SearchRequest::new);
        this.threadPool = threadPool;
        this.circuitBreaker = circuitBreakerService.getBreaker(CircuitBreaker.REQUEST);
        this.searchPhaseController = searchPhaseController;
        this.searchTransportService = searchTransportService;
        this.remoteClusterService = searchTransportService.getRemoteClusterService();
        SearchTransportService.registerRequestHandler(transportService, searchService);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.searchService = searchService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.executorSelector = executorSelector;
        this.defaultPreFilterShardSize = DEFAULT_PRE_FILTER_SHARD_SIZE.get(clusterService.getSettings());
        this.ccsCheckCompatibility = SearchService.CCS_VERSION_CHECK_SETTING.get(clusterService.getSettings());
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
                aliasMetadata -> true,
                dataStreamAlias -> true,
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
    record SearchTimeProvider(long absoluteStartMillis, long relativeStartNanos, LongSupplier relativeCurrentNanosProvider) {

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
        SearchTimeProvider {}

        long buildTookInMillis() {
            return TimeUnit.NANOSECONDS.toMillis(relativeCurrentNanosProvider.getAsLong() - relativeStartNanos);
        }
    }

    @Override
    protected void doExecute(Task task, SearchRequest searchRequest, ActionListener<SearchResponse> listener) {
        executeRequest((SearchTask) task, searchRequest, listener, AsyncSearchActionProvider::new);
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
        ActionListener<SearchRequest> rewriteListener = ActionListener.wrap(rewritten -> {
            final SearchContextId searchContext;
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
                    searchPhaseProvider.apply(listener)
                );
            } else {
                final TaskId parentTaskId = task.taskInfo(clusterService.localNode().getId(), false).taskId();
                if (shouldMinimizeRoundtrips(rewritten)) {
                    final AggregationReduceContext.Builder aggregationReduceContextBuilder = rewritten.source() != null
                        && rewritten.source().aggregations() != null
                            ? searchService.aggReduceContextBuilder(task::isCancelled, rewritten.source().aggregations())
                            : null;
                    final int totalClusters = (localIndices == null ? 0 : 1) + remoteClusterIndices.size();
                    var initClusters = new SearchResponse.Clusters(totalClusters, 0, 0, remoteClusterIndices.size(), true);
                    if (localIndices == null) {
                        // Notify the progress listener that a CCS with minimize_roundtrips is happening remote-only (no local shards)
                        task.getProgressListener().notifyListShards(Collections.emptyList(), Collections.emptyList(), initClusters, false);
                    }
                    ccsRemoteReduce(
                        parentTaskId,
                        rewritten,
                        localIndices,
                        remoteClusterIndices,
                        timeProvider,
                        aggregationReduceContextBuilder,
                        remoteClusterService,
                        threadPool,
                        listener,
                        (r, l) -> executeLocalSearch(
                            task,
                            timeProvider,
                            r,
                            localIndices,
                            clusterState,
                            initClusters,
                            searchContext,
                            searchPhaseProvider.apply(l)
                        )
                    );
                } else {
                    AtomicInteger skippedClusters = new AtomicInteger(0);
                    // TODO: pass parentTaskId
                    collectSearchShards(
                        rewritten.indicesOptions(),
                        rewritten.preference(),
                        rewritten.routing(),
                        rewritten.source() != null ? rewritten.source().query() : null,
                        Objects.requireNonNullElse(rewritten.allowPartialSearchResults(), searchService.defaultAllowPartialSearchResults()),
                        searchContext,
                        skippedClusters,
                        remoteClusterIndices,
                        transportService,
                        ActionListener.wrap(searchShardsResponses -> {
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
                            int localClusters = localIndices == null ? 0 : 1;
                            int totalClusters = remoteClusterIndices.size() + localClusters;
                            int successfulClusters = searchShardsResponses.size() + localClusters;
                            executeSearch(
                                task,
                                timeProvider,
                                rewritten,
                                localIndices,
                                remoteShardIterators,
                                clusterNodeLookup,
                                clusterState,
                                remoteAliasFilters,
                                new SearchResponse.Clusters(totalClusters, successfulClusters, skippedClusters.get()),
                                searchContext,
                                searchPhaseProvider.apply(listener)
                            );
                        }, listener::onFailure)
                    );
                }
            }
        }, listener::onFailure);
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

    static boolean shouldMinimizeRoundtrips(SearchRequest searchRequest) {
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

    static void ccsRemoteReduce(
        TaskId parentTaskId,
        SearchRequest searchRequest,
        OriginalIndices localIndices,
        Map<String, OriginalIndices> remoteIndices,
        SearchTimeProvider timeProvider,
        AggregationReduceContext.Builder aggReduceContextBuilder,
        RemoteClusterService remoteClusterService,
        ThreadPool threadPool,
        ActionListener<SearchResponse> listener,
        BiConsumer<SearchRequest, ActionListener<SearchResponse>> localSearchConsumer
    ) {
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
            Client remoteClusterClient = remoteClusterService.getRemoteClusterClient(threadPool, clusterAlias);
            remoteClusterClient.search(ccsSearchRequest, new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    Map<String, SearchProfileShardResult> profileResults = searchResponse.getProfileResults();
                    SearchProfileResults profile = profileResults == null || profileResults.isEmpty()
                        ? null
                        : new SearchProfileResults(profileResults);
                    InternalSearchResponse internalSearchResponse = new InternalSearchResponse(
                        searchResponse.getHits(),
                        (InternalAggregations) searchResponse.getAggregations(),
                        searchResponse.getSuggest(),
                        profile,
                        searchResponse.isTimedOut(),
                        searchResponse.isTerminatedEarly(),
                        searchResponse.getNumReducePhases()
                    );
                    listener.onResponse(
                        new SearchResponse(
                            internalSearchResponse,
                            searchResponse.getScrollId(),
                            searchResponse.getTotalShards(),
                            searchResponse.getSuccessfulShards(),
                            searchResponse.getSkippedShards(),
                            timeProvider.buildTookInMillis(),
                            searchResponse.getShardFailures(),
                            new SearchResponse.Clusters(1, 1, 0),
                            searchResponse.pointInTimeId()
                        )
                    );
                }

                @Override
                public void onFailure(Exception e) {
                    if (skipUnavailable) {
                        listener.onResponse(SearchResponse.empty(timeProvider::buildTookInMillis, new SearchResponse.Clusters(1, 0, 1)));
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
            AtomicInteger skippedClusters = new AtomicInteger(0);
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
                    skippedClusters,
                    exceptions,
                    searchResponseMerger,
                    totalClusters,
                    listener
                );
                Client remoteClusterClient = remoteClusterService.getRemoteClusterClient(threadPool, clusterAlias);
                remoteClusterClient.search(ccsSearchRequest, ccsListener);
            }
            if (localIndices != null) {
                ActionListener<SearchResponse> ccsListener = createCCSListener(
                    RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
                    false,
                    countDown,
                    skippedClusters,
                    exceptions,
                    searchResponseMerger,
                    totalClusters,
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

    static void collectSearchShards(
        IndicesOptions indicesOptions,
        String preference,
        String routing,
        QueryBuilder query,
        boolean allowPartialResults,
        SearchContextId searchContext,
        AtomicInteger skippedClusters,
        Map<String, OriginalIndices> remoteIndicesByCluster,
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
                    skippedClusters,
                    exceptions,
                    listener
                ) {
                    @Override
                    void innerOnResponse(SearchShardsResponse searchShardsResponse) {
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
                ActionListener.wrap(connection -> {
                    final String[] indices = entry.getValue().indices();
                    // TODO: support point-in-time
                    if (searchContext == null && connection.getTransportVersion().onOrAfter(TransportVersion.V_8_500_000)) {
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
                            SearchShardsAction.NAME,
                            searchShardsRequest,
                            TransportRequestOptions.EMPTY,
                            new ActionListenerResponseHandler<>(singleListener, SearchShardsResponse::new)
                        );
                    } else {
                        ClusterSearchShardsRequest searchShardsRequest = new ClusterSearchShardsRequest(indices).indicesOptions(
                            indicesOptions
                        ).local(true).preference(preference).routing(routing);
                        transportService.sendRequest(
                            connection,
                            ClusterSearchShardsAction.NAME,
                            searchShardsRequest,
                            TransportRequestOptions.EMPTY,
                            new ActionListenerResponseHandler<>(
                                singleListener.map(SearchShardsResponse::fromLegacyResponse),
                                ClusterSearchShardsResponse::new
                            )
                        );
                    }
                }, singleListener::onFailure)
            );
        }
    }

    private static ActionListener<SearchResponse> createCCSListener(
        String clusterAlias,
        boolean skipUnavailable,
        CountDown countDown,
        AtomicInteger skippedClusters,
        AtomicReference<Exception> exceptions,
        SearchResponseMerger searchResponseMerger,
        int totalClusters,
        ActionListener<SearchResponse> originalListener
    ) {
        return new CCSActionListener<SearchResponse, SearchResponse>(
            clusterAlias,
            skipUnavailable,
            countDown,
            skippedClusters,
            exceptions,
            originalListener
        ) {
            @Override
            void innerOnResponse(SearchResponse searchResponse) {
                searchResponseMerger.add(searchResponse);
            }

            @Override
            SearchResponse createFinalResponse() {
                SearchResponse.Clusters clusters = new SearchResponse.Clusters(
                    totalClusters,
                    searchResponseMerger.numResponses(),
                    skippedClusters.get()
                );
                return searchResponseMerger.getMergedResponse(clusters);
            }
        };
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
                final String clusterAlias = entry.getKey();
                final SearchContextIdForNode perNode = searchContextId.shards().get(shardId);
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
        return remoteShardIterators;
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
                final QueryPhaseResultConsumer queryResultConsumer = searchPhaseController.newSearchPhaseResults(
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
        private final String clusterAlias;
        private final boolean skipUnavailable;
        private final CountDown countDown;
        private final AtomicInteger skippedClusters;
        private final AtomicReference<Exception> exceptions;
        private final ActionListener<FinalResponse> originalListener;

        CCSActionListener(
            String clusterAlias,
            boolean skipUnavailable,
            CountDown countDown,
            AtomicInteger skippedClusters,
            AtomicReference<Exception> exceptions,
            ActionListener<FinalResponse> originalListener
        ) {
            this.clusterAlias = clusterAlias;
            this.skipUnavailable = skipUnavailable;
            this.countDown = countDown;
            this.skippedClusters = skippedClusters;
            this.exceptions = exceptions;
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
            if (skipUnavailable) {
                skippedClusters.incrementAndGet();
            } else {
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
                    originalListener.onResponse(response);
                } else {
                    originalListener.onFailure(exceptions.get());
                }
            }
        }

        abstract FinalResponse createFinalResponse();
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
                // Prefer executing shard requests on nodes that are part of PIT first.
                if (clusterState.nodes().nodeExists(perNode.getNode())) {
                    targetNodes.add(perNode.getNode());
                }
                try {
                    final ShardIterator shards = OperationRouting.getShards(clusterState, shardId);
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
                ShardRouting::isSearchable,
                Objects.requireNonNullElseGet(routingMap, Map::of),
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
