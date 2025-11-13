/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DelegatingActionListener;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.RemoteClusterActionType;
import org.elasticsearch.action.ResolvedIndexExpression;
import org.elasticsearch.action.ResolvedIndexExpressions;
import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.admin.cluster.shards.TransportClusterSearchShardsAction;
import org.elasticsearch.action.admin.cluster.stats.CCSUsage;
import org.elasticsearch.action.admin.cluster.stats.CCSUsageTelemetry;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.ResolvedExpression;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.routing.SearchShardRouting;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.ExecutorSelector;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.node.ResponseCollectorService;
import org.elasticsearch.rest.action.search.SearchResponseMetrics;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.crossproject.CrossProjectIndexResolutionValidator;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchContextId;
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
import org.elasticsearch.usage.UsageService;
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
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static org.elasticsearch.action.search.SearchType.DFS_QUERY_THEN_FETCH;
import static org.elasticsearch.action.search.SearchType.QUERY_THEN_FETCH;
import static org.elasticsearch.action.search.TransportSearchHelper.checkCCSVersionCompatibility;
import static org.elasticsearch.search.crossproject.CrossProjectIndexResolutionValidator.indicesOptionsForCrossProjectFanout;
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
    private final ProjectResolver projectResolver;
    private final ResponseCollectorService responseCollectorService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final CircuitBreaker circuitBreaker;
    private final ExecutorSelector executorSelector;
    private final int defaultPreFilterShardSize;
    private final boolean ccsCheckCompatibility;
    private final SearchResponseMetrics searchResponseMetrics;
    private final Client client;
    private final UsageService usageService;
    private final boolean collectCCSTelemetry;
    private final TimeValue forceConnectTimeoutSecs;
    private final CrossProjectModeDecider crossProjectModeDecider;

    @Inject
    public TransportSearchAction(
        ThreadPool threadPool,
        CircuitBreakerService circuitBreakerService,
        TransportService transportService,
        SearchService searchService,
        ResponseCollectorService responseCollectorService,
        SearchTransportService searchTransportService,
        SearchPhaseController searchPhaseController,
        ClusterService clusterService,
        ActionFilters actionFilters,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver,
        NamedWriteableRegistry namedWriteableRegistry,
        ExecutorSelector executorSelector,
        SearchResponseMetrics searchResponseMetrics,
        Client client,
        UsageService usageService
    ) {
        super(TYPE.name(), transportService, actionFilters, SearchRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.threadPool = threadPool;
        this.circuitBreaker = circuitBreakerService.getBreaker(CircuitBreaker.REQUEST);
        this.searchPhaseController = searchPhaseController;
        this.searchTransportService = searchTransportService;
        this.remoteClusterService = searchTransportService.getRemoteClusterService();
        SearchTransportService.registerRequestHandler(transportService, searchService, namedWriteableRegistry);
        SearchQueryThenFetchAsyncAction.registerNodeSearchAction(
            searchTransportService,
            searchService,
            searchPhaseController,
            namedWriteableRegistry
        );
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.searchService = searchService;
        this.projectResolver = projectResolver;
        this.responseCollectorService = responseCollectorService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.executorSelector = executorSelector;
        var settings = clusterService.getSettings();
        this.defaultPreFilterShardSize = DEFAULT_PRE_FILTER_SHARD_SIZE.get(settings);
        this.ccsCheckCompatibility = SearchService.CCS_VERSION_CHECK_SETTING.get(settings);
        this.collectCCSTelemetry = SearchService.CCS_COLLECT_TELEMETRY.get(settings);
        this.searchResponseMetrics = searchResponseMetrics;
        this.client = client;
        this.usageService = usageService;
        this.forceConnectTimeoutSecs = settings.getAsTime("search.ccs.force_connect_timeout", null);
        this.crossProjectModeDecider = new CrossProjectModeDecider(settings);
    }

    private Map<String, OriginalIndices> buildPerIndexOriginalIndices(
        ProjectState projectState,
        Set<ResolvedExpression> indicesAndAliases,
        String[] indices,
        IndicesOptions indicesOptions
    ) {
        Map<String, OriginalIndices> res = Maps.newMapWithExpectedSize(indices.length);
        var blocks = projectState.blocks();
        var projectId = projectState.projectId();
        // optimization: mostly we do not have any blocks so there's no point in the expensive per-index checking
        boolean hasBlocks = blocks.global(projectId).isEmpty() == false || blocks.indices(projectState.projectId()).isEmpty() == false;
        // Get a distinct set of index abstraction names present from the resolved expressions to help with the reverse resolution from
        // concrete index to the expression that produced it.
        Set<String> indicesAndAliasesResources = indicesAndAliases.stream().map(ResolvedExpression::resource).collect(Collectors.toSet());
        for (String index : indices) {
            if (hasBlocks) {
                blocks.indexBlockedRaiseException(projectId, ClusterBlockLevel.READ, index);
            }

            String[] aliases = indexNameExpressionResolver.allIndexAliases(projectState.metadata(), index, indicesAndAliases);
            String[] finalIndices = Strings.EMPTY_ARRAY;
            if (aliases == null
                || aliases.length == 0
                || indicesAndAliasesResources.contains(index)
                || hasDataStreamRef(projectState.metadata(), indicesAndAliasesResources, index)) {
                finalIndices = new String[] { index };
            }
            if (aliases != null) {
                finalIndices = finalIndices.length == 0 ? aliases : ArrayUtils.concat(finalIndices, aliases);
            }
            res.put(index, new OriginalIndices(finalIndices, indicesOptions));
        }
        return res;
    }

    private static boolean hasDataStreamRef(ProjectMetadata project, Set<String> indicesAndAliases, String index) {
        IndexAbstraction ret = project.getIndicesLookup().get(index);
        if (ret == null || ret.getParentDataStream() == null) {
            return false;
        }
        return indicesAndAliases.contains(ret.getParentDataStream().getName());
    }

    Map<String, AliasFilter> buildIndexAliasFilters(
        ProjectState projectState,
        Set<ResolvedExpression> indicesAndAliases,
        Index[] concreteIndices
    ) {
        final Map<String, AliasFilter> aliasFilterMap = new HashMap<>();
        for (Index index : concreteIndices) {
            projectState.blocks().indexBlockedRaiseException(projectState.projectId(), ClusterBlockLevel.READ, index.getName());
            AliasFilter aliasFilter = searchService.buildAliasFilter(projectState, index.getName(), indicesAndAliases);
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
        executeRequest((SearchTask) task, searchRequest, listener, AsyncSearchActionProvider::new, true);
    }

    void executeOpenPit(
        SearchTask task,
        SearchRequest original,
        ActionListener<SearchResponse> originalListener,
        Function<ActionListener<SearchResponse>, SearchPhaseProvider> searchPhaseProvider
    ) {
        executeRequest(task, original, originalListener, searchPhaseProvider, false);
    }

    private void executeRequest(
        SearchTask task,
        SearchRequest original,
        ActionListener<SearchResponse> originalListener,
        Function<ActionListener<SearchResponse>, SearchPhaseProvider> searchPhaseProvider,
        boolean collectSearchTelemetry
    ) {
        boolean resolvesCrossProject = crossProjectModeDecider.resolvesCrossProject(original);
        final long relativeStartNanos = System.nanoTime();
        final SearchTimeProvider timeProvider = new SearchTimeProvider(
            original.getOrCreateAbsoluteStartMillis(),
            relativeStartNanos,
            System::nanoTime
        );

        final ClusterState clusterState = clusterService.state();
        clusterState.blocks().globalBlockedRaiseException(projectResolver.getProjectId(), ClusterBlockLevel.READ);

        ProjectState projectState = projectResolver.getProjectState(clusterState);
        final ResolvedIndices resolvedIndices;

        /*
         * If this search request is originating from a search endpoint that is CPS compatible, and,
         * CPS is enabled, i.e. resolvesCrossProject() returns true, we need to modify and relax the
         * indices options. This is to:
         *   a) Prevent the downstream code from throwing an error if an index is not found since in
         *      CPS, an index can exist anywhere.
         *   b) Prevent the linked projects from re-interpreting the index expressions as CPS expressions
         *      and rather treat them as canonical/standard ones.
         */
        IndicesOptions resolutionIdxOpts = resolvesCrossProject
            ? indicesOptionsForCrossProjectFanout(original.indicesOptions())
            : original.indicesOptions();

        if (original.pointInTimeBuilder() != null) {
            resolvedIndices = ResolvedIndices.resolveWithPIT(
                original.pointInTimeBuilder(),
                resolutionIdxOpts,
                projectState.metadata(),
                namedWriteableRegistry
            );
        } else {
            resolvedIndices = ResolvedIndices.resolveWithIndexNamesAndOptions(
                original.indices(),
                resolutionIdxOpts,
                projectState.metadata(),
                indexNameExpressionResolver,
                remoteClusterService,
                timeProvider.absoluteStartMillis()
            );
            frozenIndexCheck(resolvedIndices);
        }

        final SearchSourceBuilder source = original.source();
        if (shouldOpenPIT(source)) {
            // disabling shard reordering for request
            original.setPreFilterShardSize(Integer.MAX_VALUE);
            openPIT(
                client,
                original,
                searchService.getDefaultKeepAliveInMillis(),
                originalListener.delegateFailureAndWrap((delegate, resp) -> {
                    // We set the keep alive to -1 to indicate that we don't need the pit id in the response.
                    // This is needed since we delete the pit prior to sending the response so the id doesn't exist anymore.
                    source.pointInTimeBuilder(new PointInTimeBuilder(resp.getPointInTimeId()).setKeepAlive(TimeValue.MINUS_ONE));
                    var pitListener = new ActionListener<SearchResponse>() {
                        @Override
                        public void onResponse(SearchResponse response) {
                            // we need to close the PIT first so we delay the release of the response to after the closing
                            response.incRef();
                            closePIT(
                                client,
                                original.source().pointInTimeBuilder(),
                                () -> ActionListener.respondAndRelease(delegate, response)
                            );
                        }

                        @Override
                        public void onFailure(Exception e) {
                            closePIT(client, original.source().pointInTimeBuilder(), () -> delegate.onFailure(e));
                        }
                    };
                    executeRequest(task, original, pitListener, searchPhaseProvider, true);
                })
            );
            return;
        }

        ActionListener<SearchRequest> rewriteListener = originalListener.delegateFailureAndWrap((delegate, rewritten) -> {
            if (ccsCheckCompatibility) {
                checkCCSVersionCompatibility(rewritten);
            }

            final ActionListener<SearchResponse> searchResponseActionListener;
            if (collectSearchTelemetry) {
                Map<String, Object> searchRequestAttributes = SearchRequestAttributesExtractor.extractAttributes(
                    original,
                    Arrays.stream(resolvedIndices.getConcreteLocalIndices()).map(Index::getName).toArray(String[]::new)
                );
                if (collectCCSTelemetry == false || resolvedIndices.getRemoteClusterIndices().isEmpty()) {
                    searchResponseActionListener = new SearchTelemetryListener(
                        delegate,
                        searchResponseMetrics,
                        searchRequestAttributes,
                        timeProvider.absoluteStartMillis()
                    );
                } else {
                    CCSUsage.Builder usageBuilder = new CCSUsage.Builder();
                    usageBuilder.setRemotesCount(resolvedIndices.getRemoteClusterIndices().size());
                    usageBuilder.setClientFromTask(task);
                    if (task.isAsync()) {
                        usageBuilder.setFeature(CCSUsageTelemetry.ASYNC_FEATURE);
                    }
                    if (original.pointInTimeBuilder() != null) {
                        usageBuilder.setFeature(CCSUsageTelemetry.PIT_FEATURE);
                    }
                    // Check if any of the index patterns are wildcard patterns
                    var localIndices = resolvedIndices.getLocalIndices();
                    if (localIndices != null && Arrays.stream(localIndices.indices()).anyMatch(Regex::isSimpleMatchPattern)) {
                        usageBuilder.setFeature(CCSUsageTelemetry.WILDCARD_FEATURE);
                    }
                    if (resolvedIndices.getRemoteClusterIndices()
                        .values()
                        .stream()
                        .anyMatch(indices -> Arrays.stream(indices.indices()).anyMatch(Regex::isSimpleMatchPattern))) {
                        usageBuilder.setFeature(CCSUsageTelemetry.WILDCARD_FEATURE);
                    }
                    if (shouldMinimizeRoundtrips(rewritten)) {
                        usageBuilder.setFeature(CCSUsageTelemetry.MRT_FEATURE);
                    }
                    searchResponseActionListener = new SearchTelemetryListener(
                        delegate,
                        searchResponseMetrics,
                        searchRequestAttributes,
                        timeProvider.absoluteStartMillis(),
                        usageService,
                        usageBuilder
                    );
                }
            } else {
                searchResponseActionListener = delegate;
            }

            if (resolvedIndices.getRemoteClusterIndices().isEmpty()) {
                executeLocalSearch(
                    task,
                    timeProvider,
                    rewritten,
                    resolvedIndices,
                    projectState,
                    SearchResponse.Clusters.EMPTY,
                    searchPhaseProvider.apply(searchResponseActionListener)
                );
            } else {
                final TaskId parentTaskId = task.taskInfo(clusterService.localNode().getId(), false).taskId();
                if (shouldMinimizeRoundtrips(rewritten)) {
                    final AggregationReduceContext.Builder aggregationReduceContextBuilder = rewritten.source() != null
                        && rewritten.source().aggregations() != null
                            ? searchService.aggReduceContextBuilder(task::isCancelled, rewritten.source().aggregations())
                            : null;
                    SearchResponse.Clusters clusters = new SearchResponse.Clusters(
                        resolvedIndices.getLocalIndices(),
                        resolvedIndices.getRemoteClusterIndices(),
                        true,
                        (clusterAlias) -> remoteClusterService.shouldSkipOnFailure(clusterAlias, rewritten.allowPartialSearchResults())
                    );
                    if (resolvedIndices.getLocalIndices() == null) {
                        // Notify the progress listener that a CCS with minimize_roundtrips is happening remote-only (no local shards)
                        task.getProgressListener()
                            .notifyListShards(Collections.emptyList(), Collections.emptyList(), clusters, false, timeProvider);
                    }
                    ccsRemoteReduce(
                        task,
                        parentTaskId,
                        rewritten,
                        resolvedIndices,
                        clusters,
                        timeProvider,
                        aggregationReduceContextBuilder,
                        remoteClusterService,
                        threadPool,
                        searchResponseActionListener,
                        (r, l) -> executeLocalSearch(
                            task,
                            timeProvider,
                            r,
                            resolvedIndices,
                            projectState,
                            clusters,
                            searchPhaseProvider.apply(l)
                        ),
                        transportService,
                        forceConnectTimeoutSecs
                    );
                } else {
                    final SearchContextId searchContext = resolvedIndices.getSearchContextId();
                    SearchResponse.Clusters clusters = new SearchResponse.Clusters(
                        resolvedIndices.getLocalIndices(),
                        resolvedIndices.getRemoteClusterIndices(),
                        false,
                        (clusterAlias) -> remoteClusterService.shouldSkipOnFailure(clusterAlias, rewritten.allowPartialSearchResults())
                    );

                    // TODO: pass parentTaskId
                    collectSearchShards(
                        rewritten.indicesOptions(),
                        rewritten.preference(),
                        rewritten.routing(),
                        rewritten.source() != null ? rewritten.source().query() : null,
                        Objects.requireNonNullElse(rewritten.allowPartialSearchResults(), searchService.defaultAllowPartialSearchResults()),
                        searchContext,
                        resolvedIndices.getRemoteClusterIndices(),
                        clusters,
                        timeProvider,
                        transportService,
                        searchResponseActionListener.delegateFailureAndWrap((finalDelegate, searchShardsResponses) -> {
                            SearchResponse.Clusters participatingProjects = clusters;
                            if (resolvesCrossProject && rewritten.getResolvedIndexExpressions() != null) {
                                participatingProjects = reconcileProjects(
                                    rewritten.getResolvedIndexExpressions(),
                                    searchShardsResponses,
                                    participatingProjects
                                );
                            }

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
                                    resolvedIndices.getRemoteClusterIndices()
                                );
                            } else {
                                remoteAliasFilters = new HashMap<>();
                                for (SearchShardsResponse searchShardsResponse : searchShardsResponses.values()) {
                                    remoteAliasFilters.putAll(searchShardsResponse.getAliasFilters());
                                }
                                remoteShardIterators = getRemoteShardsIterator(
                                    searchShardsResponses,
                                    resolvedIndices.getRemoteClusterIndices(),
                                    remoteAliasFilters
                                );
                            }
                            executeSearch(
                                task,
                                timeProvider,
                                rewritten,
                                resolvedIndices,
                                remoteShardIterators,
                                clusterNodeLookup,
                                projectState,
                                remoteAliasFilters,
                                participatingProjects,
                                searchPhaseProvider.apply(finalDelegate)
                            );
                        }),
                        forceConnectTimeoutSecs,
                        resolvesCrossProject,
                        rewritten.getResolvedIndexExpressions(),
                        rewritten.getProjectRouting()
                    );
                }
            }
        });

        final boolean isExplain = source != null && source.explain() != null && source.explain();
        final boolean isProfile = source != null && source.profile();
        Rewriteable.rewriteAndFetch(
            original,
            searchService.getRewriteContext(
                timeProvider::absoluteStartMillis,
                clusterState.getMinTransportVersion(),
                original.getLocalClusterAlias(),
                resolvedIndices,
                original.pointInTimeBuilder(),
                shouldMinimizeRoundtrips(original),
                isExplain,
                isProfile
            ),
            rewriteListener
        );
    }

    /**
     * Returns true if the provided source needs to open a shared point in time prior to executing the request.
     */
    private boolean shouldOpenPIT(SearchSourceBuilder source) {
        if (source == null) {
            return false;
        }
        if (source.pointInTimeBuilder() != null) {
            return false;
        }
        var retriever = source.retriever();
        return retriever != null && retriever.isCompound();
    }

    static void openPIT(Client client, SearchRequest request, long keepAliveMillis, ActionListener<OpenPointInTimeResponse> listener) {
        OpenPointInTimeRequest pitReq = new OpenPointInTimeRequest(request.indices()).indicesOptions(request.indicesOptions())
            .preference(request.preference())
            .routing(request.routing())
            .keepAlive(TimeValue.timeValueMillis(keepAliveMillis));
        client.execute(TransportOpenPointInTimeAction.TYPE, pitReq, listener);
    }

    static void closePIT(Client client, PointInTimeBuilder pit, Runnable next) {
        client.execute(
            TransportClosePointInTimeAction.TYPE,
            new ClosePointInTimeRequest(pit.getEncodedId()),
            ActionListener.runAfter(new ActionListener<>() {
                @Override
                public void onResponse(ClosePointInTimeResponse closePointInTimeResponse) {}

                @Override
                public void onFailure(Exception e) {}
            }, next)
        );
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
     * Return a subscribable listener with optional timeout depending on force reconnect setting is registered or
     * not.
     * @param forceConnectTimeoutSecs Timeout in seconds that determines how long we'll wait to establish a connection
     *                                to a remote.
     * @param threadPool The thread pool that'll be used for the timeout.
     * @param timeoutExecutor The executor that should be used for the timeout.
     * @return SubscribableListener A listener with optionally added timeout.
     */
    private static SubscribableListener<Transport.Connection> getListenerWithOptionalTimeout(
        TimeValue forceConnectTimeoutSecs,
        ThreadPool threadPool,
        Executor timeoutExecutor
    ) {
        var subscribableListener = new SubscribableListener<Transport.Connection>();
        if (forceConnectTimeoutSecs != null) {
            subscribableListener.addTimeout(forceConnectTimeoutSecs, threadPool, timeoutExecutor);
        }

        return subscribableListener;
    }

    /**
     * The default disconnected strategy for Elasticsearch is RECONNECT_UNLESS_SKIP_UNAVAILABLE. So we either force
     * connect if required (like in CPS) or when skip unavailable is false for a cluster.
     * @param forceConnectTimeoutSecs The timeout value from the force connect setting.
     *                                If it is set, use it as it takes precedence.
     * @param skipUnavailable The usual skip unavailable setting.
     * @return boolean If we should always force reconnect.
     */
    private static boolean shouldEstablishConnection(TimeValue forceConnectTimeoutSecs, boolean skipUnavailable) {
        return forceConnectTimeoutSecs != null || skipUnavailable == false;
    }

    /**
     * Handles ccs_minimize_roundtrips=true
     */
    static void ccsRemoteReduce(
        SearchTask task,
        TaskId parentTaskId,
        SearchRequest searchRequest,
        ResolvedIndices resolvedIndices,
        SearchResponse.Clusters clusters,
        SearchTimeProvider timeProvider,
        AggregationReduceContext.Builder aggReduceContextBuilder,
        RemoteClusterService remoteClusterService,
        ThreadPool threadPool,
        ActionListener<SearchResponse> listener,
        BiConsumer<SearchRequest, ActionListener<SearchResponse>> localSearchConsumer,
        TransportService transportService,
        TimeValue forceConnectTimeoutSecs
    ) {
        final var remoteClientResponseExecutor = threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION);
        if (resolvedIndices.getLocalIndices() == null && resolvedIndices.getRemoteClusterIndices().size() == 1) {
            // if we are searching against a single remote cluster, we simply forward the original search request to such cluster
            // and we directly perform final reduction in the remote cluster
            Map.Entry<String, OriginalIndices> entry = resolvedIndices.getRemoteClusterIndices().entrySet().iterator().next();
            String clusterAlias = entry.getKey();
            boolean shouldSkipOnFailure = remoteClusterService.shouldSkipOnFailure(clusterAlias, searchRequest.allowPartialSearchResults());
            OriginalIndices indices = entry.getValue();
            SearchRequest ccsSearchRequest = SearchRequest.subSearchRequest(
                parentTaskId,
                searchRequest,
                indices.indices(),
                clusterAlias,
                timeProvider.absoluteStartMillis(),
                true
            );

            var connectionListener = getListenerWithOptionalTimeout(forceConnectTimeoutSecs, threadPool, remoteClientResponseExecutor);
            var searchListener = new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    // overwrite the existing cluster entry with the updated one
                    ccsClusterInfoUpdate(searchResponse, clusters, clusterAlias, shouldSkipOnFailure);
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
                    logCCSError(failure, clusterAlias, shouldSkipOnFailure);
                    ccsClusterInfoUpdate(failure, clusters, clusterAlias, shouldSkipOnFailure);
                    if (shouldSkipOnFailure) {
                        ActionListener.respondAndRelease(listener, SearchResponse.empty(timeProvider::buildTookInMillis, clusters));
                    } else {
                        listener.onFailure(wrapRemoteClusterFailure(clusterAlias, e));
                    }
                }
            };

            connectionListener.addListener(
                searchListener.delegateFailure(
                    (responseListener, connection) -> transportService.sendRequest(
                        connection,
                        TransportSearchAction.TYPE.name(),
                        ccsSearchRequest,
                        TransportRequestOptions.EMPTY,
                        new ActionListenerResponseHandler<>(responseListener, SearchResponse::new, remoteClientResponseExecutor)
                    )
                )
            );

            remoteClusterService.maybeEnsureConnectedAndGetConnection(
                clusterAlias,
                shouldEstablishConnection(forceConnectTimeoutSecs, shouldSkipOnFailure),
                connectionListener
            );
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
            int totalClusters = resolvedIndices.getRemoteClusterIndices().size() + (resolvedIndices.getLocalIndices() == null ? 0 : 1);
            final CountDown countDown = new CountDown(totalClusters);
            for (Map.Entry<String, OriginalIndices> entry : resolvedIndices.getRemoteClusterIndices().entrySet()) {
                String clusterAlias = entry.getKey();
                boolean shouldSkipOnFailure = remoteClusterService.shouldSkipOnFailure(
                    clusterAlias,
                    searchRequest.allowPartialSearchResults()
                );
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
                    shouldSkipOnFailure,
                    countDown,
                    exceptions,
                    searchResponseMerger,
                    clusters,
                    task.getProgressListener(),
                    listener
                );

                SubscribableListener<Transport.Connection> connectionListener = getListenerWithOptionalTimeout(
                    forceConnectTimeoutSecs,
                    threadPool,
                    remoteClientResponseExecutor
                );

                connectionListener.addListener(
                    ccsListener.delegateFailure(
                        (responseListener, connection) -> transportService.sendRequest(
                            connection,
                            TransportSearchAction.REMOTE_TYPE.name(),
                            ccsSearchRequest,
                            TransportRequestOptions.EMPTY,
                            new ActionListenerResponseHandler<>(responseListener, SearchResponse::new, remoteClientResponseExecutor)
                        )
                    )
                );

                remoteClusterService.maybeEnsureConnectedAndGetConnection(
                    clusterAlias,
                    shouldEstablishConnection(forceConnectTimeoutSecs, shouldSkipOnFailure),
                    connectionListener
                );
            }
            if (resolvedIndices.getLocalIndices() != null) {
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
                    resolvedIndices.getLocalIndices().indices(),
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
     * Reconciliation is only done when Cross Project Search is enabled and requests originate from a CPS
     * compatible endpoints. Outside CPS, we're sure of projects involved and their corresponding indices.
     * However, in CPS, it may be possible that indices can exist anywhere:
     * <ul>
     *     <li>Only on the origin</li>
     *     <li>Only on the linked project(s)</li>
     *     <li>Both on the origin and the linked project(s), and,</li>
     *     <li>Nowhere</li>
     * </ul>
     *
     * Therefore, we only need to include the details of those projects hosting our indices and participating
     * in the search. Otherwise, we risk unnecessarily including them in the execution metadata and marking
     * their statuses as "successful", potentially misleading users into believing that they returned results
     * and participated in the search.
     *
     * Note that this code runs after the SearchShards API's responses have been pieced back and the CPS index
     * validation is complete.
     * @param originResolvedIdxExpressions The resolution result from origin's Security Action Filter.
     * @param shardResponses Responses pieced back from SearchShards API.
     * @param projects The clusters originally in scope for the query.
     * @return A new Clusters object containing only the Search-participating projects.
     */
    static SearchResponse.Clusters reconcileProjects(
        ResolvedIndexExpressions originResolvedIdxExpressions,
        Map<String, SearchShardsResponse> shardResponses,
        SearchResponse.Clusters projects
    ) {
        /*
         * We only fire a SearchShards API call for a project if it needs to be searched. This can either mean that it was
         * part of the search due to the flatworld behaviour, or that it was targeted specifically. If it returns an
         * empty response, it's because the project does not host any of our specified indices.
         */
        Set<String> linkedProjectsWithResponses = shardResponses.entrySet()
            .stream()
            .filter(ssr -> ssr.getValue().getGroups().isEmpty() == false)
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());

        /*
         * We don't show any metadata in the response if there are no linked projects to search.
         * In that case, it's alright to return `Clusters.EMPTY` and is in fact what we do in
         * stateful, i.e. outside CPS.
         */
        if (linkedProjectsWithResponses.isEmpty()) {
            return SearchResponse.Clusters.EMPTY;
        }

        boolean shouldIncludeOrigin = originResolvedIdxExpressions.expressions()
            .stream()
            .anyMatch(
                expr -> expr.localExpressions().localIndexResolutionResult() == ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS
            );

        Map<String, SearchResponse.Cluster> reconciledMap = new HashMap<>();
        for (String project : projects.getClusterAliases()) {
            SearchResponse.Cluster computedProjectInfo = projects.getCluster(project);
            /*
             * Selection criteria for a `project` to be included in the metadata:
             *   - This is the origin project, and there was a "success"ful resolution by the Security Action Filter,
             *   - This is a linked project with a non-empty response from SearchShards API, or,
             *   - There was an issue with this project, so let's carry over the failures and reporting them.
             */
            boolean shouldAdd = false;
            if (shouldIncludeOrigin && project.equals(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY)) {
                shouldAdd = true;
            } else if (linkedProjectsWithResponses.contains(project)) {
                shouldAdd = true;
            } else if (computedProjectInfo.getFailures().isEmpty() == false) {
                shouldAdd = true;
            }

            if (shouldAdd) {
                reconciledMap.put(project, computedProjectInfo);
            }
        }

        return new SearchResponse.Clusters(reconciledMap, false);
    }

    /**
     * Collect remote search shards that we need to search for potential matches.
     * Used for ccs_minimize_roundtrips=false
     */
    static void collectSearchShards(
        IndicesOptions originalIdxOpts,
        String preference,
        String routing,
        QueryBuilder query,
        boolean allowPartialResults,
        SearchContextId searchContext,
        Map<String, OriginalIndices> remoteIndicesByCluster,
        SearchResponse.Clusters clusters,
        SearchTimeProvider timeProvider,
        TransportService transportService,
        ActionListener<Map<String, SearchShardsResponse>> listener,
        TimeValue forceConnectTimeoutSecs,
        boolean resolvesCrossProject,
        ResolvedIndexExpressions originResolvedIdxExpressions,
        String projectRouting
    ) {
        RemoteClusterService remoteClusterService = transportService.getRemoteClusterService();
        final CountDown responsesCountDown = new CountDown(remoteIndicesByCluster.size());
        final Map<String, SearchShardsResponse> searchShardsResponses = new ConcurrentHashMap<>();
        final AtomicReference<Exception> exceptions = new AtomicReference<>();
        for (Map.Entry<String, OriginalIndices> entry : remoteIndicesByCluster.entrySet()) {
            final String clusterAlias = entry.getKey();
            boolean shouldSkipOnFailure = remoteClusterService.shouldSkipOnFailure(clusterAlias, allowPartialResults);
            CCSActionListener<SearchShardsResponse, Map<String, SearchShardsResponse>> singleListener = new CCSActionListener<>(
                clusterAlias,
                shouldSkipOnFailure,
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
                    if (resolvesCrossProject && originResolvedIdxExpressions != null) {
                        Map<String, ResolvedIndexExpressions> resolvedIndexExpressions = new HashMap<>();
                        for (Map.Entry<String, SearchShardsResponse> entry : searchShardsResponses.entrySet()) {
                            if (entry.getValue().getResolvedIndexExpressions() == null) {
                                throw new IllegalArgumentException(
                                    "Failed to get resolved index expressions for cluster [" + entry.getKey() + "]"
                                );
                            }
                            resolvedIndexExpressions.put(entry.getKey(), entry.getValue().getResolvedIndexExpressions());
                        }
                        // We do not use the relaxed index options here when validating indices' existence.
                        ElasticsearchException validationEx = CrossProjectIndexResolutionValidator.validate(
                            originalIdxOpts,
                            projectRouting,
                            originResolvedIdxExpressions,
                            resolvedIndexExpressions
                        );
                        if (validationEx != null) {
                            throw validationEx;
                        }
                    }
                    return searchShardsResponses;
                }
            };

            var threadPool = transportService.getThreadPool();
            var connectionListener = getListenerWithOptionalTimeout(
                forceConnectTimeoutSecs,
                threadPool,
                threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION)
            );

            connectionListener.addListener(singleListener.delegateFailure((responseListener, connection) -> {
                /*
                 * It may be possible that indices do not exist on the project that SearchShards API is targeting.
                 * In such cases, it throws an error because it calls the index resolution APIs underneath. We relax
                 * the index options to prevent this from happening, iff this is a CPS request and CPS is enabled.
                 * Also, it's fine to pass in these relaxed options to it because SearchShardsRequest#allowsCrossProject()
                 * returns false anyway and the index rewriting does not happen downstream.
                 */
                IndicesOptions searchShardsIdxOpts = resolvesCrossProject
                    ? indicesOptionsForCrossProjectFanout(originalIdxOpts)
                    : originalIdxOpts;

                final String[] indices = entry.getValue().indices();
                final Executor responseExecutor = transportService.getThreadPool().executor(ThreadPool.Names.SEARCH_COORDINATION);
                // TODO: support point-in-time
                if (searchContext == null && connection.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
                    SearchShardsRequest searchShardsRequest = new SearchShardsRequest(
                        indices,
                        searchShardsIdxOpts,
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
                        new ActionListenerResponseHandler<>(responseListener, SearchShardsResponse::new, responseExecutor)
                    );
                } else {
                    // does not do a can-match
                    ClusterSearchShardsRequest searchShardsRequest = new ClusterSearchShardsRequest(
                        MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
                        indices
                    ).indicesOptions(searchShardsIdxOpts).local(true).preference(preference).routing(routing);
                    transportService.sendRequest(
                        connection,
                        TransportClusterSearchShardsAction.TYPE.name(),
                        searchShardsRequest,
                        TransportRequestOptions.EMPTY,
                        new ActionListenerResponseHandler<>(
                            singleListener.map(SearchShardsResponse::fromLegacyResponse),
                            ClusterSearchShardsResponse::new,
                            responseExecutor
                        )
                    );
                }
            }));

            remoteClusterService.maybeEnsureConnectedAndGetConnection(
                clusterAlias,
                shouldEstablishConnection(forceConnectTimeoutSecs, shouldSkipOnFailure),
                connectionListener
            );
        }
    }

    /**
     * Only used for ccs_minimize_roundtrips=true pathway
     */
    private static ActionListener<SearchResponse> createCCSListener(
        String clusterAlias,
        boolean shouldSkipOnFailure,
        CountDown countDown,
        AtomicReference<Exception> exceptions,
        SearchResponseMerger searchResponseMerger,
        SearchResponse.Clusters clusters,
        SearchProgressListener progressListener,
        ActionListener<SearchResponse> originalListener
    ) {
        return new CCSActionListener<>(
            clusterAlias,
            shouldSkipOnFailure,
            countDown,
            exceptions,
            clusters,
            ActionListener.releaseAfter(originalListener, searchResponseMerger)
        ) {
            @Override
            void innerOnResponse(SearchResponse searchResponse) {
                ccsClusterInfoUpdate(searchResponse, clusters, clusterAlias, shouldSkipOnFailure);
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
     * Creates a new Cluster object using the {@link ShardSearchFailure} info and shouldSkipOnFailure
     * flag to set Status. Then it swaps it in the clusters CHM at key clusterAlias
     */
    static void ccsClusterInfoUpdate(
        ShardSearchFailure failure,
        SearchResponse.Clusters clusters,
        String clusterAlias,
        boolean shouldSkipOnFailure
    ) {
        clusters.swapCluster(clusterAlias, (k, v) -> {
            SearchResponse.Cluster.Status status;
            if (shouldSkipOnFailure) {
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
        boolean shouldSkipOnFailure
    ) {
        /*
         * Cluster Status logic:
         * 1) FAILED if total_shards > 0 && all shards failed && shouldSkipOnFailure=false
         * 2) SKIPPED if total_shards > 0 && all shards failed && shouldSkipOnFailure=true
         * 3) PARTIAL if it timed out
         * 4) PARTIAL if it at least one of the shards succeeded but not all
         * 5) SUCCESSFUL if no shards failed (and did not time out)
         */
        clusters.swapCluster(clusterAlias, (k, v) -> {
            SearchResponse.Cluster.Status status;
            int totalShards = searchResponse.getTotalShards();
            if (totalShards > 0 && searchResponse.getFailedShards() >= totalShards) {
                if (shouldSkipOnFailure) {
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
        ResolvedIndices resolvedIndices,
        ProjectState projectState,
        SearchResponse.Clusters clusterInfo,
        SearchPhaseProvider searchPhaseProvider
    ) {
        executeSearch(
            (SearchTask) task,
            timeProvider,
            searchRequest,
            resolvedIndices,
            Collections.emptyList(),
            (clusterName, nodeId) -> null,
            projectState,
            Collections.emptyMap(),
            clusterInfo,
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

    /**
     * Produce a list of {@link SearchShardIterator}s from the set of responses from remote clusters.
     * Used for ccs_minimize_roundtrips=false.
     */
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
                    searchShardsGroup.skipped(),
                    // This parameter is specific to the resharding feature.
                    // Resharding is currently not supported with CCS.
                    SplitShardCountSummary.UNSET
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
                if (perNode.getNode() != null) {
                    // If the shard was available when the PIT was created, it's included.
                    // Otherwise, we add the shard iterator without a target node, allowing a partial search failure to
                    // be thrown when a search phase attempts to access it.
                    targetNodes.add(perNode.getNode());
                    ShardSearchContextId shardSearchContextId = perNode.getSearchContextId();
                    if (shardSearchContextId != null && shardSearchContextId.isRetryable()) {
                        for (String node : group.allocatedNodes()) {
                            if (node.equals(perNode.getNode()) == false) {
                                targetNodes.add(node);
                            }
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
                    false,
                    // This parameter is specific to the resharding feature.
                    // Resharding is currently not supported with CCS.
                    SplitShardCountSummary.UNSET
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

    /**
     * If any of the indices we are searching are frozen, issue deprecation warning.
     */
    void frozenIndexCheck(ResolvedIndices resolvedIndices) {
        List<String> frozenIndices = new ArrayList<>();
        Map<Index, IndexMetadata> indexMetadataMap = resolvedIndices.getConcreteLocalIndicesMetadata();
        for (var entry : indexMetadataMap.entrySet()) {
            if (entry.getValue().getSettings().getAsBoolean("index.frozen", false)) {
                frozenIndices.add(entry.getKey().getName());
            }
        }

        if (frozenIndices.isEmpty() == false) {
            DEPRECATION_LOGGER.warn(
                DeprecationCategory.INDICES,
                "search-frozen-indices",
                FROZEN_INDICES_DEPRECATION_MESSAGE,
                String.join(",", frozenIndices)
            );
        }
    }

    /**
     * Execute search locally and for all given remote shards.
     * Used when minimize_roundtrips=false or for local search.
     */
    private void executeSearch(
        SearchTask task,
        SearchTimeProvider timeProvider,
        SearchRequest searchRequest,
        ResolvedIndices resolvedIndices,
        List<SearchShardIterator> remoteShardIterators,
        BiFunction<String, String, DiscoveryNode> remoteConnections,
        ProjectState projectState,
        Map<String, AliasFilter> remoteAliasMap,
        SearchResponse.Clusters clusters,
        SearchPhaseProvider searchPhaseProvider
    ) {
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
        if (resolvedIndices.getSearchContextId() != null) {
            assert searchRequest.pointInTimeBuilder() != null;
            aliasFilter = resolvedIndices.getSearchContextId().aliasFilter();
            concreteLocalIndices = resolvedIndices.getLocalIndices() == null ? new String[0] : resolvedIndices.getLocalIndices().indices();
            localShardIterators = getLocalShardsIteratorFromPointInTime(
                projectState,
                searchRequest.indicesOptions(),
                searchRequest.getLocalClusterAlias(),
                resolvedIndices.getSearchContextId(),
                searchRequest.pointInTimeBuilder().getKeepAlive(),
                searchRequest.allowPartialSearchResults()
            );
        } else {
            final Index[] indices = resolvedIndices.getConcreteLocalIndices();
            concreteLocalIndices = Arrays.stream(indices).map(Index::getName).toArray(String[]::new);
            final Set<ResolvedExpression> indicesAndAliases = indexNameExpressionResolver.resolveExpressions(
                projectState.metadata(),
                searchRequest.indices()
            );
            aliasFilter = buildIndexAliasFilters(projectState, indicesAndAliases, indices);
            aliasFilter.putAll(remoteAliasMap);
            localShardIterators = getLocalShardsIterator(
                projectState,
                searchRequest,
                searchRequest.getLocalClusterAlias(),
                indicesAndAliases,
                concreteLocalIndices
            );

            // localShardIterators is empty since there are no matching indices. In such cases,
            // we update the local cluster's status from RUNNING to SUCCESSFUL right away. Before
            // we attempt to do that, we must ensure that the local cluster was specified in the user's
            // search request. This is done by trying to fetch the local cluster via getCluster() and
            // checking for a non-null return value. If the local cluster was never specified, its status
            // update can be skipped.
            if (localShardIterators.isEmpty()
                && clusters != SearchResponse.Clusters.EMPTY
                && clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY) != null) {
                clusters.swapCluster(
                    RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
                    (alias, v) -> new SearchResponse.Cluster.Builder(v).setStatus(SearchResponse.Cluster.Status.SUCCESSFUL)
                        .setTotalShards(0)
                        .setSuccessfulShards(0)
                        .setSkippedShards(0)
                        .setFailedShards(0)
                        .setFailures(Collections.emptyList())
                        .setTook(TimeValue.timeValueMillis(0))
                        .setTimedOut(false)
                        .build()
                );
            }
        }
        final List<SearchShardIterator> shardIterators = mergeShardsIterators(localShardIterators, remoteShardIterators);

        failIfOverShardCountLimit(clusterService, shardIterators.size());

        if (searchRequest.getWaitForCheckpoints().isEmpty() == false) {
            if (remoteShardIterators.isEmpty() == false) {
                throw new IllegalArgumentException("Cannot use wait_for_checkpoints parameter with cross-cluster searches.");
            } else {
                validateAndResolveWaitForCheckpoint(projectState, indexNameExpressionResolver, searchRequest, concreteLocalIndices);
            }
        }

        Map<String, Float> concreteIndexBoosts = resolveIndexBoosts(searchRequest, projectState.cluster());

        adjustSearchType(searchRequest, shardIterators.size() == 1);

        final DiscoveryNodes nodes = projectState.cluster().nodes();
        BiFunction<String, String, Transport.Connection> connectionLookup = buildConnectionLookup(
            searchRequest.getLocalClusterAlias(),
            nodes::get,
            remoteConnections,
            searchTransportService::getConnection
        );
        final Executor asyncSearchExecutor = asyncSearchExecutor(concreteLocalIndices);
        final boolean preFilterSearchShards = shouldPreFilterSearchShards(
            projectState,
            searchRequest,
            concreteLocalIndices,
            localShardIterators.size() + remoteShardIterators.size(),
            defaultPreFilterShardSize
        );
        final Map<String, Object> searchRequestAttributes = SearchRequestAttributesExtractor.extractAttributes(
            searchRequest,
            concreteLocalIndices
        );
        searchPhaseProvider.runNewSearchPhase(
            task,
            searchRequest,
            asyncSearchExecutor,
            shardIterators,
            timeProvider,
            connectionLookup,
            projectState.cluster(),
            Collections.unmodifiableMap(aliasFilter),
            concreteIndexBoosts,
            preFilterSearchShards,
            threadPool,
            clusters,
            searchRequestAttributes
        );
    }

    Executor asyncSearchExecutor(final String[] indices) {
        boolean seenSystem = false;
        boolean seenCritical = false;
        for (String index : indices) {
            final String executorName = executorSelector.executorForSearch(index);
            switch (executorName) {
                case SYSTEM_READ -> seenSystem = true;
                case SYSTEM_CRITICAL_READ -> seenCritical = true;
                default -> {
                    return threadPool.executor(executorName);
                }
            }
        }
        final String executor;
        if (seenSystem == false && seenCritical) {
            executor = SYSTEM_CRITICAL_READ;
        } else if (seenSystem) {
            executor = SYSTEM_READ;
        } else {
            executor = ThreadPool.Names.SEARCH;
        }
        return threadPool.executor(executor);
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
        ProjectState projectState,
        SearchRequest searchRequest,
        String[] indices,
        int numShards,
        int defaultPreFilterShardSize
    ) {
        if (searchRequest.searchType() != QUERY_THEN_FETCH) {
            // we can't do this for DFS it needs to fan out to all shards all the time
            return false;
        }
        SearchSourceBuilder source = searchRequest.source();
        Integer preFilterShardSize = searchRequest.getPreFilterShardSize();
        if (preFilterShardSize == null) {
            if (hasReadOnlyIndices(indices, projectState) || hasPrimaryFieldSort(source)) {
                preFilterShardSize = 1;
            } else {
                preFilterShardSize = defaultPreFilterShardSize;
            }
        }
        return preFilterShardSize < numShards && (SearchService.canRewriteToMatchNone(source) || hasPrimaryFieldSort(source));
    }

    private static boolean hasReadOnlyIndices(String[] indices, ProjectState projectState) {
        var blocks = projectState.blocks();
        if (blocks.global(projectState.projectId()).isEmpty() && blocks.indices(projectState.projectId()).isEmpty()) {
            // short circuit optimization because block check below is relatively expensive for many indices
            return false;
        }
        for (String index : indices) {
            ClusterBlockException writeBlock = blocks.indexBlockedException(projectState.projectId(), ClusterBlockLevel.WRITE, index);
            if (writeBlock != null) {
                return true;
            }
        }
        return false;
    }

    // package private for testing
    static List<SearchShardIterator> mergeShardsIterators(
        List<SearchShardIterator> localShardIterators,
        List<SearchShardIterator> remoteShardIterators
    ) {
        final List<SearchShardIterator> shards;
        if (remoteShardIterators.isEmpty()) {
            shards = localShardIterators;
        } else {
            shards = CollectionUtils.concatLists(remoteShardIterators, localShardIterators);
        }
        shards.sort(SearchShardIterator::compareTo);
        return shards;
    }

    interface SearchPhaseProvider {
        void runNewSearchPhase(
            SearchTask task,
            SearchRequest searchRequest,
            Executor executor,
            List<SearchShardIterator> shardIterators,
            SearchTimeProvider timeProvider,
            BiFunction<String, String, Transport.Connection> connectionLookup,
            ClusterState clusterState,
            Map<String, AliasFilter> aliasFilter,
            Map<String, Float> concreteIndexBoosts,
            boolean preFilter,
            ThreadPool threadPool,
            SearchResponse.Clusters clusters,
            Map<String, Object> searchRequestAttributes
        );
    }

    private class AsyncSearchActionProvider implements SearchPhaseProvider {
        private final ActionListener<SearchResponse> listener;

        AsyncSearchActionProvider(ActionListener<SearchResponse> listener) {
            this.listener = listener;
        }

        @Override
        public void runNewSearchPhase(
            SearchTask task,
            SearchRequest searchRequest,
            Executor executor,
            List<SearchShardIterator> shardIterators,
            SearchTimeProvider timeProvider,
            BiFunction<String, String, Transport.Connection> connectionLookup,
            ClusterState clusterState,
            Map<String, AliasFilter> aliasFilter,
            Map<String, Float> concreteIndexBoosts,
            boolean preFilter,
            ThreadPool threadPool,
            SearchResponse.Clusters clusters,
            Map<String, Object> searchRequestAttributes
        ) {
            if (preFilter) {
                // only for aggs we need to contact shards even if there are no matches
                boolean requireAtLeastOneMatch = searchRequest.source() != null && searchRequest.source().aggregations() != null;
                CanMatchPreFilterSearchPhase.execute(
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
                    requireAtLeastOneMatch,
                    searchService.getCoordinatorRewriteContextProvider(timeProvider::absoluteStartMillis),
                    searchResponseMetrics,
                    searchRequestAttributes
                )
                    .addListener(
                        listener.delegateFailureAndWrap(
                            (l, iters) -> runNewSearchPhase(
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
                                clusters,
                                searchRequestAttributes
                            )
                        )
                    );
                return;
            }
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
            boolean success = false;
            try {
                final AbstractSearchAsyncAction<?> searchPhase;
                if (searchRequest.searchType() == DFS_QUERY_THEN_FETCH) {
                    searchPhase = new SearchDfsQueryThenFetchAsyncAction(
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
                        clusters,
                        client,
                        searchResponseMetrics,
                        searchRequestAttributes
                    );
                } else {
                    assert searchRequest.searchType() == QUERY_THEN_FETCH : searchRequest.searchType();
                    searchPhase = new SearchQueryThenFetchAsyncAction(
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
                        clusters,
                        client,
                        searchService.batchQueryPhase(),
                        searchResponseMetrics,
                        searchRequestAttributes
                    );
                }
                success = true;
                searchPhase.start();
            } finally {
                if (success == false) {
                    queryResultConsumer.close();
                }
            }
        }
    }

    private static void validateAndResolveWaitForCheckpoint(
        ProjectState projectState,
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
                resolved = resolver.concreteSingleIndex(projectState.cluster(), new IndicesRequest() {
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
            IndexMetadata indexMetadata = projectState.metadata().index(index);
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

    /**
     * {@link ActionListener} suitable for collecting cross-cluster responses.
     * @param <Response> Response type we're getting as intermediate per-cluster results.
     * @param <FinalResponse> Response type that the final listener expects.
     */
    abstract static class CCSActionListener<Response, FinalResponse> implements ActionListener<Response> {
        protected final String clusterAlias;
        protected final boolean skipOnFailure;
        private final CountDown countDown;
        private final AtomicReference<Exception> exceptions;
        protected final SearchResponse.Clusters clusters;
        private final ActionListener<FinalResponse> originalListener;

        /**
         * Used by both minimize_roundtrips true and false
         */
        CCSActionListener(
            String clusterAlias,
            boolean skipOnFailure,
            CountDown countDown,
            AtomicReference<Exception> exceptions,
            SearchResponse.Clusters clusters,
            ActionListener<FinalResponse> originalListener
        ) {
            this.clusterAlias = clusterAlias;
            this.skipOnFailure = skipOnFailure;
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

        /**
         * Specific listener type will implement this method to process its specific partial response.
         */
        abstract void innerOnResponse(Response response);

        @Override
        public final void onFailure(Exception e) {
            ShardSearchFailure f = new ShardSearchFailure(e);
            logCCSError(f, clusterAlias, skipOnFailure);
            SearchResponse.Cluster cluster = clusters.getCluster(clusterAlias);
            if (skipOnFailure && ExceptionsHelper.isTaskCancelledException(e) == false) {
                if (cluster != null) {
                    ccsClusterInfoUpdate(f, clusters, clusterAlias, true);
                }
            } else {
                if (cluster != null) {
                    ccsClusterInfoUpdate(f, clusters, clusterAlias, false);
                }
                Exception exception = e;
                if (RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY.equals(clusterAlias) == false
                    && ExceptionsHelper.isTaskCancelledException(e) == false) {
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
     * @param shouldSkipOnFailure the shouldSkipOnFailure setting of the cluster with the search error
     */
    private static void logCCSError(ShardSearchFailure f, String clusterAlias, boolean shouldSkipOnFailure) {
        String errorInfo;
        try {
            errorInfo = Strings.toString(f.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS));
        } catch (IOException ex) {
            // use the toString as a fallback if for some reason the XContent conversion to JSON fails
            errorInfo = f.toString();
        }
        logger.debug(
            "CCS remote cluster failure. Cluster [{}]. shouldSkipOnFailure: [{}]. Error: {}",
            clusterAlias,
            shouldSkipOnFailure,
            errorInfo
        );
    }

    private static RemoteTransportException wrapRemoteClusterFailure(String clusterAlias, Exception e) {
        return new RemoteTransportException("error while communicating with remote cluster [" + clusterAlias + "]", e);
    }

    static List<SearchShardIterator> getLocalShardsIteratorFromPointInTime(
        ProjectState projectState,
        IndicesOptions indicesOptions,
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
                if (perNode.getNode() != null) {
                    // If the shard was available when the PIT was created, it's included.
                    // Otherwise, we add the shard iterator without a target node, allowing a partial search failure to
                    // be thrown when a search phase attempts to access it.
                    try {
                        final ShardIterator shards = OperationRouting.getShards(projectState.routingTable(), shardId);
                        // Prefer executing shard requests on nodes that are part of PIT first.
                        if (projectState.cluster().nodes().nodeExists(perNode.getNode())) {
                            targetNodes.add(perNode.getNode());
                        } else {
                            logger.debug(
                                "Node [{}] referenced in PIT context id [{}] no longer exists.",
                                perNode.getNode(),
                                perNode.getSearchContextId()
                            );
                        }
                        ShardSearchContextId shardSearchContextId = perNode.getSearchContextId();
                        if (shardSearchContextId.isRetryable()) {
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
                }
                OriginalIndices finalIndices = new OriginalIndices(new String[] { shardId.getIndexName() }, indicesOptions);
                iterators.add(
                    new SearchShardIterator(
                        localClusterAlias,
                        shardId,
                        targetNodes,
                        finalIndices,
                        perNode.getSearchContextId(),
                        keepAlive,
                        false,
                        false,
                        // This parameter is specific to the resharding feature.
                        // It is used when creating a searcher to apply filtering needed to have correct search results
                        // while resharding is in progress.
                        // In context of PIT the searcher is reused or can be recreated only in read-only scenarios.
                        // If a searcher is reused, this value won't be used
                        // (it was calculated and used when PIT was created).
                        // If the searcher is recreated, which will happen when PITs are relocatable, then we should supply
                        // the same summary when recreating the search that was first used in order to produce the same
                        // filtering decision. See ES-13264 for the work needed when relocatable PITs land.
                        SplitShardCountSummary.UNSET
                    )
                );
            }
        }
        return iterators;
    }

    /**
     * Create a list of {@link SearchShardIterator}s for the local indices we are searching.
     * This resolves aliases and index expressions.
     */
    List<SearchShardIterator> getLocalShardsIterator(
        ProjectState projectState,
        SearchRequest searchRequest,
        String clusterAlias,
        Set<ResolvedExpression> indicesAndAliases,
        String[] concreteIndices
    ) {
        concreteIndices = ignoreBlockedIndices(projectState, concreteIndices);
        var routingMap = indexNameExpressionResolver.resolveSearchRouting(
            projectState.metadata(),
            searchRequest.routing(),
            searchRequest.indices()
        );
        List<SearchShardRouting> shardRoutings = clusterService.operationRouting()
            .searchShards(
                projectState,
                concreteIndices,
                routingMap,
                searchRequest.preference(),
                responseCollectorService,
                searchTransportService.getPendingSearchRequests()
            );
        final Map<String, OriginalIndices> originalIndices = buildPerIndexOriginalIndices(
            projectState,
            indicesAndAliases,
            concreteIndices,
            searchRequest.indicesOptions()
        );
        SearchShardIterator[] list = new SearchShardIterator[shardRoutings.size()];
        int i = 0;
        for (SearchShardRouting shardRouting : shardRoutings) {
            final ShardId shardId = shardRouting.shardId();
            OriginalIndices finalIndices = originalIndices.get(shardId.getIndex().getName());
            assert finalIndices != null;
            list[i++] = new SearchShardIterator(
                clusterAlias,
                shardId,
                shardRouting.getShardRoutings(),
                finalIndices,
                shardRouting.reshardSplitShardCountSummary()
            );
        }
        // the returned list must support in-place sorting, so this is the most memory efficient we can do here
        return Arrays.asList(list);
    }

    static String[] ignoreBlockedIndices(ProjectState projectState, String[] concreteIndices) {
        // optimization: mostly we do not have any blocks so there's no point in the expensive per-index checking
        boolean hasIndexBlocks = projectState.blocks().indices(projectState.projectId()).isEmpty() == false;
        if (hasIndexBlocks) {
            return Arrays.stream(concreteIndices)
                .filter(
                    index -> projectState.blocks()
                        .hasIndexBlock(projectState.projectId(), index, IndexMetadata.INDEX_REFRESH_BLOCK) == false
                )
                .toArray(String[]::new);
        }
        return concreteIndices;

    }

    private static class SearchTelemetryListener extends DelegatingActionListener<SearchResponse, SearchResponse> {
        private final CCSUsage.Builder usageBuilder;
        private final SearchResponseMetrics searchResponseMetrics;
        private final long nowInMillis;
        private final UsageService usageService;
        private final boolean collectCCSTelemetry;
        private final Map<String, Object> searchRequestAttributes;

        SearchTelemetryListener(
            ActionListener<SearchResponse> listener,
            SearchResponseMetrics searchResponseMetrics,
            Map<String, Object> searchRequestAttributes,
            long nowInMillis,
            UsageService usageService,
            CCSUsage.Builder usageBuilder
        ) {
            super(listener);
            this.searchResponseMetrics = searchResponseMetrics;
            this.searchRequestAttributes = searchRequestAttributes;
            this.nowInMillis = nowInMillis;
            this.collectCCSTelemetry = true;
            this.usageService = usageService;
            this.usageBuilder = usageBuilder;
        }

        SearchTelemetryListener(
            ActionListener<SearchResponse> listener,
            SearchResponseMetrics searchResponseMetrics,
            Map<String, Object> searchRequestAttributes,
            long nowInMillis
        ) {
            super(listener);
            this.searchResponseMetrics = searchResponseMetrics;
            this.searchRequestAttributes = searchRequestAttributes;
            this.nowInMillis = nowInMillis;
            this.collectCCSTelemetry = false;
            this.usageService = null;
            this.usageBuilder = null;
        }

        @Override
        public void onResponse(SearchResponse searchResponse) {
            try {
                searchResponseMetrics.recordTookTime(
                    searchResponse.getTookInMillis(),
                    searchResponse.getTimeRangeFilterFromMillis(),
                    nowInMillis,
                    searchRequestAttributes
                );
                SearchResponseMetrics.ResponseCountTotalStatus responseCountTotalStatus =
                    SearchResponseMetrics.ResponseCountTotalStatus.SUCCESS;
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
                            responseCountTotalStatus = SearchResponseMetrics.ResponseCountTotalStatus.PARTIAL_FAILURE;
                        }
                    }
                }
                searchResponseMetrics.incrementResponseCount(responseCountTotalStatus, searchRequestAttributes);

                if (collectCCSTelemetry) {
                    extractCCSTelemetry(searchResponse);
                    recordTelemetry();
                }
            } catch (Exception e) {
                onFailure(e);
                return;
            }
            // This is last because we want to collect telemetry before returning the response.
            delegate.onResponse(searchResponse);
        }

        @Override
        public void onFailure(Exception e) {
            searchResponseMetrics.incrementResponseCount(SearchResponseMetrics.ResponseCountTotalStatus.FAILURE, searchRequestAttributes);
            if (collectCCSTelemetry) {
                usageBuilder.setFailure(e);
                recordTelemetry();
            }
            super.onFailure(e);
        }

        private void recordTelemetry() {
            usageService.getCcsUsageHolder().updateUsage(usageBuilder.build());
        }

        /**
         * Extract telemetry data from the search response.
         * @param searchResponse The final response from the search.
         */
        private void extractCCSTelemetry(SearchResponse searchResponse) {
            usageBuilder.took(searchResponse.getTookInMillis());
            for (String clusterAlias : searchResponse.getClusters().getClusterAliases()) {
                SearchResponse.Cluster cluster = searchResponse.getClusters().getCluster(clusterAlias);
                if (cluster.getStatus() == SearchResponse.Cluster.Status.SKIPPED) {
                    usageBuilder.skippedRemote(clusterAlias);
                } else {
                    usageBuilder.perClusterUsage(clusterAlias, cluster.getTook());
                }
            }
        }
    }
}
