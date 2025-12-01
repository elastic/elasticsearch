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
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.ResolvedIndexExpression;
import org.elasticsearch.action.ResolvedIndexExpressions;
import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.search.SearchResponseMetrics;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.crossproject.CrossProjectIndexResolutionValidator;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.search.crossproject.CrossProjectIndexResolutionValidator.indicesOptionsForCrossProjectFanout;
import static org.elasticsearch.transport.RemoteClusterAware.buildRemoteIndexName;

public class TransportOpenPointInTimeAction extends HandledTransportAction<OpenPointInTimeRequest, OpenPointInTimeResponse> {

    private static final Logger logger = LogManager.getLogger(TransportOpenPointInTimeAction.class);

    public static final String OPEN_SHARD_READER_CONTEXT_NAME = "indices:data/read/open_reader_context";
    public static final ActionType<OpenPointInTimeResponse> TYPE = new ActionType<>("indices:data/read/open_point_in_time");

    private final TransportSearchAction transportSearchAction;
    private final SearchTransportService searchTransportService;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final TransportService transportService;
    private final SearchService searchService;
    private final ClusterService clusterService;
    private final SearchResponseMetrics searchResponseMetrics;
    private final CrossProjectModeDecider crossProjectModeDecider;
    private final TimeValue forceConnectTimeoutSecs;

    @Inject
    public TransportOpenPointInTimeAction(
        TransportService transportService,
        SearchService searchService,
        ActionFilters actionFilters,
        TransportSearchAction transportSearchAction,
        SearchTransportService searchTransportService,
        NamedWriteableRegistry namedWriteableRegistry,
        ClusterService clusterService,
        SearchResponseMetrics searchResponseMetrics
    ) {
        super(TYPE.name(), transportService, actionFilters, OpenPointInTimeRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.transportService = transportService;
        this.transportSearchAction = transportSearchAction;
        this.searchService = searchService;
        this.searchTransportService = searchTransportService;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.clusterService = clusterService;
        this.searchResponseMetrics = searchResponseMetrics;
        this.crossProjectModeDecider = new CrossProjectModeDecider(clusterService.getSettings());
        this.forceConnectTimeoutSecs = clusterService.getSettings()
            .getAsTime("search.ccs.force_connect_timeout", TimeValue.timeValueSeconds(3L));
        transportService.registerRequestHandler(
            OPEN_SHARD_READER_CONTEXT_NAME,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            ShardOpenReaderRequest::new,
            new ShardOpenReaderRequestHandler()
        );
        TransportActionProxy.registerProxyAction(
            transportService,
            OPEN_SHARD_READER_CONTEXT_NAME,
            false,
            ShardOpenReaderResponse::new,
            namedWriteableRegistry
        );
    }

    @Override
    protected void doExecute(Task task, OpenPointInTimeRequest request, ActionListener<OpenPointInTimeResponse> listener) {
        final ClusterState clusterState = clusterService.state();
        // Check if all the nodes in this cluster know about the service
        if (request.allowPartialSearchResults() && clusterState.getMinTransportVersion().before(TransportVersions.V_8_16_0)) {
            listener.onFailure(
                new ElasticsearchStatusException(
                    format(
                        "The [allow_partial_search_results] parameter cannot be used while the cluster is still upgrading. "
                            + "Please wait until the upgrade is fully completed and try again."
                    ),
                    RestStatus.BAD_REQUEST
                )
            );
            return;
        }

        final boolean resolveCrossProject = crossProjectModeDecider.resolvesCrossProject(request);
        if (resolveCrossProject) {
            executeOpenPitCrossProject((SearchTask) task, request, listener);
        } else {
            executeOpenPit((SearchTask) task, request, listener);
        }
    }

    private void executeOpenPitCrossProject(
        SearchTask task,
        OpenPointInTimeRequest request,
        ActionListener<OpenPointInTimeResponse> listener
    ) {
        String[] indices = request.indices();
        IndicesOptions originalIndicesOptions = request.indicesOptions();
        // in CPS before executing the open pit request we need to get index resolution and possibly throw based on merged project view
        // rules. This should happen only if either ignore_unavailable or allow_no_indices is set to false (strict).
        // If instead both are true we can continue with the "normal" pit execution.
        if (originalIndicesOptions.ignoreUnavailable() && originalIndicesOptions.allowNoIndices()) {
            // lenient indicesOptions thus execute standard pit
            executeOpenPit(task, request, listener);
            return;
        }

        // ResolvedIndexExpression for the origin cluster (only) as determined by the Security Action Filter
        final ResolvedIndexExpressions localResolvedIndexExpressions = request.getResolvedIndexExpressions();

        RemoteClusterService remoteClusterService = searchTransportService.getRemoteClusterService();
        final Map<String, OriginalIndices> indicesPerCluster = remoteClusterService.groupIndices(
            indicesOptionsForCrossProjectFanout(originalIndicesOptions),
            indices
        );
        // local indices resolution was already taken care of by the Security Action Filter
        indicesPerCluster.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);

        if (indicesPerCluster.isEmpty()) {
            // for CPS requests that are targeting origin only, could be because of project_routing or other reasons, execute standard pit.
            final Exception ex = CrossProjectIndexResolutionValidator.validate(
                originalIndicesOptions,
                request.getProjectRouting(),
                localResolvedIndexExpressions,
                Map.of()
            );
            if (ex != null) {
                listener.onFailure(ex);
                return;
            }
            executeOpenPit(task, request, listener);
            return;
        }

        // CPS
        final int linkedProjectsToQuery = indicesPerCluster.size();
        ActionListener<Collection<Map.Entry<String, ResolveIndexAction.Response>>> responsesListener = listener.delegateFailureAndWrap(
            (l, responses) -> {
                Map<String, ResolvedIndexExpressions> resolvedRemoteExpressions = responses.stream()
                    .filter(e -> e.getValue().getResolvedIndexExpressions() != null)
                    .collect(
                        Collectors.toMap(
                            Map.Entry::getKey,
                            e -> e.getValue().getResolvedIndexExpressions()

                        )
                    );
                final Exception ex = CrossProjectIndexResolutionValidator.validate(
                    originalIndicesOptions,
                    request.getProjectRouting(),
                    localResolvedIndexExpressions,
                    resolvedRemoteExpressions
                );
                if (ex != null) {
                    listener.onFailure(ex);
                    return;
                }
                Set<String> collectedIndices = new HashSet<>(indices.length);

                for (Map.Entry<String, ResolvedIndexExpressions> resolvedRemoteExpressionEntry : resolvedRemoteExpressions.entrySet()) {
                    String remoteAlias = resolvedRemoteExpressionEntry.getKey();
                    for (ResolvedIndexExpression expression : resolvedRemoteExpressionEntry.getValue().expressions()) {
                        ResolvedIndexExpression.LocalExpressions oneRemoteExpression = expression.localExpressions();
                        if (false == oneRemoteExpression.indices().isEmpty()
                            && oneRemoteExpression
                                .localIndexResolutionResult() == ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS) {
                            collectedIndices.addAll(
                                oneRemoteExpression.indices()
                                    .stream()
                                    .map(i -> buildRemoteIndexName(remoteAlias, i))
                                    .collect(Collectors.toSet())
                            );
                        }
                    }
                }
                if (localResolvedIndexExpressions != null) { // this should never be null in CPS
                    collectedIndices.addAll(localResolvedIndexExpressions.getLocalIndicesList());
                }
                request.indices(collectedIndices.toArray(String[]::new));
                executeOpenPit(task, request, listener);
            }
        );
        ActionListener<Map.Entry<String, ResolveIndexAction.Response>> groupedListener = new GroupedActionListener<>(
            linkedProjectsToQuery,
            responsesListener
        );

        // make CPS calls
        for (Map.Entry<String, OriginalIndices> remoteClusterIndices : indicesPerCluster.entrySet()) {
            String clusterAlias = remoteClusterIndices.getKey();
            OriginalIndices originalIndices = remoteClusterIndices.getValue();
            IndicesOptions relaxedFanoutIdxOptions = originalIndices.indicesOptions(); // from indicesOptionsForCrossProjectFanout
            ResolveIndexAction.Request remoteRequest = new ResolveIndexAction.Request(originalIndices.indices(), relaxedFanoutIdxOptions);

            SubscribableListener<Transport.Connection> connectionListener = new SubscribableListener<>();
            connectionListener.addTimeout(forceConnectTimeoutSecs, transportService.getThreadPool(), EsExecutors.DIRECT_EXECUTOR_SERVICE);

            connectionListener.addListener(groupedListener.delegateResponse((l, failure) -> {
                logger.info("failed to resolve indices on remote cluster [" + clusterAlias + "]", failure);
                l.onFailure(failure);
            })
                .delegateFailure(
                    (ignored, connection) -> transportService.sendRequest(
                        connection,
                        ResolveIndexAction.REMOTE_TYPE.name(),
                        remoteRequest,
                        TransportRequestOptions.EMPTY,
                        new ActionListenerResponseHandler<>(groupedListener.delegateResponse((l, failure) -> {
                            logger.info("Error occurred on remote cluster [" + clusterAlias + "]", failure);
                            l.onFailure(failure);
                        }).map(resolveIndexResponse -> Map.entry(clusterAlias, resolveIndexResponse)),
                            ResolveIndexAction.Response::new,
                            EsExecutors.DIRECT_EXECUTOR_SERVICE
                        )
                    )
                ));

            remoteClusterService.maybeEnsureConnectedAndGetConnection(clusterAlias, true, connectionListener);
        }
    }

    private void executeOpenPit(SearchTask task, OpenPointInTimeRequest request, ActionListener<OpenPointInTimeResponse> listener) {
        final SearchRequest searchRequest = new SearchRequest().indices(request.indices())
            .indicesOptions(request.indicesOptions())
            .preference(request.preference())
            .routing(request.routing())
            .allowPartialSearchResults(request.allowPartialSearchResults())
            .source(new SearchSourceBuilder().query(request.indexFilter()));
        searchRequest.setMaxConcurrentShardRequests(request.maxConcurrentShardRequests());
        searchRequest.setCcsMinimizeRoundtrips(false);

        transportSearchAction.executeOpenPit(task, searchRequest, listener.map(r -> {
            assert r.pointInTimeId() != null : r;
            return new OpenPointInTimeResponse(
                r.pointInTimeId(),
                r.getTotalShards(),
                r.getSuccessfulShards(),
                r.getFailedShards(),
                r.getSkippedShards()
            );
        }), searchListener -> new OpenPointInTimePhase(request, searchListener));
    }

    private final class OpenPointInTimePhase implements TransportSearchAction.SearchPhaseProvider {
        private final OpenPointInTimeRequest pitRequest;
        private final ActionListener<SearchResponse> listener;

        OpenPointInTimePhase(OpenPointInTimeRequest pitRequest, ActionListener<SearchResponse> listener) {
            this.pitRequest = pitRequest;
            this.listener = listener;
        }

        @Override
        public void runNewSearchPhase(
            SearchTask task,
            SearchRequest searchRequest,
            Executor executor,
            List<SearchShardIterator> shardIterators,
            TransportSearchAction.SearchTimeProvider timeProvider,
            BiFunction<String, String, Transport.Connection> connectionLookup,
            ClusterState clusterState,
            Map<String, AliasFilter> aliasFilter,
            Map<String, Float> concreteIndexBoosts,
            boolean preFilter,
            ThreadPool threadPool,
            SearchResponse.Clusters clusters,
            Map<String, Object> searchRequestAttributes
        ) {
            // Note: remote shards are prefiltered via can match as part of search shards. They don't need additional pre-filtering and
            // that is signaled to the local can match through the SearchShardIterator#prefiltered flag. Local shards do need to go
            // through the local can match phase.
            if (SearchService.canRewriteToMatchNone(searchRequest.source())) {
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
                    false,
                    searchService.getCoordinatorRewriteContextProvider(timeProvider::absoluteStartMillis),
                    searchResponseMetrics,
                    searchRequestAttributes
                )
                    .addListener(
                        listener.delegateFailureAndWrap(
                            (searchResponseActionListener, searchShardIterators) -> runOpenPointInTimePhase(
                                task,
                                searchRequest,
                                executor,
                                searchShardIterators,
                                timeProvider,
                                connectionLookup,
                                clusterState,
                                aliasFilter,
                                concreteIndexBoosts,
                                clusters,
                                searchRequestAttributes
                            )
                        )
                    );
            } else {
                runOpenPointInTimePhase(
                    task,
                    searchRequest,
                    executor,
                    shardIterators,
                    timeProvider,
                    connectionLookup,
                    clusterState,
                    aliasFilter,
                    concreteIndexBoosts,
                    clusters,
                    searchRequestAttributes
                );
            }
        }

        void runOpenPointInTimePhase(
            SearchTask task,
            SearchRequest searchRequest,
            Executor executor,
            List<SearchShardIterator> shardIterators,
            TransportSearchAction.SearchTimeProvider timeProvider,
            BiFunction<String, String, Transport.Connection> connectionLookup,
            ClusterState clusterState,
            Map<String, AliasFilter> aliasFilter,
            Map<String, Float> concreteIndexBoosts,
            SearchResponse.Clusters clusters,
            Map<String, Object> searchRequestAttributes
        ) {
            assert searchRequest.getMaxConcurrentShardRequests() == pitRequest.maxConcurrentShardRequests()
                : searchRequest.getMaxConcurrentShardRequests() + " != " + pitRequest.maxConcurrentShardRequests();
            TransportVersion minTransportVersion = clusterState.getMinTransportVersion();
            new AbstractSearchAsyncAction<>(
                "open_pit",
                logger,
                namedWriteableRegistry,
                searchTransportService,
                connectionLookup,
                aliasFilter,
                concreteIndexBoosts,
                executor,
                searchRequest,
                listener,
                shardIterators,
                timeProvider,
                clusterState,
                task,
                new ArraySearchPhaseResults<>(shardIterators.size()),
                searchRequest.getMaxConcurrentShardRequests(),
                clusters,
                searchResponseMetrics,
                searchRequestAttributes
            ) {
                @Override
                protected void executePhaseOnShard(
                    SearchShardIterator shardIt,
                    Transport.Connection connection,
                    SearchActionListener<SearchPhaseResult> phaseListener
                ) {
                    transportService.sendChildRequest(
                        connection,
                        OPEN_SHARD_READER_CONTEXT_NAME,
                        new ShardOpenReaderRequest(shardIt.shardId(), shardIt.getOriginalIndices(), pitRequest.keepAlive()),
                        task,
                        new ActionListenerResponseHandler<>(
                            phaseListener,
                            ShardOpenReaderResponse::new,
                            TransportResponseHandler.TRANSPORT_WORKER
                        )
                    );
                }

                @Override
                protected SearchPhase getNextPhase() {
                    return new SearchPhase(getName()) {
                        @Override
                        protected void run() {
                            sendSearchResponse(SearchResponseSections.EMPTY_WITH_TOTAL_HITS, results.getAtomicArray());
                        }
                    };
                }

                @Override
                protected BytesReference buildSearchContextId(ShardSearchFailure[] failures) {
                    return SearchContextId.encode(results.getAtomicArray().asList(), aliasFilter, minTransportVersion, failures);
                }
            }.start();
        }
    }

    private static final class ShardOpenReaderRequest extends AbstractTransportRequest implements IndicesRequest {
        final ShardId shardId;
        final OriginalIndices originalIndices;
        final TimeValue keepAlive;

        ShardOpenReaderRequest(ShardId shardId, OriginalIndices originalIndices, TimeValue keepAlive) {
            this.shardId = shardId;
            this.originalIndices = originalIndices;
            this.keepAlive = keepAlive;
        }

        ShardOpenReaderRequest(StreamInput in) throws IOException {
            super(in);
            shardId = new ShardId(in);
            originalIndices = OriginalIndices.readOriginalIndices(in);
            keepAlive = in.readTimeValue();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            OriginalIndices.writeOriginalIndices(originalIndices, out);
            out.writeTimeValue(keepAlive);
        }

        public ShardId getShardId() {
            return shardId;
        }

        @Override
        public String[] indices() {
            return originalIndices.indices();
        }

        @Override
        public IndicesOptions indicesOptions() {
            return originalIndices.indicesOptions();
        }
    }

    private static final class ShardOpenReaderResponse extends SearchPhaseResult {
        ShardOpenReaderResponse(ShardSearchContextId contextId) {
            this.contextId = contextId;
        }

        ShardOpenReaderResponse(StreamInput in) throws IOException {
            contextId = new ShardSearchContextId(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            contextId.writeTo(out);
        }
    }

    private class ShardOpenReaderRequestHandler implements TransportRequestHandler<ShardOpenReaderRequest> {
        @Override
        public void messageReceived(ShardOpenReaderRequest request, TransportChannel channel, Task task) {
            searchService.openReaderContext(
                request.getShardId(),
                request.keepAlive,
                new ChannelActionListener<>(channel).map(ShardOpenReaderResponse::new)
            );
        }
    }
}
