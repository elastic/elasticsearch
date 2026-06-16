/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.transform.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.ResolvedIndexExpression;
import org.elasticsearch.action.ResolvedIndexExpressions;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchShardsGroup;
import org.elasticsearch.action.search.SearchShardsRequest;
import org.elasticsearch.action.search.SearchShardsResponse;
import org.elasticsearch.action.search.TransportSearchShardsAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.search.crossproject.CrossProjectIndexResolutionValidator;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointAction;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointAction.Request;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointAction.Response;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointNodeAction;
import org.elasticsearch.xpack.transform.TransformServices;

import java.time.Clock;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.search.crossproject.CrossProjectIndexResolutionValidator.indicesOptionsForCrossProjectFanout;

public class TransportGetCheckpointAction extends HandledTransportAction<Request, Response> {

    /**
     * Per-cluster result from a GetCheckpoint fan-out leg: at most one of {@code response} and
     * {@code exception} is non-null.
     */
    private record ClusterCheckpointResult(Response response, Exception exception) {
        ClusterCheckpointResult {
            assert response == null || exception == null : "ClusterCheckpointResult cannot hold both a response and an exception";
        }

        ClusterCheckpointResult(Response response) {
            this(response, null);
        }

        ClusterCheckpointResult(Exception exception) {
            this(null, exception);
        }
    }

    private static final Logger logger = LogManager.getLogger(TransportGetCheckpointAction.class);
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final TransportService transportService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final Client client;
    private final CrossProjectModeDecider crossProjectModeDecider;
    private final RemoteClusterService remoteClusterService;
    private final ProjectResolver projectResolver;

    @Inject
    public TransportGetCheckpointAction(
        final TransportService transportService,
        final ActionFilters actionFilters,
        final IndicesService indicesService,
        final ClusterService clusterService,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final Client client,
        final TransformServices transformServices,
        final ProjectResolver projectResolver
    ) {
        super(GetCheckpointAction.NAME, transportService, actionFilters, Request::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.transportService = transportService;
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.client = client;
        this.crossProjectModeDecider = transformServices.crossProjectModeDecider();
        this.remoteClusterService = transportService.getRemoteClusterService();
        this.projectResolver = projectResolver;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        ClusterState clusterState = clusterService.state();

        var remoteClusterIndices = remoteClusterService.groupIndices(
            crossProjectModeDecider.resolvesCrossProject(request)
                ? indicesOptionsForCrossProjectFanout(request.indicesOptions())
                : request.indicesOptions(),
            request.indices()
        );

        if (Assertions.ENABLED) {
            assert remoteClusterIndices.size() > 0 : "We assume there is always at least the local cluster group.";
            var visited = new HashSet<String>();
            remoteClusterIndices.forEach((cluster, ignored) -> {
                var uniqueCluster = visited.add(cluster);
                assert uniqueCluster : "must only have one cluster grouped by remoteClusterService.groupIndices";
            });
        }

        fanOut(task, request, clusterState, remoteClusterIndices, listener);
    }

    private void fanOut(
        Task task,
        Request request,
        ClusterState clusterState,
        Map<String, OriginalIndices> remoteClusterIndices,
        ActionListener<Response> listener
    ) {
        var localIndices = remoteClusterIndices.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);

        // we only need the response's resolved indices if we are cross-project, otherwise we ignore it
        var includeResolvedIndexExpressions = crossProjectModeDecider.resolvesCrossProject(request);

        int numRemote = remoteClusterIndices.size();

        if (numRemote == 0) {
            // Only local indices; no fan-out needed.
            assert localIndices != null;
            resolveLocalCheckpoints(
                task,
                request,
                clusterState,
                localIndices,
                includeResolvedIndexExpressions,
                listener.delegateFailureAndWrap(
                    (l, response) -> validateAndMergeResponses(
                        request,
                        Map.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, response),
                        Map.of(),
                        l
                    )
                )
            );
            return;
        }

        // Multiple clusters: collect one ClusterCheckpointResult per leg. Failures are wrapped in the
        // result rather than aborting the group (mirrors TransportSearchAction.collectRemoteResolvedIndices),
        // then forwarded to CrossProjectIndexResolutionValidator. The local leg failure is fatal.
        int numClusters = numRemote + (localIndices != null ? 1 : 0);
        var groupedListener = new GroupedActionListener<Map.Entry<String, ClusterCheckpointResult>>(
            numClusters,
            listener.delegateFailureAndWrap((l, results) -> {
                Map<String, Response> responsesByCluster = new HashMap<>(numClusters);
                Map<String, Exception> remoteExceptions = new HashMap<>(numRemote);
                Exception localErr = null;
                for (var entry : results) {
                    var result = entry.getValue();
                    if (result.exception() != null) {
                        if (RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY.equals(entry.getKey())) {
                            localErr = result.exception();
                        } else {
                            remoteExceptions.put(entry.getKey(), result.exception());
                        }
                    } else {
                        responsesByCluster.put(entry.getKey(), result.response());
                    }
                }
                if (localErr != null) {
                    l.onFailure(localErr);
                } else {
                    validateAndMergeResponses(request, responsesByCluster, remoteExceptions, l);
                }
            })
        );

        if (localIndices != null) {
            resolveLocalCheckpoints(
                task,
                request,
                clusterState,
                localIndices,
                includeResolvedIndexExpressions,
                ActionListener.wrap(
                    response -> groupedListener.onResponse(
                        Map.entry(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, new ClusterCheckpointResult(response))
                    ),
                    e -> groupedListener.onResponse(Map.entry(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, new ClusterCheckpointResult(e)))
                )
            );
        }

        var nodeId = clusterState.nodes().getLocalNode().getId();
        var parentTaskId = new TaskId(nodeId, task.getId());

        remoteClusterIndices.forEach((cluster, remoteIndices) -> {
            var remoteRequest = request.rewriteRequest(
                remoteIndices.indices(),
                remoteIndices.indicesOptions(),
                cluster,
                includeResolvedIndexExpressions
            );
            remoteRequest.setParentTask(parentTaskId);
            var remoteClusterClient = remoteClusterService.getRemoteClusterClient(
                cluster,
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                RemoteClusterService.DisconnectedStrategy.RECONNECT_UNLESS_SKIP_UNAVAILABLE
            );
            remoteClusterClient.execute(
                GetCheckpointAction.REMOTE_TYPE,
                remoteRequest,
                ActionListener.wrap(
                    response -> groupedListener.onResponse(Map.entry(cluster, new ClusterCheckpointResult(response))),
                    e -> groupedListener.onResponse(Map.entry(cluster, new ClusterCheckpointResult(e)))
                )
            );
        });
    }

    private void resolveLocalCheckpoints(
        Task task,
        Request request,
        ClusterState clusterState,
        OriginalIndices localIndices,
        boolean includeResolvedIndexExpressions,
        ActionListener<Response> listener
    ) {
        String[] concreteIndices;
        if (includeResolvedIndexExpressions) {
            // For cross-project search, ResolvedIndexExpressions will have resolved our local indices already, and we will filter ones
            // that have successfully resolved before calling their shards. Any indices that did not successfully resolve are validated
            // when aggregated with all Response objects in validateAndMergeResponses().
            ResolvedIndexExpressions resolvedIndexExpressions = request.getResolvedIndexExpressions();
            if (resolvedIndexExpressions != null) {
                concreteIndices = resolvedIndexExpressions.expressions()
                    .stream()
                    .map(ResolvedIndexExpression::localExpressions)
                    .filter(
                        localExpression -> localExpression
                            .localIndexResolutionResult() == ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS
                    )
                    .map(ResolvedIndexExpression.LocalExpressions::indices)
                    .flatMap(Collection::stream)
                    .distinct()
                    .toArray(String[]::new);
            } else {
                concreteIndices = Strings.EMPTY_ARRAY;
            }
        } else {
            // note: when security is turned on, the indices are already resolved
            // TODO: do a quick check and only resolve if necessary??
            concreteIndices = this.indexNameExpressionResolver.concreteIndexNames(clusterState, new IndicesRequest() {
                @Override
                public String[] indices() {
                    return localIndices.indices();
                }

                @Override
                public IndicesOptions indicesOptions() {
                    return localIndices.indicesOptions();
                }
            });
        }

        if (concreteIndices.length > 0) {
            var localRequest = request.rewriteRequest(
                concreteIndices,
                localIndices.indicesOptions(),
                RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
                includeResolvedIndexExpressions
            );
            resolveIndicesAndGetCheckpoint(task, localRequest, clusterState, listener);
        } else {
            listener.onResponse(new Response(Map.of(), includeResolvedIndexExpressions ? request.getResolvedIndexExpressions() : null));
        }
    }

    private void validateAndMergeResponses(
        Request request,
        Map<String, Response> responsesByCluster,
        Map<String, Exception> remoteExceptions,
        ActionListener<Response> mergedResponseListener
    ) {
        // if we're calling cross-project, we have to validate all cross-project responses together with the local response
        if (crossProjectModeDecider.resolvesCrossProject(request)) {
            Map<String, ResolvedIndexExpressions> remoteResolvedExpressions = responsesByCluster.entrySet()
                .stream()
                .filter(entry -> RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY.equals(entry.getKey()) == false)
                .filter(entry -> entry.getValue().resolvedIndexExpressions() != null)
                .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue().resolvedIndexExpressions()));
            // remoteExceptions carries per-cluster failures (e.g. connection errors from linked projects).
            // The validator decides whether these surface as 404/403 or are tolerated based on project routing and
            // allow_no_indices. Checkpoints use LENIENT_EXPAND_OPEN, so the validator short-circuits in lenient mode
            // (allowNoIndices && ignoreUnavailable) and tolerates connection failures by design on the CPS path.
            var crossProjectException = CrossProjectIndexResolutionValidator.validate(
                request.indicesOptions(),
                request.getProjectRouting(),
                request.getResolvedIndexExpressions(),
                remoteResolvedExpressions,
                remoteExceptions
            );
            if (crossProjectException != null) {
                mergedResponseListener.onFailure(crossProjectException);
                return;
            }
        } else if (remoteExceptions.isEmpty() == false) {
            // Classic CCS: honor skip_unavailable. A skip_unavailable=false remote that is unreachable must fail
            // the checkpoint so the transform retries rather than producing a checkpoint over partial data.
            // skip_unavailable=true clusters are tolerated and omitted from the result. This matches
            // TransportSearchAction's shouldSkipOnFailure and the RECONNECT_UNLESS_SKIP_UNAVAILABLE dispatch above.
            // See https://github.com/elastic/elasticsearch/issues/84090.
            var fatal = firstNonSkippableRemoteFailure(
                remoteExceptions,
                cluster -> remoteClusterService.shouldSkipOnFailure(cluster, null)
            );
            if (fatal != null) {
                mergedResponseListener.onFailure(fatal);
                return;
            }
        }

        // merge responses
        var checkpointsByIndex = responsesByCluster.entrySet().stream().flatMap(responseByCluster -> {
            var cluster = responseByCluster.getKey();
            return responseByCluster.getValue().getCheckpoints().entrySet().stream().map(cbi -> {
                if (RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY.equals(cluster)) {
                    return cbi;
                } else {
                    return Map.entry(cluster + RemoteClusterService.REMOTE_CLUSTER_INDEX_SEPARATOR + cbi.getKey(), cbi.getValue());
                }
            });
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        ResolvedIndexExpressions resolvedIndexExpressions = null;
        if (request.includeResolvedIndexExpressions()) {
            var resolvedIndexExpressionList = responsesByCluster.values()
                .stream()
                .map(Response::resolvedIndexExpressions)
                .filter(Objects::nonNull)
                .map(ResolvedIndexExpressions::expressions)
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .filter(Objects::nonNull)
                .toList();
            if (resolvedIndexExpressionList.isEmpty() == false) {
                resolvedIndexExpressions = new ResolvedIndexExpressions(resolvedIndexExpressionList);
            }
        }

        mergedResponseListener.onResponse(new Response(checkpointsByIndex, resolvedIndexExpressions));
    }

    protected void resolveIndicesAndGetCheckpoint(
        Task task,
        Request request,
        ClusterState clusterState,
        ActionListener<Response> listener
    ) {
        ActionListener<Map<String, long[]>> checkpointListener = listener.map(checkpoints -> {
            var localResolvedIndexExpressions = request.includeResolvedIndexExpressions() ? request.getResolvedIndexExpressions() : null;
            return new Response(checkpoints, localResolvedIndexExpressions);
        });

        var nodeId = clusterState.nodes().getLocalNode().getId();
        var parentTaskId = new TaskId(nodeId, task.getId());

        Map<String, Set<ShardId>> nodesAndShards = resolveIndicesToPrimaryShards(
            clusterState,
            projectResolver.getProjectId(),
            request.indices()
        );
        if (nodesAndShards.isEmpty()) {
            checkpointListener.onResponse(Map.of());
            return;
        }

        if (request.getQuery() == null) {  // If there is no query, then there is no point in filtering
            getCheckpointsFromNodes(
                clusterState,
                task,
                nodesAndShards,
                new OriginalIndices(request),
                request.getTimeout(),
                checkpointListener
            );
            return;
        }

        var searchShardsRequest = new SearchShardsRequest(
            request.indices(),
            SearchRequest.DEFAULT_INDICES_OPTIONS,
            request.getQuery(),
            null,
            null,
            false,
            null  // clusterAlias: this request runs against the local TransportSearchShardsAction regardless of request.getCluster()
        );
        searchShardsRequest.setParentTask(parentTaskId);
        searchShardsRequest.setIncludeSkippedShardsInIterators(true);
        ClientHelper.executeAsyncWithOrigin(
            client,
            ClientHelper.TRANSFORM_ORIGIN,
            TransportSearchShardsAction.TYPE,
            searchShardsRequest,
            ActionListener.wrap(searchShardsResponse -> {
                Map<String, Set<ShardId>> filteredNodesAndShards = filterOutSkippedShards(nodesAndShards, searchShardsResponse);
                getCheckpointsFromNodes(
                    clusterState,
                    task,
                    filteredNodesAndShards,
                    new OriginalIndices(request),
                    request.getTimeout(),
                    checkpointListener
                );
            }, e -> {
                // search_shards API failed so we just log the error here and continue just like there was no query
                logger.atWarn().withThrowable(e).log("search_shards API failed for cluster [{}]", request.getCluster());
                logger.atTrace()
                    .withThrowable(e)
                    .log("search_shards API failed for cluster [{}], request was [{}]", request.getCluster(), searchShardsRequest);
                getCheckpointsFromNodes(
                    clusterState,
                    task,
                    nodesAndShards,
                    new OriginalIndices(request),
                    request.getTimeout(),
                    checkpointListener
                );
            })
        );
    }

    private static Map<String, Set<ShardId>> resolveIndicesToPrimaryShards(
        ClusterState clusterState,
        ProjectId projectId,
        String[] concreteIndices
    ) {
        if (concreteIndices.length == 0) {
            return Collections.emptyMap();
        }

        final DiscoveryNodes nodes = clusterState.nodes();
        Map<String, Set<ShardId>> nodesAndShards = new HashMap<>();

        ShardsIterator shardsIt = clusterState.routingTable(projectId).allShards(concreteIndices);
        for (ShardRouting shard : shardsIt) {
            // only take primary shards, which should be exactly 1, this isn't strictly necessary
            // and we should consider taking any shard copy, but then we need another way to de-dup
            if (shard.primary() == false) {
                continue;
            }
            if (shard.assignedToNode() && nodes.get(shard.currentNodeId()) != null) {
                // special case: The minimum TransportVersion in the cluster is on an old version

                String nodeId = shard.currentNodeId();
                nodesAndShards.computeIfAbsent(nodeId, k -> new HashSet<>()).add(shard.shardId());
            } else {
                throw new NoShardAvailableActionException(shard.shardId(), " no primary shards available for shard [" + shard + "]");
            }
        }
        return nodesAndShards;
    }

    /**
     * Removes (node, shard) pairs for shards flagged as skipped by the can-match phase, leaving
     * all other shards intact. Shards absent from the response are kept so that index-resolution
     * divergence between the routing table and {@code search_shards} (e.g. under cross-project or
     * security re-resolution) cannot silently drop legitimate shards from the checkpoint.
     * <p>
     * Requires the {@code search_shards} request to have been issued with
     * {@link SearchShardsRequest#setIncludeSkippedShardsInIterators(boolean)
     * includeSkippedShardsInIterators=true} so that can-match-skipped shards are returned with
     * {@link SearchShardsGroup#skipped()} set.
     *
     * @param nodesAndShards       primary shards per node from the cluster state
     * @param searchShardsResponse result of the search_shards API for the same request
     * @return map of node id to shard ids, with can-match-skipped shards removed
     */
    static Map<String, Set<ShardId>> filterOutSkippedShards(
        Map<String, Set<ShardId>> nodesAndShards,
        SearchShardsResponse searchShardsResponse
    ) {
        Map<String, Set<ShardId>> filteredNodesAndShards = new HashMap<>(nodesAndShards.size());
        // Start with a deep copy of all routing-table shards.
        for (Map.Entry<String, Set<ShardId>> entry : nodesAndShards.entrySet()) {
            filteredNodesAndShards.put(entry.getKey(), new HashSet<>(entry.getValue()));
        }
        // Remove (node, shard) pairs for shards the can-match phase flagged as skipped.
        for (SearchShardsGroup shardGroup : searchShardsResponse.getGroups()) {
            if (shardGroup.skipped()) {
                for (String allocatedNode : shardGroup.allocatedNodes()) {
                    Set<ShardId> shards = filteredNodesAndShards.get(allocatedNode);
                    if (shards != null) {
                        shards.remove(shardGroup.shardId());
                        if (shards.isEmpty()) {
                            filteredNodesAndShards.remove(allocatedNode);
                        }
                    }
                }
            }
        }
        return filteredNodesAndShards;
    }

    /**
     * Returns the first exception that should cause the checkpoint to fail given a map of per-cluster
     * remote failures, or {@code null} if every failed cluster is tolerated by {@code shouldSkip}.
     * Non-skippable failures are wrapped in a {@link RemoteTransportException} and additional ones
     * are attached as suppressed exceptions.
     * <p>
     * For classic CCS, {@code shouldSkip} is {@link RemoteClusterService#shouldSkipOnFailure} which
     * returns {@code true} when {@code skip_unavailable=true}: those clusters are omitted from the
     * checkpoint. {@code skip_unavailable=false} clusters (including the default) surface as failures
     * so the transform retries rather than checkpointing over partial data. See
     * https://github.com/elastic/elasticsearch/issues/84090.
     */
    static Exception firstNonSkippableRemoteFailure(Map<String, Exception> remoteExceptions, Predicate<String> shouldSkip) {
        Exception fatal = null;
        for (var entry : remoteExceptions.entrySet()) {
            if (shouldSkip.test(entry.getKey()) == false) {
                var wrapped = new RemoteTransportException(
                    "error while communicating with remote cluster [" + entry.getKey() + "]",
                    entry.getValue()
                );
                if (fatal == null) {
                    fatal = wrapped;
                } else {
                    fatal.addSuppressed(wrapped);
                }
            }
        }
        return fatal;
    }

    private void getCheckpointsFromNodes(
        ClusterState clusterState,
        Task task,
        Map<String, Set<ShardId>> nodesAndShards,
        OriginalIndices originalIndices,
        TimeValue timeout,
        ActionListener<Map<String, long[]>> listener
    ) {
        if (nodesAndShards.isEmpty()) {
            listener.onResponse(Map.of());
            return;
        }

        final String localNodeId = clusterService.localNode().getId();

        GroupedActionListener<GetCheckpointNodeAction.Response> groupedListener = new GroupedActionListener<>(
            nodesAndShards.size(),
            ActionListener.wrap(responses -> listener.onResponse(mergeNodeResponses(responses)), listener::onFailure)
        );

        for (Entry<String, Set<ShardId>> oneNodeAndItsShards : nodesAndShards.entrySet()) {
            if (task instanceof CancellableTask) {
                // There is no point continuing this work if the task has been cancelled.
                if (((CancellableTask) task).notifyIfCancelled(listener)) {
                    return;
                }
            }
            if (localNodeId.equals(oneNodeAndItsShards.getKey())) {
                TransportGetCheckpointNodeAction.getGlobalCheckpoints(
                    indicesService,
                    task,
                    oneNodeAndItsShards.getValue(),
                    timeout,
                    Clock.systemUTC(),
                    groupedListener
                );
                continue;
            }

            DiscoveryNodes nodes = clusterState.nodes();
            DiscoveryNode node = nodes.get(oneNodeAndItsShards.getKey());

            // paranoia: this should not be possible using the same cluster state
            if (node == null) {
                listener.onFailure(
                    new UnavailableShardsException(
                        oneNodeAndItsShards.getValue().iterator().next(),
                        "Node not found for [{}] shards",
                        oneNodeAndItsShards.getValue().size()
                    )
                );
                return;
            }

            logger.trace("get checkpoints from node {}", node);
            GetCheckpointNodeAction.Request nodeCheckpointsRequest = new GetCheckpointNodeAction.Request(
                oneNodeAndItsShards.getValue(),
                originalIndices,
                timeout
            );
            transportService.sendChildRequest(
                node,
                GetCheckpointNodeAction.NAME,
                nodeCheckpointsRequest,
                task,
                TransportRequestOptions.EMPTY,
                new ActionListenerResponseHandler<>(
                    groupedListener,
                    GetCheckpointNodeAction.Response::new,
                    TransportResponseHandler.TRANSPORT_WORKER
                )
            );
        }
    }

    private static Map<String, long[]> mergeNodeResponses(Collection<GetCheckpointNodeAction.Response> responses) {
        // the final list should be ordered by key
        Map<String, long[]> checkpointsByIndexReduced = new TreeMap<>();

        // merge the node responses
        for (GetCheckpointNodeAction.Response response : responses) {
            response.getCheckpoints().forEach((index, checkpoint) -> {
                if (checkpointsByIndexReduced.containsKey(index)) {
                    long[] shardCheckpoints = checkpointsByIndexReduced.get(index);
                    for (int i = 0; i < checkpoint.length; ++i) {
                        shardCheckpoints[i] = Math.max(shardCheckpoints[i], checkpoint[i]);
                    }
                } else {
                    checkpointsByIndexReduced.put(index, checkpoint);
                }
            });
        }

        return checkpointsByIndexReduced;
    }
}
