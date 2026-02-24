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
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
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
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointAction;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointAction.Request;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointAction.Response;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointNodeAction;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
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
import java.util.stream.Collectors;

import static org.elasticsearch.search.crossproject.CrossProjectIndexResolutionValidator.indicesOptionsForCrossProjectFanout;

public class TransportGetCheckpointAction extends HandledTransportAction<Request, Response> {

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

        ActionListener<Map<String, Response>> mergeAndValidateListener = listener.delegateFailureAndWrap(
            (l, responsesByCluster) -> validateAndMergeResponses(request, responsesByCluster, l)
        );

        fanOut(task, request, clusterState, remoteClusterIndices, mergeAndValidateListener);
    }

    private void fanOut(
        Task task,
        Request request,
        ClusterState clusterState,
        Map<String, OriginalIndices> remoteClusterIndices,
        ActionListener<Map<String, Response>> listener
    ) {
        var localIndices = remoteClusterIndices.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);

        // we only need the response's resolved indices if we are cross-project, otherwise we ignore it
        var includeResolvedIndexExpressions = crossProjectModeDecider.resolvesCrossProject(request);

        ActionListener<Tuple<String, Response>> responsesByClusterListener;
        var numClusters = remoteClusterIndices.size() + (localIndices != null ? 1 : 0);
        if (numClusters > 1) {
            responsesByClusterListener = new GroupedActionListener<>(
                numClusters,
                listener.map(responsesByCluster -> responsesByCluster.stream().collect(Collectors.toMap(Tuple::v1, Tuple::v2)))
            );
        } else {
            responsesByClusterListener = listener.map(tuple -> Map.of(tuple.v1(), tuple.v2()));
        }

        if (localIndices != null) {
            resolveLocalCheckpoints(
                task,
                request,
                clusterState,
                localIndices,
                includeResolvedIndexExpressions,
                responsesByClusterListener.map(response -> Tuple.tuple(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, response))
            );
        }

        var threadContext = client.threadPool().getThreadContext();
        var headers = request.headers();

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
            ClientHelper.executeWithHeadersAsync(
                threadContext,
                headers,
                ClientHelper.TRANSFORM_ORIGIN,
                remoteRequest,
                responsesByClusterListener.<Response>map(response -> Tuple.tuple(cluster, response)),
                (r, l) -> remoteClusterClient.execute(GetCheckpointAction.REMOTE_TYPE, r, l)
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
        if (crossProjectModeDecider.crossProjectEnabled() && TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled()) {
            // For cross-project search, ResolvedIndexExpressions will have resolved our local indices already, and we will filter ones
            // that have successfully resolved before calling their shards. Any indices that did not successfully resolve are validated
            // when aggregated with all Response objects in validateAndMergeResponses().
            concreteIndices = request.getResolvedIndexExpressions()
                .expressions()
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
        ActionListener<Response> mergedResponseListener
    ) {
        // if we're calling cross-project, we have to validate all cross-project responses together with the local response
        if (crossProjectModeDecider.resolvesCrossProject(request)) {
            Map<String, ResolvedIndexExpressions> remoteResolvedExpressions = responsesByCluster.entrySet()
                .stream()
                .filter(entry -> RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY.equals(entry.getKey()) == false)
                .filter(entry -> entry.getValue().resolvedIndexExpressions() != null)
                .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue().resolvedIndexExpressions()));
            var crossProjectException = CrossProjectIndexResolutionValidator.validate(
                request.indicesOptions(),
                request.getProjectRouting(),
                request.getResolvedIndexExpressions(),
                remoteResolvedExpressions
            );
            if (crossProjectException != null) {
                mergedResponseListener.onFailure(crossProjectException);
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
            request.getCluster()
        );
        searchShardsRequest.setParentTask(parentTaskId);
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

    static Map<String, Set<ShardId>> filterOutSkippedShards(
        Map<String, Set<ShardId>> nodesAndShards,
        SearchShardsResponse searchShardsResponse
    ) {
        Map<String, Set<ShardId>> filteredNodesAndShards = new HashMap<>(nodesAndShards.size());
        // Create a deep copy of the given nodes and shards map.
        for (Map.Entry<String, Set<ShardId>> nodeAndShardsEntry : nodesAndShards.entrySet()) {
            String node = nodeAndShardsEntry.getKey();
            Set<ShardId> shards = nodeAndShardsEntry.getValue();
            filteredNodesAndShards.put(node, new HashSet<>(shards));
        }
        // Remove (node, shard) pairs for all the skipped shards.
        for (SearchShardsGroup shardGroup : searchShardsResponse.getGroups()) {
            if (shardGroup.skipped()) {
                for (String allocatedNode : shardGroup.allocatedNodes()) {
                    Set<ShardId> shards = filteredNodesAndShards.get(allocatedNode);
                    if (shards != null) {
                        shards.remove(shardGroup.shardId());
                        if (shards.isEmpty()) {
                            // Remove node if no shards were left.
                            filteredNodesAndShards.remove(allocatedNode);
                        }
                    }
                }
            }
        }
        return filteredNodesAndShards;
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
