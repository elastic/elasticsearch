/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportService;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class TransportFieldCapabilitiesAction extends HandledTransportAction<FieldCapabilitiesRequest, FieldCapabilitiesResponse> {
    public static final String ACTION_NODE_NAME = FieldCapabilitiesAction.NAME + "[n]";
    public static final String ACTION_SHARD_NAME = FieldCapabilitiesAction.NAME + "[index][s]";

    private final ThreadPool threadPool;
    private final TransportService transportService;
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    private final FieldCapabilitiesFetcher fieldCapabilitiesFetcher;
    private final Predicate<String> metadataFieldPred;

    @Inject
    public TransportFieldCapabilitiesAction(TransportService transportService,
                                            ClusterService clusterService,
                                            ThreadPool threadPool,
                                            ActionFilters actionFilters,
                                            IndicesService indicesService,
                                            IndexNameExpressionResolver indexNameExpressionResolver) {
        super(FieldCapabilitiesAction.NAME, transportService, actionFilters, FieldCapabilitiesRequest::new);
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;

        this.fieldCapabilitiesFetcher = new FieldCapabilitiesFetcher(indicesService);
        final Set<String> metadataFields = indicesService.getAllMetadataFields();
        this.metadataFieldPred = metadataFields::contains;

        transportService.registerRequestHandler(ACTION_NODE_NAME, ThreadPool.Names.MANAGEMENT,
            FieldCapabilitiesNodeRequest::new, new NodeTransportHandler());
        transportService.registerRequestHandler(ACTION_SHARD_NAME, ThreadPool.Names.SAME,
            FieldCapabilitiesIndexRequest::new, new ShardTransportHandler());
    }

    @Override
    protected void doExecute(Task task, FieldCapabilitiesRequest request, final ActionListener<FieldCapabilitiesResponse> listener) {
        // retrieve the initial timestamp in case the action is a cross cluster search
        long nowInMillis = request.nowInMillis() == null ? System.currentTimeMillis() : request.nowInMillis();
        final ClusterState clusterState = clusterService.state();
        final Map<String, OriginalIndices> remoteClusterIndices =
            transportService.getRemoteClusterService().groupIndices(request.indicesOptions(),
                    request.indices(), idx -> indexNameExpressionResolver.hasIndexAbstraction(idx, clusterState));
        final OriginalIndices localIndices = remoteClusterIndices.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
        final String[] concreteIndices;
        if (localIndices == null) {
            // in the case we have one or more remote indices but no local we don't expand to all local indices and just do remote indices
            concreteIndices = Strings.EMPTY_ARRAY;
        } else {
            concreteIndices = indexNameExpressionResolver.concreteIndexNames(clusterState, localIndices);
        }

        if (concreteIndices.length == 0 && remoteClusterIndices.isEmpty()) {
            listener.onResponse(new FieldCapabilitiesResponse(new String[0], Collections.emptyMap()));
            return;
        }

        checkIndexBlocks(clusterState, concreteIndices);

        // Once for each cluster including the local cluster
        final CombinedResponse.Builder responseBuilder = CombinedResponse.newBuilder(request.getMergeMode(),
            request.includeUnmapped(), metadataFieldPred, threadPool.executor(ThreadPool.Names.MANAGEMENT),
            ActionListener.wrap(r -> {
                if (request.getMergeMode() == MergeResultsMode.FULL_MERGE &&
                    r.getIndices().isEmpty() && r.getIndexFailures().isEmpty() == false) {
                    // throw back the only exception
                    listener.onFailure(r.getIndexFailures().get(0).getException());
                } else {
                    listener.onResponse(r.toFieldCapResponse());
                }
            }, listener::onFailure));
        final Runnable countDown = createResponseMerger(new CountDown(1 + remoteClusterIndices.size()), responseBuilder);
        final RequestDispatcher requestDispatcher = new RequestDispatcher(
            clusterService, transportService, task, request, localIndices, nowInMillis, concreteIndices,
            threadPool.executor(ThreadPool.Names.MANAGEMENT), responseBuilder, countDown);
        requestDispatcher.execute();

        // this is the cross cluster part of this API - we force the other cluster to not merge the results but instead
        // send us back all individual index results.
        for (Map.Entry<String, OriginalIndices> remoteIndices : remoteClusterIndices.entrySet()) {
            String clusterAlias = remoteIndices.getKey();
            OriginalIndices originalIndices = remoteIndices.getValue();
            Client remoteClusterClient = transportService.getRemoteClusterService().getRemoteClusterClient(threadPool, clusterAlias);
            FieldCapabilitiesRequest remoteRequest = prepareRemoteRequest(request, originalIndices, nowInMillis);
            remoteClusterClient.fieldCaps(remoteRequest, ActionListener.wrap(response -> {
                final List<FieldCapabilitiesIndexResponse> mappedIndexResponses = response.getIndexResponses().stream()
                    .map(r -> new FieldCapabilitiesIndexResponse(
                        RemoteClusterAware.buildRemoteIndexName(clusterAlias, r.getIndexName()), r.get(), r.canMatch()))
                    .collect(Collectors.toList());
                responseBuilder.addIndexResponses(mappedIndexResponses);
                for (FieldCapabilitiesFailure failure : response.getFailures()) {
                    for (String index : failure.getIndices()) {
                        final String indexName = RemoteClusterAware.buildRemoteIndexName(clusterAlias, index);
                        responseBuilder.addIndexFailure(indexName, failure.getException());
                    }
                }
                countDown.run();
            }, ex -> {
                for (String index : originalIndices.indices()) {
                    responseBuilder.addIndexFailure(RemoteClusterAware.buildRemoteIndexName(clusterAlias, index), ex);
                }
                countDown.run();
            }));
        }
    }

    private void checkIndexBlocks(ClusterState clusterState, String[] concreteIndices) {
        clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);
        for (String index : concreteIndices) {
            clusterState.blocks().indexBlockedRaiseException(ClusterBlockLevel.READ, index);
        }
    }

    private Runnable createResponseMerger(CountDown completionCounter, CombinedResponse.Builder responseBuilder) {
        return () -> {
            if (completionCounter.countDown()) {
                responseBuilder.complete();
            }
        };
    }

    private static FieldCapabilitiesRequest prepareRemoteRequest(FieldCapabilitiesRequest request,
                                                                 OriginalIndices originalIndices,
                                                                 long nowInMillis) {
        FieldCapabilitiesRequest remoteRequest = new FieldCapabilitiesRequest();
        // TODO: Enable merging on remote clusters
        remoteRequest.setMergeMode(MergeResultsMode.NO_MERGE);
        remoteRequest.indicesOptions(originalIndices.indicesOptions());
        remoteRequest.indices(originalIndices.indices());
        remoteRequest.fields(request.fields());
        remoteRequest.runtimeFields(request.runtimeFields());
        remoteRequest.indexFilter(request.indexFilter());
        remoteRequest.nowInMillis(nowInMillis);
        return remoteRequest;
    }

    private class NodeTransportHandler implements TransportRequestHandler<FieldCapabilitiesNodeRequest> {
        @Override
        public void messageReceived(FieldCapabilitiesNodeRequest request, TransportChannel channel, Task task) throws Exception {
            final ActionListener<FieldCapabilitiesNodeResponse> listener = new ChannelActionListener<>(channel, ACTION_NODE_NAME, request);
            // If the request has an index filter, it may contain several shards belonging to the same
            // index. We make sure to skip over a shard if we already found a match for that index.
            final Map<String, List<ShardId>> groupedShardIds =
                request.shardIds().stream().collect(Collectors.groupingBy(ShardId::getIndexName));
            final Set<ShardId> unmatchedShardIds = new HashSet<>();
            final MergeResultsMode mergeMode = request.getMergeMode();
            final CombinedResponse.Builder responseBuilder = CombinedResponse.newBuilder(mergeMode,
                false, metadataFieldPred, EsExecutors.DIRECT_EXECUTOR_SERVICE, listener.map(r -> r.toNodeResponse(unmatchedShardIds)));
            for (List<ShardId> shardIds : groupedShardIds.values()) {
                for (ShardId shardId : shardIds) {
                    final FieldCapabilitiesIndexRequest indexRequest = new FieldCapabilitiesIndexRequest(request.fields(), shardId,
                        request.originalIndices(), request.indexFilter(), request.nowInMillis(), request.runtimeFields());
                    try {
                        final FieldCapabilitiesIndexResponse resp = fieldCapabilitiesFetcher.fetch(indexRequest);
                        if (resp.canMatch()) {
                            responseBuilder.addIndexResponses(Collections.singletonList(resp));
                            break;
                        } else {
                            unmatchedShardIds.add(shardId);
                        }
                    } catch (Exception e) {
                        responseBuilder.addIndexFailure(shardId.getIndexName(), e);
                    }
                }
            }
            responseBuilder.complete();
        }
    }

    private class ShardTransportHandler implements TransportRequestHandler<FieldCapabilitiesIndexRequest> {
        @Override
        public void messageReceived(FieldCapabilitiesIndexRequest request, TransportChannel channel, Task task) throws Exception {
            ActionListener<FieldCapabilitiesIndexResponse> listener = new ChannelActionListener<>(channel, ACTION_SHARD_NAME, request);
            ActionListener.completeWith(listener, () -> fieldCapabilitiesFetcher.fetch(request));
        }
    }

}
