/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.state;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Metadata.Custom;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;
import java.util.function.Predicate;

public class TransportClusterStateAction extends TransportMasterNodeReadAction<ClusterStateRequest, ClusterStateResponse> {

    private static final Logger logger = LogManager.getLogger(TransportClusterStateAction.class);

    @Inject
    public TransportClusterStateAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ClusterStateAction.NAME,
            false,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ClusterStateRequest::new,
            indexNameExpressionResolver,
            ClusterStateResponse::new,
            ThreadPool.Names.MANAGEMENT
        );
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterStateRequest request, ClusterState state) {
        // cluster state calls are done also on a fully blocked cluster to figure out what is going
        // on in the cluster. For example, which nodes have joined yet the recovery has not yet kicked
        // in, we need to make sure we allow those calls
        // return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA);
        return null;
    }

    @Override
    protected void masterOperation(
        Task task,
        final ClusterStateRequest request,
        final ClusterState state,
        final ActionListener<ClusterStateResponse> listener
    ) throws IOException {

        assert task instanceof CancellableTask : task + " not cancellable";
        final CancellableTask cancellableTask = (CancellableTask) task;

        final Predicate<ClusterState> acceptableClusterStatePredicate = request.waitForMetadataVersion() == null
            ? clusterState -> true
            : clusterState -> clusterState.metadata().version() >= request.waitForMetadataVersion();

        final Predicate<ClusterState> acceptableClusterStateOrFailedPredicate = request.local()
            ? acceptableClusterStatePredicate
            : acceptableClusterStatePredicate.or(clusterState -> clusterState.nodes().isLocalNodeElectedMaster() == false);

        if (cancellableTask.notifyIfCancelled(listener)) {
            return;
        }
        if (acceptableClusterStatePredicate.test(state)) {
            ActionListener.completeWith(listener, () -> buildResponse(request, state));
        } else {
            assert acceptableClusterStateOrFailedPredicate.test(state) == false;
            new ClusterStateObserver(state, clusterService, request.waitForTimeout(), logger, threadPool.getThreadContext())
                .waitForNextChange(new ClusterStateObserver.Listener() {

                    @Override
                    public void onNewClusterState(ClusterState newState) {
                        if (cancellableTask.notifyIfCancelled(listener)) {
                            return;
                        }

                        if (acceptableClusterStatePredicate.test(newState)) {
                            ActionListener.completeWith(listener, () -> buildResponse(request, newState));
                        } else {
                            listener.onFailure(
                                new NotMasterException(
                                    "master stepped down waiting for metadata version " + request.waitForMetadataVersion()
                                )
                            );
                        }
                    }

                    @Override
                    public void onClusterServiceClose() {
                        listener.onFailure(new NodeClosedException(clusterService.localNode()));
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        ActionListener.run(listener, l -> {
                            if (cancellableTask.notifyIfCancelled(l) == false) {
                                l.onResponse(new ClusterStateResponse(state.getClusterName(), null, true));
                            }
                        });
                    }
                }, clusterState -> cancellableTask.isCancelled() || acceptableClusterStateOrFailedPredicate.test(clusterState));
        }
    }

    private ClusterStateResponse buildResponse(final ClusterStateRequest request, final ClusterState currentState) {
        logger.trace("Serving cluster state request using version {}", currentState.version());
        ClusterState.Builder builder = ClusterState.builder(currentState.getClusterName());
        builder.version(currentState.version());
        builder.stateUUID(currentState.stateUUID());

        if (request.nodes()) {
            builder.nodes(currentState.nodes());
            builder.transportVersions(currentState.transportVersions());
        }
        if (request.routingTable()) {
            if (request.indices().length > 0) {
                RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
                String[] indices = indexNameExpressionResolver.concreteIndexNames(currentState, request);
                for (String filteredIndex : indices) {
                    if (currentState.routingTable().getIndicesRouting().containsKey(filteredIndex)) {
                        routingTableBuilder.add(currentState.routingTable().getIndicesRouting().get(filteredIndex));
                    }
                }
                builder.routingTable(routingTableBuilder.build());
            } else {
                builder.routingTable(currentState.routingTable());
            }
        }
        if (request.blocks()) {
            builder.blocks(currentState.blocks());
        }

        Metadata.Builder mdBuilder = Metadata.builder();
        mdBuilder.clusterUUID(currentState.metadata().clusterUUID());
        mdBuilder.coordinationMetadata(currentState.coordinationMetadata());

        if (request.metadata()) {
            if (request.indices().length > 0) {
                mdBuilder.version(currentState.metadata().version());
                String[] indices = indexNameExpressionResolver.concreteIndexNames(currentState, request);
                for (String filteredIndex : indices) {
                    // If the requested index is part of a data stream then that data stream should also be included:
                    IndexAbstraction indexAbstraction = currentState.metadata().getIndicesLookup().get(filteredIndex);
                    if (indexAbstraction.getParentDataStream() != null) {
                        DataStream dataStream = indexAbstraction.getParentDataStream();
                        // Also the IMD of other backing indices need to be included, otherwise the cluster state api
                        // can't create a valid cluster state instance:
                        for (Index backingIndex : dataStream.getIndices()) {
                            mdBuilder.put(currentState.metadata().index(backingIndex), false);
                        }
                        mdBuilder.put(dataStream);
                    } else {
                        IndexMetadata indexMetadata = currentState.metadata().index(filteredIndex);
                        if (indexMetadata != null) {
                            mdBuilder.put(indexMetadata, false);
                        }
                    }
                }
            } else {
                mdBuilder = Metadata.builder(currentState.metadata());
            }

            // filter out metadata that shouldn't be returned by the API
            for (Map.Entry<String, Custom> custom : currentState.metadata().customs().entrySet()) {
                if (custom.getValue().context().contains(Metadata.XContentContext.API) == false) {
                    mdBuilder.removeCustom(custom.getKey());
                }
            }
        }
        builder.metadata(mdBuilder);

        if (request.customs()) {
            for (Map.Entry<String, ClusterState.Custom> custom : currentState.customs().entrySet()) {
                if (custom.getValue().isPrivate() == false) {
                    builder.putCustom(custom.getKey(), custom.getValue());
                }
            }
        }

        return new ClusterStateResponse(currentState.getClusterName(), builder.build(), false);
    }

}
