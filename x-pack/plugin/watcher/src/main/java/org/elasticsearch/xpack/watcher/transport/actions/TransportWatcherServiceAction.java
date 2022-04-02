/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.transport.actions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.ack.AckedRequest;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.watcher.WatcherMetadata;
import org.elasticsearch.xpack.core.watcher.transport.actions.service.WatcherServiceAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.service.WatcherServiceRequest;

public class TransportWatcherServiceAction extends AcknowledgedTransportMasterNodeAction<WatcherServiceRequest> {

    private static final Logger logger = LogManager.getLogger(TransportWatcherServiceAction.class);

    @Inject
    public TransportWatcherServiceAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            WatcherServiceAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            WatcherServiceRequest::new,
            indexNameExpressionResolver,
            ThreadPool.Names.MANAGEMENT
        );
    }

    @Override
    protected void masterOperation(
        Task task,
        WatcherServiceRequest request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        final boolean manuallyStopped = request.getCommand() == WatcherServiceRequest.Command.STOP;
        final String source = manuallyStopped ? "update_watcher_manually_stopped" : "update_watcher_manually_started";

        // TODO: make WatcherServiceRequest a real AckedRequest so that we have both a configurable timeout and master node timeout like
        // we do elsewhere
        clusterService.submitStateUpdateTask(source, new AckedClusterStateUpdateTask(new AckedRequest() {
            @Override
            public TimeValue ackTimeout() {
                return AcknowledgedRequest.DEFAULT_ACK_TIMEOUT;
            }

            @Override
            public TimeValue masterNodeTimeout() {
                return request.masterNodeTimeout();
            }
        }, listener) {
            @Override
            public ClusterState execute(ClusterState clusterState) {
                XPackPlugin.checkReadyForXPackCustomMetadata(clusterState);

                WatcherMetadata newWatcherMetadata = new WatcherMetadata(manuallyStopped);
                WatcherMetadata currentMetadata = clusterState.metadata().custom(WatcherMetadata.TYPE);

                // adhere to the contract of returning the original state if nothing has changed
                if (newWatcherMetadata.equals(currentMetadata)) {
                    return clusterState;
                } else {
                    ClusterState.Builder builder = new ClusterState.Builder(clusterState);
                    builder.metadata(Metadata.builder(clusterState.getMetadata()).putCustom(WatcherMetadata.TYPE, newWatcherMetadata));
                    return builder.build();
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(
                    new ParameterizedMessage("could not update watcher stopped status to [{}], source [{}]", manuallyStopped, source),
                    e
                );
                listener.onFailure(e);
            }
        }, newExecutor());
    }

    @Override
    protected ClusterBlockException checkBlock(WatcherServiceRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private static <T extends ClusterStateUpdateTask> ClusterStateTaskExecutor<T> newExecutor() {
        return ClusterStateTaskExecutor.unbatched();
    }
}
