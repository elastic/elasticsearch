/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.transport.actions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.watcher.WatcherMetadata;
import org.elasticsearch.xpack.core.watcher.transport.actions.service.WatcherServiceAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.service.WatcherServiceRequest;

import static org.elasticsearch.core.Strings.format;

public class TransportWatcherServiceAction extends AcknowledgedTransportMasterNodeAction<WatcherServiceRequest> {

    private static final Logger logger = LogManager.getLogger(TransportWatcherServiceAction.class);

    @Inject
    public TransportWatcherServiceAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters
    ) {
        super(
            WatcherServiceAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            WatcherServiceRequest::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
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

        // TODO: make WatcherServiceRequest a real AcknowledgedRequest so that we have both a configurable timeout and master node timeout
        // like we do elsewhere
        submitUnbatchedTask(
            source,
            new AckedClusterStateUpdateTask(request.masterNodeTimeout(), AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, listener) {
                @Override
                public ClusterState execute(ClusterState clusterState) {
                    XPackPlugin.checkReadyForXPackCustomMetadata(clusterState);

                    WatcherMetadata newWatcherMetadata = new WatcherMetadata(manuallyStopped);
                    final var project = clusterState.metadata().getProject();
                    WatcherMetadata currentMetadata = project.custom(WatcherMetadata.TYPE);

                    // adhere to the contract of returning the original state if nothing has changed
                    if (newWatcherMetadata.equals(currentMetadata)) {
                        return clusterState;
                    } else {
                        return clusterState.copyAndUpdateProject(project.id(), b -> b.putCustom(WatcherMetadata.TYPE, newWatcherMetadata));
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error(() -> format("could not update watcher stopped status to [%s], source [%s]", manuallyStopped, source), e);
                    listener.onFailure(e);
                }
            }
        );
    }

    @Override
    protected ClusterBlockException checkBlock(WatcherServiceRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

}
