/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.actions.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.AckedRequest;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.watcher.WatcherMetadata;
import org.elasticsearch.xpack.core.watcher.transport.actions.service.WatcherServiceAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.service.WatcherServiceRequest;

import java.io.IOException;

public class TransportWatcherServiceAction extends TransportMasterNodeAction<WatcherServiceRequest, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportWatcherServiceAction.class);

    private AckedRequest ackedRequest = new AckedRequest() {
        @Override
        public TimeValue ackTimeout() {
            return AcknowledgedRequest.DEFAULT_ACK_TIMEOUT;
        }

        @Override
        public TimeValue masterNodeTimeout() {
            return AcknowledgedRequest.DEFAULT_ACK_TIMEOUT;
        }
    };

    @Inject
    public TransportWatcherServiceAction(TransportService transportService, ClusterService clusterService,
                                         ThreadPool threadPool, ActionFilters actionFilters,
                                         IndexNameExpressionResolver indexNameExpressionResolver) {
        super(WatcherServiceAction.NAME, transportService, clusterService, threadPool, actionFilters,
            WatcherServiceRequest::new, indexNameExpressionResolver);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void masterOperation(Task task, WatcherServiceRequest request, ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) {
        switch (request.getCommand()) {
            case STOP:
                setWatcherMetadataAndWait(true, listener);
                break;
            case START:
                setWatcherMetadataAndWait(false, listener);
                break;
        }
    }

    private void setWatcherMetadataAndWait(boolean manuallyStopped, final ActionListener<AcknowledgedResponse> listener) {
        String source = manuallyStopped ? "update_watcher_manually_stopped" : "update_watcher_manually_started";

        clusterService.submitStateUpdateTask(source,
                new AckedClusterStateUpdateTask<AcknowledgedResponse>(ackedRequest, listener) {

                    @Override
                    protected AcknowledgedResponse newResponse(boolean acknowledged) {
                        return new AcknowledgedResponse(acknowledged);
                    }

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
                            builder.metadata(Metadata.builder(clusterState.getMetadata())
                                    .putCustom(WatcherMetadata.TYPE, newWatcherMetadata));
                            return builder.build();
                        }
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        logger.error(new ParameterizedMessage("could not update watcher stopped status to [{}], source [{}]",
                                manuallyStopped, source), e);
                        listener.onFailure(e);
                    }
                });
    }

    @Override
    protected ClusterBlockException checkBlock(WatcherServiceRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
