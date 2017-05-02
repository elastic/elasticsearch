/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.actions.service;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.AckedRequest;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.watcher.WatcherMetaData;

public class TransportWatcherServiceAction extends TransportMasterNodeAction<WatcherServiceRequest, WatcherServiceResponse> {

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
    public TransportWatcherServiceAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                         ThreadPool threadPool, ActionFilters actionFilters,
                                         IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, WatcherServiceAction.NAME, transportService, clusterService, threadPool, actionFilters,
                indexNameExpressionResolver, WatcherServiceRequest::new);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected WatcherServiceResponse newResponse() {
        return new WatcherServiceResponse();
    }

    @Override
    protected void masterOperation(WatcherServiceRequest request, ClusterState state,
                                   ActionListener<WatcherServiceResponse> listener) throws Exception {
        switch (request.getCommand()) {
            case STOP:
                setWatcherMetaDataAndWait(true, listener);
                break;
            case START:
                setWatcherMetaDataAndWait(false, listener);
                break;
        }
    }

    private void setWatcherMetaDataAndWait(boolean manuallyStopped, final ActionListener
            <WatcherServiceResponse> listener) {
        String source = manuallyStopped ? "update_watcher_manually_stopped" : "update_watcher_manually_started";

        clusterService.submitStateUpdateTask(source,
                new AckedClusterStateUpdateTask<Boolean>(ackedRequest,
                        ActionListener.wrap(b -> listener.onResponse(new WatcherServiceResponse(true)), listener::onFailure)) {

                    @Override
                    protected Boolean newResponse(boolean result) {
                        return result;
                    }

                    @Override
                    public ClusterState execute(ClusterState clusterState) throws Exception {
                        ClusterState.Builder builder = new ClusterState.Builder(clusterState);
                        builder.metaData(MetaData.builder(clusterState.getMetaData())
                                .putCustom(WatcherMetaData.TYPE, new WatcherMetaData(manuallyStopped)));
                        return builder.build();
                    }

                    @Override
                    public void onFailure(String source, Exception throwable) {
                        listener.onFailure(throwable);
                    }
                });
    }

    @Override
    protected ClusterBlockException checkBlock(WatcherServiceRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
