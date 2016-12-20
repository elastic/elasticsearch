/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.actions.delete;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.watcher.support.init.proxy.WatcherClientProxy;
import org.elasticsearch.xpack.watcher.trigger.TriggerService;
import org.elasticsearch.xpack.watcher.watch.Watch;

/**
 * Performs the delete operation. This inherits directly from TransportMasterNodeAction, because deletion should always work
 * independently from the license check in WatcherTransportAction!
 */
public class TransportDeleteWatchAction extends TransportMasterNodeAction<DeleteWatchRequest, DeleteWatchResponse> {

    private final WatcherClientProxy client;
    private final TriggerService triggerService;

    @Inject
    public TransportDeleteWatchAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                      ThreadPool threadPool, ActionFilters actionFilters,
                                      IndexNameExpressionResolver indexNameExpressionResolver, WatcherClientProxy client,
                                      TriggerService triggerService) {
        super(settings, DeleteWatchAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
                DeleteWatchRequest::new);
        this.client = client;
        this.triggerService = triggerService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected DeleteWatchResponse newResponse() {
        return new DeleteWatchResponse();
    }

    @Override
    protected void masterOperation(DeleteWatchRequest request, ClusterState state, ActionListener<DeleteWatchResponse> listener) throws
            ElasticsearchException {
        client.deleteWatch(request.getId(), ActionListener.wrap(deleteResponse -> {
                    boolean deleted = deleteResponse.getResult() == DocWriteResponse.Result.DELETED;
                    DeleteWatchResponse response = new DeleteWatchResponse(deleteResponse.getId(), deleteResponse.getVersion(), deleted);
                    if (deleted) {
                        triggerService.remove(request.getId());
                    }
                    listener.onResponse(response);
                },
                listener::onFailure));
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteWatchRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, Watch.INDEX);
    }
}
