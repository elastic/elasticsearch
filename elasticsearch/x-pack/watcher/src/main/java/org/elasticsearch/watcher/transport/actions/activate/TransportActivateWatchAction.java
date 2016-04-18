/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.actions.activate;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.watcher.WatcherService;
import org.elasticsearch.watcher.WatcherLicensee;
import org.elasticsearch.watcher.transport.actions.WatcherTransportAction;
import org.elasticsearch.watcher.watch.WatchStatus;
import org.elasticsearch.watcher.watch.WatchStore;

/**
 * Performs the watch de/activation operation.
 */
public class TransportActivateWatchAction extends WatcherTransportAction<ActivateWatchRequest, ActivateWatchResponse> {

    private final WatcherService watcherService;

    @Inject
    public TransportActivateWatchAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                        ThreadPool threadPool, ActionFilters actionFilters,
                                        IndexNameExpressionResolver indexNameExpressionResolver, WatcherService watcherService,
                                        WatcherLicensee watcherLicensee) {
        super(settings, ActivateWatchAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
                watcherLicensee, ActivateWatchRequest::new);
        this.watcherService = watcherService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected ActivateWatchResponse newResponse() {
        return new ActivateWatchResponse();
    }

    @Override
    protected void masterOperation(ActivateWatchRequest request, ClusterState state, ActionListener<ActivateWatchResponse> listener)
            throws ElasticsearchException {
        try {
            WatchStatus watchStatus = request.isActivate() ?
                    watcherService.activateWatch(request.getWatchId(), request.masterNodeTimeout()) :
                    watcherService.deactivateWatch(request.getWatchId(), request.masterNodeTimeout());
            ActivateWatchResponse response = new ActivateWatchResponse(watchStatus);
            listener.onResponse(response);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    protected ClusterBlockException checkBlock(ActivateWatchRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, WatchStore.INDEX);
    }


}
