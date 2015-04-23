/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.actions.delete;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.watcher.license.LicenseService;
import org.elasticsearch.watcher.transport.actions.WatcherTransportAction;
import org.elasticsearch.watcher.WatcherService;
import org.elasticsearch.watcher.watch.WatchStore;

/**
 * Performs the delete operation.
 */
public class TransportDeleteWatchAction extends WatcherTransportAction<DeleteWatchRequest, DeleteWatchResponse> {

    private final WatcherService watcherService;

    @Inject
    public TransportDeleteWatchAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                      ThreadPool threadPool, ActionFilters actionFilters, WatcherService watcherService, LicenseService licenseService) {
        super(settings, DeleteWatchAction.NAME, transportService, clusterService, threadPool, actionFilters, licenseService);
        this.watcherService = watcherService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected DeleteWatchRequest newRequest() {
        return new DeleteWatchRequest();
    }

    @Override
    protected DeleteWatchResponse newResponse() {
        return new DeleteWatchResponse();
    }

    @Override
    protected void masterOperation(DeleteWatchRequest request, ClusterState state, ActionListener<DeleteWatchResponse> listener) throws ElasticsearchException {
        try {
            DeleteResponse deleteResponse = watcherService.deleteWatch(request.getId()).deleteResponse();
            DeleteWatchResponse response = new DeleteWatchResponse(deleteResponse.getId(), deleteResponse.getVersion(), deleteResponse.isFound());
            listener.onResponse(response);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteWatchRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, WatchStore.INDEX);
    }


}
