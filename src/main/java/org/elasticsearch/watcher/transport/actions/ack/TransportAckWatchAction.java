/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.actions.ack;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
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
public class TransportAckWatchAction extends WatcherTransportAction<AckWatchRequest, AckWatchResponse> {

    private final WatcherService watcherService;

    @Inject
    public TransportAckWatchAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                   ThreadPool threadPool, ActionFilters actionFilters, WatcherService watcherService, LicenseService licenseService) {
        super(settings, AckWatchAction.NAME, transportService, clusterService, threadPool, actionFilters, licenseService);
        this.watcherService = watcherService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected AckWatchRequest newRequest() {
        return new AckWatchRequest();
    }

    @Override
    protected AckWatchResponse newResponse() {
        return new AckWatchResponse();
    }

    @Override
    protected void masterOperation(AckWatchRequest request, ClusterState state, ActionListener<AckWatchResponse> listener) throws ElasticsearchException {
        try {
            AckWatchResponse response = new AckWatchResponse(watcherService.ackWatch(request.getId()));
            listener.onResponse(response);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    protected ClusterBlockException checkBlock(AckWatchRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, WatchStore.INDEX);
    }


}
