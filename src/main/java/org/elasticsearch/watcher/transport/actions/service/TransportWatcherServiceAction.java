/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.actions.service;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
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
import org.elasticsearch.watcher.WatcherLifeCycleService;
import org.elasticsearch.watcher.license.LicenseService;
import org.elasticsearch.watcher.transport.actions.WatcherTransportAction;

/**
 */
public class TransportWatcherServiceAction extends WatcherTransportAction<WatcherServiceRequest, WatcherServiceResponse> {

    private final WatcherLifeCycleService lifeCycleService;

    @Inject
    public TransportWatcherServiceAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool, ActionFilters actionFilters, WatcherLifeCycleService lifeCycleService, LicenseService licenseService) {
        super(settings, WatcherServiceAction.NAME, transportService, clusterService, threadPool, actionFilters, licenseService);
        this.lifeCycleService = lifeCycleService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected WatcherServiceRequest newRequest() {
        return new WatcherServiceRequest();
    }

    @Override
    protected WatcherServiceResponse newResponse() {
        return new WatcherServiceResponse();
    }

    @Override
    protected void masterOperation(WatcherServiceRequest request, ClusterState state, ActionListener<WatcherServiceResponse> listener) throws ElasticsearchException {
        switch (request.getCommand()) {
            case START:
                lifeCycleService.start();
                break;
            case STOP:
                lifeCycleService.stop();
                break;
            case RESTART:
                lifeCycleService.stop();
                lifeCycleService.start();
                break;
            default:
                listener.onFailure(new ElasticsearchIllegalArgumentException("Command [" + request.getCommand() + "] is undefined"));
                return;
        }
        listener.onResponse(new WatcherServiceResponse(true));
    }

    @Override
    protected ClusterBlockException checkBlock(WatcherServiceRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA);
    }
}
