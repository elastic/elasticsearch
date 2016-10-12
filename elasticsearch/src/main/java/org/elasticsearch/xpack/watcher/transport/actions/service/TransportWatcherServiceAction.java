/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.actions.service;

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
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.watcher.WatcherLifeCycleService;
import org.elasticsearch.xpack.watcher.transport.actions.WatcherTransportAction;

public class TransportWatcherServiceAction extends WatcherTransportAction<WatcherServiceRequest, WatcherServiceResponse> {

    private final WatcherLifeCycleService lifeCycleService;

    @Inject
    public TransportWatcherServiceAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                         ThreadPool threadPool, ActionFilters actionFilters,
                                         IndexNameExpressionResolver indexNameExpressionResolver,
                                         WatcherLifeCycleService lifeCycleService, XPackLicenseState licenseState) {
        super(settings, WatcherServiceAction.NAME, transportService, clusterService, threadPool, actionFilters,
                indexNameExpressionResolver, licenseState, WatcherServiceRequest::new);
        this.lifeCycleService = lifeCycleService;
    }

    @Override
    protected String executor() {
        // Ideally we should use SAME TP here, because we should always be able to stop Watcher even if management TP has been exhausted,
        // but we can't use SAME TP here, because certain parts of the start process can't be executed on a transport thread.
        // ( put template, or anything client related with #actionGet() )
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected WatcherServiceResponse newResponse() {
        return new WatcherServiceResponse();
    }

    @Override
    protected void masterOperation(WatcherServiceRequest request, ClusterState state, ActionListener<WatcherServiceResponse> listener)
            throws ElasticsearchException {
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
                listener.onFailure(new IllegalArgumentException("Command [" + request.getCommand() + "] is undefined"));
                return;
        }
        listener.onResponse(new WatcherServiceResponse(true));
    }

    @Override
    protected ClusterBlockException checkBlock(WatcherServiceRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
