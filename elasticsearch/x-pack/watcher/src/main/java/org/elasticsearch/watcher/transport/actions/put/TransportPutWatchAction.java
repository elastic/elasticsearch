/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.actions.put;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.plugin.core.LicenseUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.watcher.WatcherService;
import org.elasticsearch.watcher.WatcherLicensee;
import org.elasticsearch.watcher.transport.actions.WatcherTransportAction;
import org.elasticsearch.watcher.watch.WatchStore;

/**
 */
public class TransportPutWatchAction extends WatcherTransportAction<PutWatchRequest, PutWatchResponse> {

    private final WatcherService watcherService;

    @Inject
    public TransportPutWatchAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                   ThreadPool threadPool, ActionFilters actionFilters,
                                   IndexNameExpressionResolver indexNameExpressionResolver, WatcherService watcherService,
                                   WatcherLicensee watcherLicensee) {
        super(settings, PutWatchAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
                watcherLicensee, PutWatchRequest::new);
        this.watcherService = watcherService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected PutWatchResponse newResponse() {
        return new PutWatchResponse();
    }

    @Override
    protected void masterOperation(PutWatchRequest request, ClusterState state, ActionListener<PutWatchResponse> listener) throws
            ElasticsearchException {
        if (!watcherLicensee.isPutWatchAllowed()) {
            listener.onFailure(LicenseUtils.newComplianceException(WatcherLicensee.ID));
            return;
        }

        try {
            IndexResponse indexResponse = watcherService.putWatch(request.getId(), request.getSource(), request.masterNodeTimeout(),
                    request.isActive());
            listener.onResponse(new PutWatchResponse(indexResponse.getId(), indexResponse.getVersion(), indexResponse.isCreated()));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    protected ClusterBlockException checkBlock(PutWatchRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, WatchStore.INDEX);
    }

}
