/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.actions.get;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.watcher.WatcherService;
import org.elasticsearch.watcher.license.LicenseService;
import org.elasticsearch.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.watcher.transport.actions.WatcherTransportAction;
import org.elasticsearch.watcher.watch.Watch;
import org.elasticsearch.watcher.watch.WatchStore;

import java.io.IOException;

/**
 * Performs the get operation.
 */
public class TransportGetWatchAction extends WatcherTransportAction<GetWatchRequest, GetWatchResponse> {

    private final WatcherService watcherService;

    @Inject
    public TransportGetWatchAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                   ThreadPool threadPool, ActionFilters actionFilters, WatcherService watcherService, LicenseService licenseService) {
        super(settings, GetWatchAction.NAME, transportService, clusterService, threadPool, actionFilters, licenseService);
        this.watcherService = watcherService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME; // Super lightweight operation, so don't fork
    }

    @Override
    protected GetWatchRequest newRequest() {
        return new GetWatchRequest();
    }

    @Override
    protected GetWatchResponse newResponse() {
        return new GetWatchResponse();
    }

    @Override
    protected void masterOperation(GetWatchRequest request, ClusterState state, ActionListener<GetWatchResponse> listener) throws ElasticsearchException {
        try {
            Watch watch = watcherService.getWatch(request.getId());
            if (watch == null) {
                listener.onResponse(new GetWatchResponse(request.getId(), -1, false, null));
                return;
            }

            BytesReference watchSource = null;
            try (XContentBuilder builder = JsonXContent.contentBuilder()) {
                watch.toXContent(builder, WatcherParams.builder().hideSecrets(true).build());
                watchSource = builder.bytes();
            } catch (IOException e) {
                listener.onFailure(e);
                return;
            }
            listener.onResponse(new GetWatchResponse(watch.id(), watch.status().version(), true, watchSource));

        } catch (Throwable t) {
            logger.error("failed to get watch [{}]", t, request.getId());
            throw t;
        }
    }

    @Override
    protected ClusterBlockException checkBlock(GetWatchRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.READ, WatchStore.INDEX);
    }
}
