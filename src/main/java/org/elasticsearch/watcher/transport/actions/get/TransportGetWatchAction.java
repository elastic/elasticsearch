/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.actions.get;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.watcher.watch.Watch;
import org.elasticsearch.watcher.watch.WatchService;
import org.elasticsearch.watcher.watch.WatchStore;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

/**
 * Performs the get operation.
 */
public class TransportGetWatchAction extends TransportMasterNodeOperationAction<GetWatchRequest, GetWatchResponse> {

    private final WatchService watchService;

    @Inject
    public TransportGetWatchAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                   ThreadPool threadPool, ActionFilters actionFilters, WatchService watchService) {
        super(settings, GetWatchAction.NAME, transportService, clusterService, threadPool, actionFilters);
        this.watchService = watchService;
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
        Watch watch = watchService.getWatch(request.watchName());
        GetResult getResult;
        if (watch != null) {
            BytesReference watchSource = null;
            try (XContentBuilder builder = JsonXContent.contentBuilder()) {
                builder.value(watch);
                watchSource = builder.bytes();
            } catch (IOException e) {
                listener.onFailure(e);
            }
            getResult = new GetResult(WatchStore.INDEX, WatchStore.DOC_TYPE, watch.name(), watch.status().version(), true, watchSource, null);
        } else {
            getResult = new GetResult(WatchStore.INDEX, WatchStore.DOC_TYPE, request.watchName(), -1, false, null, null);
        }
        listener.onResponse(new GetWatchResponse(new GetResponse(getResult)));
    }

    @Override
    protected ClusterBlockException checkBlock(GetWatchRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.READ, WatchStore.INDEX);
    }
}
