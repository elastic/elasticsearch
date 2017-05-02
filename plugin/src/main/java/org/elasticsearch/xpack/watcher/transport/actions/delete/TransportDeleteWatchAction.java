/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.actions.delete;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.watcher.support.init.proxy.WatcherClientProxy;

/**
 * Performs the delete operation. This inherits directly from HandledTransportAction, because deletion should always work
 * independently from the license check in WatcherTransportAction!
 */
public class TransportDeleteWatchAction extends HandledTransportAction<DeleteWatchRequest, DeleteWatchResponse> {

    private final WatcherClientProxy client;

    @Inject
    public TransportDeleteWatchAction(Settings settings, TransportService transportService,ThreadPool threadPool,
                                      ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                      WatcherClientProxy client) {
        super(settings, DeleteWatchAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                DeleteWatchRequest::new);
        this.client = client;
    }

    @Override
    protected void doExecute(DeleteWatchRequest request, ActionListener<DeleteWatchResponse> listener) {
        client.deleteWatch(request.getId(), ActionListener.wrap(deleteResponse -> {
                    boolean deleted = deleteResponse.getResult() == DocWriteResponse.Result.DELETED;
                    DeleteWatchResponse response = new DeleteWatchResponse(deleteResponse.getId(), deleteResponse.getVersion(), deleted);
                    listener.onResponse(response);
                },
                listener::onFailure));
    }
}
