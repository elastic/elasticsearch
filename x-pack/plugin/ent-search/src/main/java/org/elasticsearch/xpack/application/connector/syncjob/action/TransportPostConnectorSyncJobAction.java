/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.syncjob.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJobIndexService;

public class TransportPostConnectorSyncJobAction extends HandledTransportAction<
    PostConnectorSyncJobAction.Request,
    PostConnectorSyncJobAction.Response> {

    protected final ConnectorSyncJobIndexService syncJobIndexService;

    @Inject
    public TransportPostConnectorSyncJobAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(
            PostConnectorSyncJobAction.NAME,
            transportService,
            actionFilters,
            PostConnectorSyncJobAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.syncJobIndexService = new ConnectorSyncJobIndexService(client);
    }

    @Override
    protected void doExecute(
        Task task,
        PostConnectorSyncJobAction.Request request,
        ActionListener<PostConnectorSyncJobAction.Response> listener
    ) {
        syncJobIndexService.createConnectorSyncJob(request, listener);
    }
}
