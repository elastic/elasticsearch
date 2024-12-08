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

public class TransportGetConnectorSyncJobAction extends HandledTransportAction<
    GetConnectorSyncJobAction.Request,
    GetConnectorSyncJobAction.Response> {

    protected final ConnectorSyncJobIndexService connectorSyncJobIndexService;

    @Inject
    public TransportGetConnectorSyncJobAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(
            GetConnectorSyncJobAction.NAME,
            transportService,
            actionFilters,
            GetConnectorSyncJobAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.connectorSyncJobIndexService = new ConnectorSyncJobIndexService(client);
    }

    @Override
    protected void doExecute(
        Task task,
        GetConnectorSyncJobAction.Request request,
        ActionListener<GetConnectorSyncJobAction.Response> listener
    ) {
        connectorSyncJobIndexService.getConnectorSyncJob(
            request.getConnectorSyncJobId(),
            listener.map(GetConnectorSyncJobAction.Response::new)
        );
    }
}
