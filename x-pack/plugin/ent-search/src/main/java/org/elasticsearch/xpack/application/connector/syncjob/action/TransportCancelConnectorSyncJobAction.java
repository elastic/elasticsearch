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
import org.elasticsearch.xpack.application.connector.action.ConnectorUpdateActionResponse;
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJobIndexService;

public class TransportCancelConnectorSyncJobAction extends HandledTransportAction<
    CancelConnectorSyncJobAction.Request,
    ConnectorUpdateActionResponse> {

    protected ConnectorSyncJobIndexService connectorSyncJobIndexService;

    @Inject
    public TransportCancelConnectorSyncJobAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(
            CancelConnectorSyncJobAction.NAME,
            transportService,
            actionFilters,
            CancelConnectorSyncJobAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.connectorSyncJobIndexService = new ConnectorSyncJobIndexService(client);
    }

    @Override
    protected void doExecute(
        Task task,
        CancelConnectorSyncJobAction.Request request,
        ActionListener<ConnectorUpdateActionResponse> listener
    ) {
        connectorSyncJobIndexService.cancelConnectorSyncJob(
            request.getConnectorSyncJobId(),
            listener.map(r -> new ConnectorUpdateActionResponse(r.getResult()))
        );
    }
}
