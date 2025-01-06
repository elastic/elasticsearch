/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.application.connector.ConnectorIndexService;

import static org.elasticsearch.xpack.core.ClientHelper.ENT_SEARCH_ORIGIN;

public class TransportDeleteConnectorAction extends HandledTransportAction<DeleteConnectorAction.Request, AcknowledgedResponse> {

    protected final ConnectorIndexService connectorIndexService;

    @Inject
    public TransportDeleteConnectorAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(
            DeleteConnectorAction.NAME,
            transportService,
            actionFilters,
            DeleteConnectorAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.connectorIndexService = new ConnectorIndexService(new OriginSettingClient(client, ENT_SEARCH_ORIGIN));
    }

    @Override
    protected void doExecute(Task task, DeleteConnectorAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        String connectorId = request.getConnectorId();
        boolean shouldDeleteSyncJobs = request.shouldDeleteSyncJobs();
        connectorIndexService.deleteConnector(connectorId, shouldDeleteSyncJobs, listener.map(v -> AcknowledgedResponse.TRUE));
    }
}
