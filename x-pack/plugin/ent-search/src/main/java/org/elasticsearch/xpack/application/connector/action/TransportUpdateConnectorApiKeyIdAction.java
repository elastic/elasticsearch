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
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.application.connector.ConnectorIndexService;

public class TransportUpdateConnectorApiKeyIdAction extends HandledTransportAction<
    UpdateConnectorApiKeyIdAction.Request,
    ConnectorUpdateActionResponse> {

    protected final ConnectorIndexService connectorIndexService;

    @Inject
    public TransportUpdateConnectorApiKeyIdAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(
            UpdateConnectorApiKeyIdAction.NAME,
            transportService,
            actionFilters,
            UpdateConnectorApiKeyIdAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.connectorIndexService = new ConnectorIndexService(client);
    }

    @Override
    protected void doExecute(
        Task task,
        UpdateConnectorApiKeyIdAction.Request request,
        ActionListener<ConnectorUpdateActionResponse> listener
    ) {
        connectorIndexService.updateConnectorApiKeyIdOrApiKeySecretId(
            request,
            listener.map(r -> new ConnectorUpdateActionResponse(r.getResult()))
        );
    }
}
