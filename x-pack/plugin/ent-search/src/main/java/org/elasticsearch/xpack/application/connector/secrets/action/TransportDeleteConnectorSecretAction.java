/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.secrets.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.application.connector.secrets.ConnectorSecretsIndexService;

public class TransportDeleteConnectorSecretAction extends TransportAction<DeleteConnectorSecretRequest, DeleteConnectorSecretResponse> {

    private final ConnectorSecretsIndexService connectorSecretsIndexService;

    @Inject
    public TransportDeleteConnectorSecretAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(DeleteConnectorSecretAction.NAME, actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.connectorSecretsIndexService = new ConnectorSecretsIndexService(client);
    }

    protected void doExecute(Task task, DeleteConnectorSecretRequest request, ActionListener<DeleteConnectorSecretResponse> listener) {
        connectorSecretsIndexService.deleteSecret(request.id(), listener);
    }
}
