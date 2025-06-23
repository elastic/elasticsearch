/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.secrets.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.application.connector.secrets.ConnectorSecretsIndexService;

public class TransportPostConnectorSecretAction extends HandledTransportAction<PostConnectorSecretRequest, PostConnectorSecretResponse> {

    private final ConnectorSecretsIndexService connectorSecretsIndexService;

    @Inject
    public TransportPostConnectorSecretAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(
            PostConnectorSecretAction.NAME,
            transportService,
            actionFilters,
            PostConnectorSecretRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.connectorSecretsIndexService = new ConnectorSecretsIndexService(client);
    }

    protected void doExecute(Task task, PostConnectorSecretRequest request, ActionListener<PostConnectorSecretResponse> listener) {
        connectorSecretsIndexService.createSecret(request, listener);
    }
}
