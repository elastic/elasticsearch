/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.secrets.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import static org.elasticsearch.xpack.application.connector.secrets.ConnectorSecretsIndexService.CONNECTOR_SECRETS_INDEX_NAME;
import static org.elasticsearch.xpack.core.ClientHelper.CONNECTORS_ORIGIN;

public class TransportGetConnectorSecretAction extends HandledTransportAction<GetConnectorSecretRequest, GetConnectorSecretResponse> {
    private final Client client;

    @Inject
    public TransportGetConnectorSecretAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(GetConnectorSecretAction.NAME, transportService, actionFilters, GetConnectorSecretRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.client = new OriginSettingClient(client, CONNECTORS_ORIGIN);
    }

    protected void doExecute(Task task, GetConnectorSecretRequest request, ActionListener<GetConnectorSecretResponse> listener) {
        client.prepareGet(CONNECTOR_SECRETS_INDEX_NAME, request.id()).execute(listener.delegateFailureAndWrap((delegate, getResponse) -> {
            if (getResponse.isSourceEmpty()) {
                delegate.onFailure(new ResourceNotFoundException("No secret with id [" + request.id() + "]"));
                return;
            }
            delegate.onResponse(new GetConnectorSecretResponse(getResponse.getId(), getResponse.getSource().get("value").toString()));
        }));
    }
}
