/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import static org.elasticsearch.xpack.core.ClientHelper.FLEET_ORIGIN;
import static org.elasticsearch.xpack.fleet.Fleet.FLEET_SECRETS_INDEX_NAME;

public class TransportGetSecretAction extends HandledTransportAction<GetSecretRequest, GetSecretResponse> {
    private final Client client;

    @Inject
    public TransportGetSecretAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(GetSecretAction.NAME, transportService, actionFilters, GetSecretRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.client = new OriginSettingClient(client, FLEET_ORIGIN);
    }

    protected void doExecute(Task task, GetSecretRequest request, ActionListener<GetSecretResponse> listener) {
        client.prepareGet(FLEET_SECRETS_INDEX_NAME, request.id()).execute(listener.delegateFailureAndWrap((delegate, getResponse) -> {
            if (getResponse.isSourceEmpty()) {
                delegate.onFailure(new ResourceNotFoundException("No secret with id [" + request.id() + "]"));
                return;
            }
            delegate.onResponse(new GetSecretResponse(getResponse.getId(), getResponse.getSource().get("value").toString()));
        }));
    }
}
