/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse.Result;
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

public class TransportDeleteSecretAction extends HandledTransportAction<DeleteSecretRequest, DeleteSecretResponse> {

    private final Client client;

    @Inject
    public TransportDeleteSecretAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(DeleteSecretAction.NAME, transportService, actionFilters, DeleteSecretRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.client = new OriginSettingClient(client, FLEET_ORIGIN);
    }

    @Override
    protected void doExecute(Task task, DeleteSecretRequest request, ActionListener<DeleteSecretResponse> listener) {
        client.prepareDelete(FLEET_SECRETS_INDEX_NAME, request.id()).execute(listener.delegateFailureAndWrap((delegate, deleteResponse) -> {
            if (deleteResponse.getResult() == Result.NOT_FOUND) {
                delegate.onFailure(new ResourceNotFoundException("No secret with id [" + request.id() + "]"));
                return;
            }
            delegate.onResponse(new DeleteSecretResponse(deleteResponse.getResult() == Result.DELETED));
        }));
    }
}
