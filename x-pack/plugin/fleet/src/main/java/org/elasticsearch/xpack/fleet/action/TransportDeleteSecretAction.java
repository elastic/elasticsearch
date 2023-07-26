/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import static org.elasticsearch.xpack.core.ClientHelper.FLEET_ORIGIN;

public class TransportDeleteSecretAction extends HandledTransportAction<DeleteSecretRequest, DeleteSecretResponse> {

    private final Client client;

    @Inject
    public TransportDeleteSecretAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(DeleteSecretAction.NAME, transportService, actionFilters, DeleteSecretRequest::new);
        this.client = new OriginSettingClient(client, FLEET_ORIGIN);
    }

    protected void doExecute(Task task, DeleteSecretRequest request, ActionListener<DeleteSecretResponse> listener) {
        client.prepareDelete(".fleet-secrets", request.id())
            .execute(
                ActionListener.wrap(
                    deleteResponse -> listener.onResponse(
                        new DeleteSecretResponse(deleteResponse.getResult() == DocWriteResponse.Result.DELETED)
                    ),
                    listener::onFailure
                )
            ); // TODO: check impl and failure handling
    }
}
