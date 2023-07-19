/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

public class TransportGetSecretAction extends HandledTransportAction<GetSecretRequest, GetSecretResponse> {
    private final Client client; // TODO: use

    @Inject
    public TransportGetSecretAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(GetSecretAction.NAME, transportService, actionFilters, GetSecretRequest::new);
        this.client = client;
    }

    protected void doExecute(Task task, GetSecretRequest request, ActionListener<GetSecretResponse> listener) {
        listener.onResponse(new GetSecretResponse(request.id(), "foo")); // TODO: actual impl
    }
}
