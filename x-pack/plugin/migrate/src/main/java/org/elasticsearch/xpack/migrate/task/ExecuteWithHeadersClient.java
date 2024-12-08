/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.task;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.support.AbstractClient;
import org.elasticsearch.xpack.core.ClientHelper;

import java.util.Map;

public class ExecuteWithHeadersClient extends AbstractClient {

    private final Client client;
    private final Map<String, String> headers;

    public ExecuteWithHeadersClient(Client client, Map<String, String> headers) {
        super(client.settings(), client.threadPool());
        this.client = client;
        this.headers = headers;
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        ClientHelper.executeWithHeadersAsync(headers, null, client, action, request, listener);
    }

}
