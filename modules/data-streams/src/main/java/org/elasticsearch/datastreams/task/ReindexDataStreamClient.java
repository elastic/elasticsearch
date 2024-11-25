/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.task;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ClientHelperService;
import org.elasticsearch.client.internal.support.AbstractClient;

import java.util.Map;

public class ReindexDataStreamClient extends AbstractClient {

    private final ClientHelperService clientHelperService;
    private final Client client;
    private final Map<String, String> headers;

    public ReindexDataStreamClient(ClientHelperService clientHelperService, Client client, Map<String, String> headers) {
        super(client.settings(), client.threadPool());
        this.clientHelperService = clientHelperService;
        this.client = client;
        this.headers = headers;
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        clientHelperService.executeWithHeadersAsync(headers, "", client, action, request, listener);
    }

}
