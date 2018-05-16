/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.xpack.core.ClientHelper;

import java.util.Map;

public class LifecyclePolicyClient extends AbstractClient {

    private Client client;
    private Map<String, String> headers;
    private String origin;

    public LifecyclePolicyClient(Client client, String origin, Map<String, String> headers) {
        super(client.settings(), client.threadPool());
        this.client = client;
        this.origin = origin;
        this.headers = headers;
    }

    @Override
    public void close() {
        // Doesn't close the wrapped client since this client object is shared
        // among multiple instances
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void doExecute(
            Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
        ClientHelper.executeWithHeadersAsync(headers, origin, client, action, request, listener);
    }

}
