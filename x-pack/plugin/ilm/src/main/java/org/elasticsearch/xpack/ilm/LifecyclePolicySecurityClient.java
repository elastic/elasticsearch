/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ilm.Step;

import java.util.Map;

/**
 * This class wraps a client and calls the client using the headers provided in
 * constructor. The intent is to abstract away the fact that there are headers
 * so {@link Step}s etc. can call this client as if it was a normal client.
 *
 * Note: This client will not close the wrapped {@link Client} instance since
 * the intent is that the wrapped client is shared between multiple instances of
 * this class.
 */
public class LifecyclePolicySecurityClient extends AbstractClient {

    private Client client;
    private Map<String, String> headers;
    private String origin;

    public LifecyclePolicySecurityClient(Client client, String origin, Map<String, String> headers) {
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
    protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(ActionType<Response> action, Request request,
                                                                                              ActionListener<Response> listener) {
        ClientHelper.executeWithHeadersAsync(headers, origin, client, action, request, listener);
    }

}
