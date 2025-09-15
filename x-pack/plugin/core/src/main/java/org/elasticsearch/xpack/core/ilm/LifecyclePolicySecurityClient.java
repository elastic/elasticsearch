/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.support.AbstractClient;
import org.elasticsearch.xpack.core.ClientHelper;

import java.util.Map;

/**
 * This class wraps a client and calls the client using the headers provided in
 * constructor. The intent is to abstract away the fact that there are headers
 * so {@link Step}s etc. can call this client as if it was a normal client.
 * <p>
 * Note: This client will not close the wrapped {@link Client} instance since
 * the intent is that the wrapped client is shared between multiple instances of
 * this class.
 */
public class LifecyclePolicySecurityClient extends AbstractClient {

    private final Client client;
    private final Map<String, String> headers;
    private final String origin;

    public LifecyclePolicySecurityClient(Client client, String origin, Map<String, String> headers) {
        super(client.settings(), client.threadPool());
        this.client = client;
        this.origin = origin;
        this.headers = headers;
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        ClientHelper.executeWithHeadersAsync(headers, origin, client, action, request, listener);
    }

}
