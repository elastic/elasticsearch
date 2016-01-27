/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.shield;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.FilterClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.ThreadContext;

/**
 *
 */
public class SecuredClient extends FilterClient {

    private MarvelShieldIntegration shieldIntegration;

    @Inject
    public SecuredClient(Client in, MarvelShieldIntegration shieldIntegration) {
        super(in);
        this.shieldIntegration = shieldIntegration;
    }

    @Override
    protected <Request extends ActionRequest<Request>, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void doExecute(
                Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
        try (ThreadContext.StoredContext ctx = threadPool().getThreadContext().stashContext()) {
            this.shieldIntegration.bindInternalMarvelUser();
            super.doExecute(action, request, listener);
        }
    }
}
