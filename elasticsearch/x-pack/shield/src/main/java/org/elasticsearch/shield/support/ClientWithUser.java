/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.support;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.FilterClient;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authc.AuthenticationService;

import java.io.IOException;

/**
 * A client that takes a user and attaches it to all requests
 */
public class ClientWithUser extends FilterClient {

    private final AuthenticationService authenticationService;
    private final User user;

    /**
     * Creates a new client that will attach the user to each request.
     *
     * @param in the client to delegate to
     * @param authenticationService the authentication service that will be used for attaching the user
     * @param user the user to attach to each request
     * @see #in()
     */
    public ClientWithUser(Client in, AuthenticationService authenticationService, User user) {
        super(in);
        this.authenticationService = authenticationService;
        this.user = user;
    }

    @Override
    protected <Request extends ActionRequest<Request>, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void doExecute(Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
        try (ThreadContext.StoredContext ctx = threadPool().getThreadContext().stashContext()) {
            try {
                authenticationService.attachUserHeaderIfMissing(user);
            } catch (IOException e) {
                throw new ElasticsearchException("failed to set user [{}]", e, user.principal());
            }
            super.doExecute(action, request, listener);
        }
    }
}
