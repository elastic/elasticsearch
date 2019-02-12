/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.token;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenAction;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenRequest;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.TokenService;

import java.io.IOException;
import java.util.Collections;

/**
 * Transport action responsible for creating a token based on a request. Requests provide user
 * credentials that can be different than those of the user that is currently authenticated so we
 * always re-authenticate within this action. This authenticated user will be the user that the
 * token represents
 */
public final class TransportCreateTokenAction extends HandledTransportAction<CreateTokenRequest, CreateTokenResponse> {

    private static final String DEFAULT_SCOPE = "full";
    private final ThreadPool threadPool;
    private final TokenService tokenService;
    private final AuthenticationService authenticationService;

    @Inject
    public TransportCreateTokenAction(ThreadPool threadPool, TransportService transportService, ActionFilters actionFilters,
                                      TokenService tokenService, AuthenticationService authenticationService) {
        super(CreateTokenAction.NAME, transportService, actionFilters, CreateTokenRequest::new);
        this.threadPool = threadPool;
        this.tokenService = tokenService;
        this.authenticationService = authenticationService;
    }

    @Override
    protected void doExecute(Task task, CreateTokenRequest request, ActionListener<CreateTokenResponse> listener) {
        CreateTokenRequest.GrantType type = CreateTokenRequest.GrantType.fromString(request.getGrantType());
        assert type != null : "type should have been validated in the action";
        switch (type) {
            case PASSWORD:
                authenticateAndCreateToken(request, listener);
                break;
            case CLIENT_CREDENTIALS:
                Authentication authentication = Authentication.getAuthentication(threadPool.getThreadContext());
                createToken(request, authentication, authentication, false, listener);
                break;
            default:
                listener.onFailure(new IllegalStateException("grant_type [" + request.getGrantType() +
                    "] is not supported by the create token action"));
                break;
        }
    }

    private void authenticateAndCreateToken(CreateTokenRequest request, ActionListener<CreateTokenResponse> listener) {
        Authentication originatingAuthentication = Authentication.getAuthentication(threadPool.getThreadContext());
        try (ThreadContext.StoredContext ignore = threadPool.getThreadContext().stashContext()) {
            final UsernamePasswordToken authToken = new UsernamePasswordToken(request.getUsername(), request.getPassword());
            authenticationService.authenticate(CreateTokenAction.NAME, request, authToken,
                ActionListener.wrap(authentication -> {
                    request.getPassword().close();
                    createToken(request, authentication, originatingAuthentication, true, listener);
                }, e -> {
                    // clear the request password
                    request.getPassword().close();
                    listener.onFailure(e);
                }));
        }
    }

    private void createToken(CreateTokenRequest request, Authentication authentication, Authentication originatingAuth,
                             boolean includeRefreshToken, ActionListener<CreateTokenResponse> listener) {
        try {
            tokenService.createUserToken(authentication, originatingAuth, ActionListener.wrap(tuple -> {
                final String tokenStr = tokenService.getUserTokenString(tuple.v1());
                final String scope = getResponseScopeValue(request.getScope());

                final CreateTokenResponse response =
                    new CreateTokenResponse(tokenStr, tokenService.getExpirationDelay(), scope, tuple.v2());
                listener.onResponse(response);
            }, listener::onFailure), Collections.emptyMap(), includeRefreshToken);
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }

    static String getResponseScopeValue(String requestScope) {
        final String scope;
        // the OAuth2.0 RFC requires the scope to be provided in the
        // response if it differs from the user provided scope. If the
        // scope was not provided then it does not need to be returned.
        // if the scope is not supported, the value of the scope that the
        // token is for must be returned
        if (requestScope != null) {
            scope = DEFAULT_SCOPE; // this is the only non-null value that is currently supported
        } else {
            scope = null;
        }
        return scope;
    }
}
