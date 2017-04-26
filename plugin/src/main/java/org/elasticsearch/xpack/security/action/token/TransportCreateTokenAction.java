/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.token;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.TokenService;
import org.elasticsearch.xpack.security.authc.UserToken;

/**
 * Transport action responsible for creating a token based on a request. Requests provide user
 * credentials that can be different than those of the user that is currently authenticated so we
 * always re-authenticate within this action. This authenticated user will be the user that the
 * token represents
 */
public final class TransportCreateTokenAction extends HandledTransportAction<CreateTokenRequest, CreateTokenResponse> {

    private static final String DEFAULT_SCOPE = "full";
    private final TokenService tokenService;
    private final AuthenticationService authenticationService;

    @Inject
    public TransportCreateTokenAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                      ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                      TokenService tokenService, AuthenticationService authenticationService) {
        super(settings, CreateTokenAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                CreateTokenRequest::new);
        this.tokenService = tokenService;
        this.authenticationService = authenticationService;
    }

    @Override
    protected void doExecute(CreateTokenRequest request, ActionListener<CreateTokenResponse> listener) {
        try (ThreadContext.StoredContext ignore = threadPool.getThreadContext().stashContext()) {
            authenticationService.authenticate(CreateTokenAction.NAME, request,
                    request.getUsername(), request.getPassword(),
                    ActionListener.wrap(authentication -> {
                        try (SecureString ignore1 = request.getPassword()) {
                            final UserToken token = tokenService.createUserToken(authentication);
                            final String tokenStr = tokenService.getUserTokenString(token);
                            final String scope;
                            // the OAuth2.0 RFC requires the scope to be provided in the
                            // response if it differs from the user provided scope. If the
                            // scope was not provided then it does not need to be returned.
                            // if the scope is not supported, the value of the scope that the
                            // token is for must be returned
                            if (request.getScope() != null) {
                                scope = DEFAULT_SCOPE; // this is the only non-null value that is currently supported
                            } else {
                                scope = null;
                            }

                            listener.onResponse(new CreateTokenResponse(tokenStr, tokenService.getExpirationDelay(), scope));
                        }
                    }, e -> {
                        // clear the request password
                        request.getPassword().close();
                        listener.onFailure(e);
                    }));
        }
    }
}
