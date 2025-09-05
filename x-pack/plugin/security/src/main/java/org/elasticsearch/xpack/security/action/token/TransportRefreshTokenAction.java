/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.token;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenRequest;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenResponse;
import org.elasticsearch.xpack.core.security.action.token.RefreshTokenAction;
import org.elasticsearch.xpack.security.authc.TokenService;

import static org.elasticsearch.xpack.security.action.token.TransportCreateTokenAction.getResponseScopeValue;

public class TransportRefreshTokenAction extends HandledTransportAction<CreateTokenRequest, CreateTokenResponse> {

    private final TokenService tokenService;

    @Inject
    public TransportRefreshTokenAction(TransportService transportService, ActionFilters actionFilters, TokenService tokenService) {
        super(RefreshTokenAction.NAME, transportService, actionFilters, CreateTokenRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.tokenService = tokenService;
    }

    @Override
    protected void doExecute(Task task, CreateTokenRequest request, ActionListener<CreateTokenResponse> listener) {
        tokenService.refreshToken(request.getRefreshToken(), ActionListener.wrap(tokenResult -> {
            final String scope = getResponseScopeValue(request.getScope());
            final CreateTokenResponse response = new CreateTokenResponse(
                tokenResult.getAccessToken(),
                tokenService.getExpirationDelay(),
                scope,
                tokenResult.getRefreshToken(),
                null,
                tokenResult.getAuthentication()
            );
            listener.onResponse(response);
        }, listener::onFailure));
    }
}
