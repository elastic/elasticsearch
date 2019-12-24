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
        super(RefreshTokenAction.NAME, transportService, actionFilters, CreateTokenRequest::new);
        this.tokenService = tokenService;
    }

    @Override
    protected void doExecute(Task task, CreateTokenRequest request, ActionListener<CreateTokenResponse> listener) {
        tokenService.refreshToken(request.getRefreshToken(), ActionListener.wrap(tuple -> {
            final String scope = getResponseScopeValue(request.getScope());
            final CreateTokenResponse response =
                    new CreateTokenResponse(tuple.v1(), tokenService.getExpirationDelay(), scope, tuple.v2(), null);
            listener.onResponse(response);
        }, listener::onFailure));
    }
}
