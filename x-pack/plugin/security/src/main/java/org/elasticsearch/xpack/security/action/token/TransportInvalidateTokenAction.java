/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.token;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.token.InvalidateTokenAction;
import org.elasticsearch.xpack.core.security.action.token.InvalidateTokenRequest;
import org.elasticsearch.xpack.core.security.action.token.InvalidateTokenResponse;
import org.elasticsearch.xpack.core.security.authc.support.TokensInvalidationResult;
import org.elasticsearch.xpack.security.authc.TokenService;

/**
 * Transport action responsible for handling invalidation of tokens
 */
public final class TransportInvalidateTokenAction extends HandledTransportAction<InvalidateTokenRequest, InvalidateTokenResponse> {

    private final TokenService tokenService;

    @Inject
    public TransportInvalidateTokenAction(TransportService transportService, ActionFilters actionFilters, TokenService tokenService) {
        super(InvalidateTokenAction.NAME, transportService, actionFilters, InvalidateTokenRequest::new);
        this.tokenService = tokenService;
    }

    @Override
    protected void doExecute(Task task, InvalidateTokenRequest request, ActionListener<InvalidateTokenResponse> listener) {
        final ActionListener<TokensInvalidationResult> invalidateListener =
            ActionListener.wrap(tokensInvalidationResult ->
                listener.onResponse(new InvalidateTokenResponse(tokensInvalidationResult)), listener::onFailure);
        if (Strings.hasText(request.getUserName()) || Strings.hasText(request.getRealmName())) {
            tokenService.invalidateActiveTokensForRealmAndUser(request.getRealmName(), request.getUserName(), invalidateListener);
        } else if (request.getTokenType() == InvalidateTokenRequest.Type.ACCESS_TOKEN) {
            tokenService.invalidateAccessToken(request.getTokenString(), invalidateListener);
        } else {
            assert request.getTokenType() == InvalidateTokenRequest.Type.REFRESH_TOKEN;
            tokenService.invalidateRefreshToken(request.getTokenString(), invalidateListener);
        }
    }
}
