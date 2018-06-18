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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.token.InvalidateTokenAction;
import org.elasticsearch.xpack.core.security.action.token.InvalidateTokenRequest;
import org.elasticsearch.xpack.core.security.action.token.InvalidateTokenResponse;
import org.elasticsearch.xpack.security.authc.TokenService;

/**
 * Transport action responsible for handling invalidation of tokens
 */
public final class TransportInvalidateTokenAction extends HandledTransportAction<InvalidateTokenRequest, InvalidateTokenResponse> {

    private final TokenService tokenService;

    @Inject
    public TransportInvalidateTokenAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                          ActionFilters actionFilters, TokenService tokenService) {
        super(settings, InvalidateTokenAction.NAME, threadPool, transportService, actionFilters,
            InvalidateTokenRequest::new);
        this.tokenService = tokenService;
    }

    @Override
    protected void doExecute(InvalidateTokenRequest request, ActionListener<InvalidateTokenResponse> listener) {
        final ActionListener<Boolean> invalidateListener =
                ActionListener.wrap(created -> listener.onResponse(new InvalidateTokenResponse(created)), listener::onFailure);
        if (request.getTokenType() == InvalidateTokenRequest.Type.ACCESS_TOKEN) {
            tokenService.invalidateAccessToken(request.getTokenString(), invalidateListener);
        } else {
            assert request.getTokenType() == InvalidateTokenRequest.Type.REFRESH_TOKEN;
            tokenService.invalidateRefreshToken(request.getTokenString(), invalidateListener);
        }
    }
}
