/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.saml;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.security.action.saml.SamlAuthenticateResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.TokenService;

import java.util.Map;

/**
 * Default implementation of {@link SamlAuthenticateResponseHandler} that returns tokens crested using the {@link TokenService}.
 */
public final class DefaultSamlAuthenticateResponseHandler implements SamlAuthenticateResponseHandler {

    private final TokenService tokenService;

    public DefaultSamlAuthenticateResponseHandler(TokenService tokenService) {
        this.tokenService = tokenService;
    }

    @Override
    public void handleTokenResponse(
        Authentication authentication,
        Authentication originatingAuthentication,
        AuthenticationResult<User> authenticationResult,
        ActionListener<SamlAuthenticateResponse> listener
    ) {
        @SuppressWarnings("unchecked")
        final Map<String, Object> tokenMeta = (Map<String, Object>) authenticationResult.getMetadata().get(SamlRealm.CONTEXT_TOKEN_DATA);
        final String inResponseTo = (String) tokenMeta.get(SamlRealm.TOKEN_METADATA_IN_RESPONSE_TO);
        tokenService.createOAuth2Tokens(authentication, originatingAuthentication, tokenMeta, true, ActionListener.wrap(tokenResult -> {
            final TimeValue expiresIn = tokenService.getExpirationDelay();
            listener.onResponse(
                new SamlAuthenticateResponse(
                    authentication,
                    tokenResult.getAccessToken(),
                    tokenResult.getRefreshToken(),
                    expiresIn,
                    inResponseTo
                )
            );
        }, listener::onFailure));
    }
}
