/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.support.BearerToken;

class OAuth2TokenAuthenticator implements Authenticator {

    private static final Logger logger = LogManager.getLogger(OAuth2TokenAuthenticator.class);
    private final TokenService tokenService;

    OAuth2TokenAuthenticator(TokenService tokenService) {
        this.tokenService = tokenService;
    }

    @Override
    public String name() {
        return "oauth2 token";
    }

    @Override
    public AuthenticationToken extractCredentials(Context context) {
        final SecureString bearerString = context.getBearerString();
        return bearerString == null ? null : new BearerToken(bearerString);
    }

    @Override
    public void authenticate(Context context, ActionListener<AuthenticationResult<Authentication>> listener) {
        final AuthenticationToken authenticationToken = context.getMostRecentAuthenticationToken();
        if (false == authenticationToken instanceof BearerToken) {
            listener.onResponse(AuthenticationResult.notHandled());
            return;
        }
        final BearerToken bearerToken = (BearerToken) authenticationToken;
        tokenService.tryAuthenticateToken(bearerToken.credentials(), ActionListener.wrap(userToken -> {
            if (userToken != null) {
                listener.onResponse(AuthenticationResult.success(userToken.getAuthentication()));
            } else {
                listener.onResponse(AuthenticationResult.unsuccessful("invalid token", null));
            }
        }, e -> {
            logger.debug(new ParameterizedMessage("Failed to validate token authentication for request [{}]", context.getRequest()), e);
            if (e instanceof ElasticsearchSecurityException
                && false == TokenService.isExpiredTokenException((ElasticsearchSecurityException) e)) {
                // intentionally ignore the returned exception; we call this primarily
                // for the auditing as we already have a purpose built exception
                context.getRequest().tamperedRequest();
            }
            listener.onFailure(e);
        }));
    }
}
