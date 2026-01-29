/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.saml;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.security.action.saml.SamlAuthenticateResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.TokenService;

import java.time.Clock;

/**
 * Interface for handling successful SAML authentications.
 */
public interface SamlAuthenticateResponseHandler {

    /**
     * Called to handle and return a ({@link SamlAuthenticateResponse}) after successful SAML authentication.
     */
    void handleTokenResponse(
        Authentication authentication,
        Authentication originatingAuthentication,
        AuthenticationResult<User> authenticationResult,
        ActionListener<SamlAuthenticateResponse> listener
    );

    /**
     * The factory is used to make handler pluggable.
     */
    interface Factory {
        SamlAuthenticateResponseHandler create(Settings settings, TokenService tokenService, Clock clock);
    }

    /**
     * The default factory that creates {@link DefaultSamlAuthenticateResponseHandler}.
     */
    class DefaultFactory implements Factory {

        @Override
        public SamlAuthenticateResponseHandler create(Settings settings, TokenService tokenService, Clock clock) {
            return new DefaultSamlAuthenticateResponseHandler(tokenService);
        }
    }
}
