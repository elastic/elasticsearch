/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc.apikey;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;

/**
 * An extension point to provide a custom API key authenticator implementation.
 * The implementation is wrapped by a core Authenticator class and included in the authenticator chain _before_ the
 * default API key authenticator.
 */
public interface CustomApiKeyAuthenticator {
    String name();

    AuthenticationToken extractCredentials(@Nullable SecureString apiKeyCredentials);

    void authenticate(@Nullable AuthenticationToken authenticationToken, ActionListener<AuthenticationResult<Authentication>> listener);

    /**
     * A no-op implementation of {@link CustomApiKeyAuthenticator} that does nothing and is effectively skipped in the authenticator chain.
     */
    class Noop implements CustomApiKeyAuthenticator {
        @Override
        public String name() {
            return "noop";
        }

        @Override
        public AuthenticationToken extractCredentials(@Nullable SecureString apiKeyCredentials) {
            return null;
        }

        @Override
        public void authenticate(
            @Nullable AuthenticationToken authenticationToken,
            ActionListener<AuthenticationResult<Authentication>> listener
        ) {
            listener.onResponse(AuthenticationResult.notHandled());
        }
    }
}
