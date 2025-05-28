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

public interface CustomApiKeyAuthenticator {
    String name();

    AuthenticationToken extractCredentials(@Nullable SecureString apiKeyCredentials);

    void authenticate(@Nullable AuthenticationToken authenticationToken, ActionListener<AuthenticationResult<Authentication>> listener);

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
