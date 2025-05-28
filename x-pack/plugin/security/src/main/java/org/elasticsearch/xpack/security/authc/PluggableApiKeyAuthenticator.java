/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.apikey.CustomApiKeyAuthenticator;

public class PluggableApiKeyAuthenticator implements Authenticator {
    private final CustomApiKeyAuthenticator authenticator;

    public PluggableApiKeyAuthenticator(CustomApiKeyAuthenticator authenticator) {
        this.authenticator = authenticator;
    }

    @Override
    public String name() {
        return authenticator.name();
    }

    @Override
    public AuthenticationToken extractCredentials(Context context) {
        return authenticator.extractCredentials(context.getApiKeyString());
    }

    @Override
    public void authenticate(Context context, ActionListener<AuthenticationResult<Authentication>> listener) {
        final AuthenticationToken authenticationToken = context.getMostRecentAuthenticationToken();
        authenticator.authenticate(
            authenticationToken,
            ActionListener.wrap(
                listener::onResponse,
                ex -> listener.onFailure(context.getRequest().exceptionProcessingRequest(ex, authenticationToken))
            )
        );
    }
}
