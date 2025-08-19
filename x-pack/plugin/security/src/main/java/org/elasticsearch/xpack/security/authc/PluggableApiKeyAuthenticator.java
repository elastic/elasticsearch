/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.apikey.CustomTokenAuthenticator;

/**
 * An adapter for {@link CustomTokenAuthenticator} that implements the {@link Authenticator} interface, so the custom API key authenticator
 * can be plugged into the authenticator chain. Module dependencies prevent us from introducing a direct extension point for
 * an {@link Authenticator}.
 */
public class PluggableApiKeyAuthenticator extends AbstractPluggableAuthenticator {
    private final CustomTokenAuthenticator apiKeyAuthenticator;

    public PluggableApiKeyAuthenticator(CustomTokenAuthenticator apiKeyAuthenticator) {
        this.apiKeyAuthenticator = apiKeyAuthenticator;
    }

    @Override
    public String name() {
        return apiKeyAuthenticator.name();
    }

    @Override
    public AuthenticationToken extractCredentials(Authenticator.Context context) {
        return apiKeyAuthenticator.extractCredentials(context.getApiKeyString());

    }

    @Override
    public CustomTokenAuthenticator getAuthenticator() {
        return apiKeyAuthenticator;
    }
}
