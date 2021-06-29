/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.token;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.settings.SecureString;

/**
 * Request builder used to populate a {@link CreateTokenRequest}
 */
public final class CreateTokenRequestBuilder
        extends ActionRequestBuilder<CreateTokenRequest, CreateTokenResponse> {

    public CreateTokenRequestBuilder(ElasticsearchClient client, ActionType<CreateTokenResponse> action) {
        super(client, action, new CreateTokenRequest());
    }

    /**
     * Specifies the grant type for this request. Currently only <code>password</code> is supported
     */
    public CreateTokenRequestBuilder setGrantType(String grantType) {
        request.setGrantType(grantType);
        return this;
    }

    /**
     * Set the username to be used for authentication with a password grant
     */
    public CreateTokenRequestBuilder setUsername(@Nullable String username) {
        request.setUsername(username);
        return this;
    }

    /**
     * Set the password credentials associated with the user. These credentials will be used for
     * authentication and the resulting token will be for this user
     */
    public CreateTokenRequestBuilder setPassword(@Nullable SecureString password) {
        request.setPassword(password);
        return this;
    }

    /**
     * Set the scope of the access token. A <code>null</code> scope implies the default scope. If
     * the requested scope differs from the scope of the token, the token's scope will be returned
     * in the response
     */
    public CreateTokenRequestBuilder setScope(@Nullable String scope) {
        request.setScope(scope);
        return this;
    }

    public CreateTokenRequestBuilder setRefreshToken(@Nullable String refreshToken) {
        request.setRefreshToken(refreshToken);
        return this;
    }
}
