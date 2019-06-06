/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.token;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Request builder that is used to populate a {@link InvalidateTokenRequest}
 */
public final class InvalidateTokenRequestBuilder
        extends ActionRequestBuilder<InvalidateTokenRequest, InvalidateTokenResponse> {

    public InvalidateTokenRequestBuilder(ElasticsearchClient client) {
        super(client, InvalidateTokenAction.INSTANCE, new InvalidateTokenRequest());
    }

    /**
     * The string representation of the token that is being invalidated. This is the value returned
     * from a create token request.
     */
    public InvalidateTokenRequestBuilder setTokenString(String token) {
        request.setTokenString(token);
        return this;
    }

    /**
     * Sets the type of the token that should be invalidated
     */
    public InvalidateTokenRequestBuilder setType(InvalidateTokenRequest.Type type) {
        request.setTokenType(type);
        return this;
    }

    /**
     * Sets the name of the realm for which all tokens should be invalidated
     */
    public InvalidateTokenRequestBuilder setRealmName(String realmName) {
        request.setRealmName(realmName);
        return this;
    }

    /**
     * Sets the username for which all tokens should be invalidated
     */
    public InvalidateTokenRequestBuilder setUserName(String username) {
        request.setUserName(username);
        return this;
    }
}
