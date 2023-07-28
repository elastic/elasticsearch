/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.token;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;

/**
 * Request builder used to populate a {@link CreateTokenRequest}
 */
public final class CreateTokenRequestBuilder extends ActionRequestBuilder<CreateTokenRequest, CreateTokenResponse> {

    public CreateTokenRequestBuilder(ElasticsearchClient client) {
        super(client, CreateTokenAction.INSTANCE, new CreateTokenRequest());
    }

    /**
     * Specifies the grant type for this request. Currently only <code>password</code> is supported
     */
    public CreateTokenRequestBuilder setGrantType(String grantType) {
        request.setGrantType(grantType);
        return this;
    }

}
