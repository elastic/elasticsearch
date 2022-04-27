/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;

public class AuthenticateRequestBuilder extends ActionRequestBuilder<AuthenticateRequest, AuthenticateResponse> {

    public AuthenticateRequestBuilder(ElasticsearchClient client) {
        this(client, AuthenticateAction.INSTANCE);
    }

    public AuthenticateRequestBuilder(ElasticsearchClient client, AuthenticateAction action) {
        super(client, action, new AuthenticateRequest());
    }

    public AuthenticateRequestBuilder username(String username) {
        request.username(username);
        return this;
    }
}
