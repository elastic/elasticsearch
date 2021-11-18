/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.saml;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Request builder used to populate a {@link SamlLogoutRequest}
 */
public final class SamlLogoutRequestBuilder extends ActionRequestBuilder<SamlLogoutRequest, SamlLogoutResponse> {

    public SamlLogoutRequestBuilder(ElasticsearchClient client) {
        super(client, SamlLogoutAction.INSTANCE, new SamlLogoutRequest());
    }

    public SamlLogoutRequestBuilder token(String token) {
        request.setToken(token);
        return this;
    }
}
