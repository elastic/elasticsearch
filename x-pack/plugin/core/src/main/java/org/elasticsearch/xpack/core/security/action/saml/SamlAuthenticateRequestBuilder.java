/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.saml;

import java.util.List;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Request builder used to populate a {@link SamlAuthenticateRequest}
 */
public final class SamlAuthenticateRequestBuilder
        extends ActionRequestBuilder<SamlAuthenticateRequest, SamlAuthenticateResponse> {

    public SamlAuthenticateRequestBuilder(ElasticsearchClient client) {
        super(client, SamlAuthenticateAction.INSTANCE, new SamlAuthenticateRequest());
    }

    public SamlAuthenticateRequestBuilder saml(byte[] saml) {
        request.setSaml(saml);
        return this;
    }

    public SamlAuthenticateRequestBuilder validRequestIds(List<String> validRequestIds) {
        request.setValidRequestIds(validRequestIds);
        return this;
    }

    public SamlAuthenticateRequestBuilder authenticatingRealm(String realm) {
        request.setRealm(realm);
        return this;
    }
}
