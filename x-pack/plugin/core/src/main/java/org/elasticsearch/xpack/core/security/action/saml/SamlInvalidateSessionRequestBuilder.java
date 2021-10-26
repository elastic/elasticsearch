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
 * Request builder used to populate a {@link SamlInvalidateSessionRequest}
 */
public final class SamlInvalidateSessionRequestBuilder
        extends ActionRequestBuilder<SamlInvalidateSessionRequest, SamlInvalidateSessionResponse> {

    public SamlInvalidateSessionRequestBuilder(ElasticsearchClient client) {
        super(client, SamlInvalidateSessionAction.INSTANCE, new SamlInvalidateSessionRequest());
    }

    public SamlInvalidateSessionRequestBuilder queryString(String query) {
        request.setQueryString(query);
        return this;
    }

    public SamlInvalidateSessionRequestBuilder realmName(String name) {
        request.setRealmName(name);
        return this;
    }

    public SamlInvalidateSessionRequestBuilder assertionConsumerService(String url) {
        request.setAssertionConsumerServiceURL(url);
        return this;
    }

}
