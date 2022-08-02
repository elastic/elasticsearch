/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.saml;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;

/**
 * Request builder used to populate a {@link SamlPrepareAuthenticationRequest}
 */
public final class SamlPrepareAuthenticationRequestBuilder extends ActionRequestBuilder<
    SamlPrepareAuthenticationRequest,
    SamlPrepareAuthenticationResponse> {

    public SamlPrepareAuthenticationRequestBuilder(ElasticsearchClient client) {
        super(client, SamlPrepareAuthenticationAction.INSTANCE, new SamlPrepareAuthenticationRequest());
    }

    public SamlPrepareAuthenticationRequestBuilder realmName(String name) {
        request.setRealmName(name);
        return this;
    }

    public SamlPrepareAuthenticationRequestBuilder assertionConsumerService(String url) {
        request.setAssertionConsumerServiceURL(url);
        return this;
    }
}
