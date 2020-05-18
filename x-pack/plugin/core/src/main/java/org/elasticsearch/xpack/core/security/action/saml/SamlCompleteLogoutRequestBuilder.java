/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.saml;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

import java.util.List;

/**
 * Request builder used to populate a {@link SamlCompleteLogoutRequest}
 */
public final class SamlCompleteLogoutRequestBuilder
        extends ActionRequestBuilder<SamlCompleteLogoutRequest, SamlCompleteLogoutResponse> {

    public SamlCompleteLogoutRequestBuilder(ElasticsearchClient client) {
        super(client, SamlCompleteLogoutAction.INSTANCE, new SamlCompleteLogoutRequest());
    }

    public SamlCompleteLogoutRequestBuilder content(String content) {
        request.setContent(content);
        return this;
    }

    public SamlCompleteLogoutRequestBuilder validRequestIds(List<String> validRequestIds) {
        request.setValidRequestIds(validRequestIds);
        return this;
    }

    public SamlCompleteLogoutRequestBuilder authenticatingRealm(String realm) {
        request.setRealm(realm);
        return this;
    }
}
