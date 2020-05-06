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
 * Request builder used to populate a {@link SamlLogoutResponseRequest}
 */
public final class SamlLogoutResponseRequestBuilder
        extends ActionRequestBuilder<SamlLogoutResponseRequest, SamlLogoutResponseResponse> {

    public SamlLogoutResponseRequestBuilder(ElasticsearchClient client) {
        super(client, SamlLogoutResponseAction.INSTANCE, new SamlLogoutResponseRequest());
    }

    public SamlLogoutResponseRequestBuilder saml(byte[] saml) {
        request.setSaml(saml);
        return this;
    }

    public SamlLogoutResponseRequestBuilder validRequestIds(List<String> validRequestIds) {
        request.setValidRequestIds(validRequestIds);
        return this;
    }

    public SamlLogoutResponseRequestBuilder authenticatingRealm(String realm) {
        request.setRealm(realm);
        return this;
    }
}
