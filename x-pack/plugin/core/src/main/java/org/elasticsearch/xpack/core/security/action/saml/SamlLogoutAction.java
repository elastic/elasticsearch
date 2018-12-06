/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.saml;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Action for initiating a logout process for a SAML-SSO user
 */
public final class SamlLogoutAction extends Action<SamlLogoutRequest, SamlLogoutResponse, SamlLogoutRequestBuilder> {

    public static final String NAME = "cluster:admin/xpack/security/saml/logout";
    public static final SamlLogoutAction INSTANCE = new SamlLogoutAction();

    private SamlLogoutAction() {
        super(NAME);
    }

    @Override
    public SamlLogoutRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new SamlLogoutRequestBuilder(client);
    }

    @Override
    public SamlLogoutResponse newResponse() {
        return new SamlLogoutResponse();
    }
}
