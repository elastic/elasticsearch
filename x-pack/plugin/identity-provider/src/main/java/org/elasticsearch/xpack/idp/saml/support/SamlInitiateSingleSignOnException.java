/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp.saml.support;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.idp.action.SamlInitiateSingleSignOnResponse;

import java.io.IOException;

public class SamlInitiateSingleSignOnException extends ElasticsearchSecurityException {

    private SamlInitiateSingleSignOnResponse samlInitiateSingleSignOnResponse;

    public SamlInitiateSingleSignOnException(
        String msg,
        RestStatus status,
        SamlInitiateSingleSignOnResponse samlInitiateSingleSignOnResponse
    ) {
        super(msg, status);
        this.samlInitiateSingleSignOnResponse = samlInitiateSingleSignOnResponse;
    }

    public SamlInitiateSingleSignOnException(String msg, RestStatus status) {
        super(msg, status);
    }

    @Override
    protected void metadataToXContent(XContentBuilder builder, Params params) throws IOException {
        if (this.samlInitiateSingleSignOnResponse != null) {
            builder.startObject("saml_initiate_single_sign_on_response");
            this.samlInitiateSingleSignOnResponse.toXContent(builder);
            builder.endObject();
        }
    }

    public SamlInitiateSingleSignOnResponse getSamlInitiateSingleSignOnResponse() {
        return samlInitiateSingleSignOnResponse;
    }
}
