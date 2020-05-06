/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.saml;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.List;

/**
 * Represents a request to handle LogoutResponse using SAML assertions.
 */
public final class SamlLogoutResponseRequest extends ActionRequest {

    private byte[] saml;
    private List<String> validRequestIds;
    @Nullable
    private String realm;
    @Nullable
    private String assertionConsumerServiceURL;

    public SamlLogoutResponseRequest(StreamInput in) throws IOException {
        super(in);
    }

    public SamlLogoutResponseRequest() {
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public byte[] getSaml() {
        return saml;
    }

    public void setSaml(byte[] saml) {
        this.saml = saml;
    }

    public List<String> getValidRequestIds() {
        return validRequestIds;
    }

    public void setValidRequestIds(List<String> validRequestIds) {
        this.validRequestIds = validRequestIds;
    }

    public String getRealm() {
        return realm;
    }

    public void setRealm(String realm) {
        this.realm = realm;
    }

    public String getAssertionConsumerServiceURL() {
        return assertionConsumerServiceURL;
    }

    public void setAssertionConsumerServiceURL(String assertionConsumerServiceURL) {
        this.assertionConsumerServiceURL = assertionConsumerServiceURL;
    }
}
