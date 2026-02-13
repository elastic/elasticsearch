/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.saml;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.List;

/**
 * Represents a request to authenticate using SAML assertions.
 */
public final class SamlAuthenticateRequest extends LegacyActionRequest {

    private byte[] saml;
    private List<String> validRequestIds;
    @Nullable
    private String realm;

    public SamlAuthenticateRequest(StreamInput in) throws IOException {
        super(in);
    }

    public SamlAuthenticateRequest() {}

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
}
