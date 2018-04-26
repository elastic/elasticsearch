/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.saml;

import java.util.List;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;

/**
 * Represents a request to authenticate using SAML assertions.
 */
public final class SamlAuthenticateRequest extends ActionRequest {

    private byte[] saml;
    private List<String> validRequestIds;

    public SamlAuthenticateRequest() {
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
}
