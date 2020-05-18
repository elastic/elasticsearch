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
 * Represents a request to complete SAML LogoutResponse
 */
public final class SamlCompleteLogoutRequest extends ActionRequest {

    private String content;
    private List<String> validRequestIds;
    @Nullable
    private String realm;

    public SamlCompleteLogoutRequest(StreamInput in) throws IOException {
        super(in);
    }

    public SamlCompleteLogoutRequest() {
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
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
