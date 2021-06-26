/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.saml;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Represents a request to complete SAML LogoutResponse
 */
public final class SamlCompleteLogoutRequest extends ActionRequest {

    @Nullable
    private String queryString;
    @Nullable
    private String content;
    private List<String> validRequestIds;
    private String realm;

    public SamlCompleteLogoutRequest(StreamInput in) throws IOException {
        super(in);
    }

    public SamlCompleteLogoutRequest() {
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.hasText(realm) == false) {
            validationException = addValidationError("realm may not be empty", validationException);
        }
        if (Strings.hasText(queryString) == false && Strings.hasText(content) == false) {
            validationException = addValidationError("query_string and content may not both be empty", validationException);
        }
        if (Strings.hasText(queryString) && Strings.hasText(content)) {
            validationException = addValidationError("query_string and content may not both present", validationException);
        }
        return validationException;
    }

    public String getQueryString() {
        return queryString;
    }

    public void setQueryString(String queryString) {
        if (this.queryString == null) {
            this.queryString = queryString;
        } else {
            throw new IllegalArgumentException("Must use either [query_string] or [queryString], not both at the same time");
        }
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

    public boolean isHttpRedirect() {
        return queryString != null;
    }

    public String getPayload() {
        return isHttpRedirect() ? queryString : content;
    }
}
