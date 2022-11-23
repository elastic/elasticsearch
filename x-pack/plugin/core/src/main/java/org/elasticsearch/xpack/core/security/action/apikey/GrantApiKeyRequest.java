/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.security.action.GrantRequest;

import java.io.IOException;
import java.util.Objects;

/**
 * Request class used for the creation of an API key on behalf of another user.
 * Logically this is similar to {@link CreateApiKeyRequest}, but is for cases when the user that has permission to call this action
 * is different to the user for whom the API key should be created
 */
public final class GrantApiKeyRequest extends GrantRequest {

    private CreateApiKeyRequest apiKey;

    public GrantApiKeyRequest() {
        super();
        this.apiKey = new CreateApiKeyRequest();
    }

    public GrantApiKeyRequest(StreamInput in) throws IOException {
        super(in);
        this.apiKey = new CreateApiKeyRequest(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        apiKey.writeTo(out);
    }

    public WriteRequest.RefreshPolicy getRefreshPolicy() {
        return apiKey.getRefreshPolicy();
    }

    public void setRefreshPolicy(WriteRequest.RefreshPolicy refreshPolicy) {
        apiKey.setRefreshPolicy(refreshPolicy);
    }

    public CreateApiKeyRequest getApiKeyRequest() {
        return apiKey;
    }

    public void setApiKeyRequest(CreateApiKeyRequest apiKeyRequest) {
        this.apiKey = Objects.requireNonNull(apiKeyRequest, "Cannot set a null api_key");
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = apiKey.validate();
        return grant.validate(validationException);
    }
}
