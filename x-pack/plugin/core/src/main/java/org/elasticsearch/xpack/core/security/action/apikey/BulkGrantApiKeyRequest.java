/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.xpack.core.security.action.GrantRequest;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Request class used to create multiple API keys on behalf of another user.
 * Grant credentials apply to the whole request; each item in {@code api_keys} is a
 * {@link CreateApiKeyRequest} (name, optional expiration, role descriptors, and metadata).
 */
public final class BulkGrantApiKeyRequest extends GrantRequest {

    private List<CreateApiKeyRequest> apiKeyRequests = List.of();

    public BulkGrantApiKeyRequest() {
        super();
    }

    public List<CreateApiKeyRequest> getApiKeyRequests() {
        return apiKeyRequests;
    }

    public void setApiKeyRequests(List<CreateApiKeyRequest> apiKeyRequests) {
        this.apiKeyRequests = List.copyOf(Objects.requireNonNull(apiKeyRequests, "Cannot set a null api_keys"));
    }

    public WriteRequest.RefreshPolicy getRefreshPolicy() {
        return apiKeyRequests.isEmpty() ? null : apiKeyRequests.get(0).getRefreshPolicy();
    }

    public void setRefreshPolicy(WriteRequest.RefreshPolicy refreshPolicy) {
        for (CreateApiKeyRequest apiKeyRequest : apiKeyRequests) {
            apiKeyRequest.setRefreshPolicy(refreshPolicy);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (apiKeyRequests.isEmpty()) {
            validationException = addValidationError("[api_keys] must not be empty", validationException);
        } else {
            for (CreateApiKeyRequest apiKeyRequest : apiKeyRequests) {
                ActionRequestValidationException keyException = apiKeyRequest.validate();
                if (keyException != null) {
                    for (String error : keyException.validationErrors()) {
                        validationException = addValidationError(error, validationException);
                    }
                }
            }
        }
        return grant.validate(validationException);
    }
}
