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

import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xpack.core.security.action.Grant.ACCESS_TOKEN_GRANT_TYPE;
import static org.elasticsearch.xpack.core.security.action.Grant.PASSWORD_GRANT_TYPE;
import static org.elasticsearch.xpack.core.security.action.Grant.USER_MANAGED_SERVICE_ACCOUNT_GRANT_TYPE;

/**
 * Request class used for the creation of an API key on behalf of another user.
 * Logically this is similar to {@link CreateApiKeyRequest}, but is for cases when the user that has permission to call this action
 * is different to the user for whom the API key should be created
 */
public final class GrantApiKeyRequest extends GrantRequest {

    private CreateApiKeyRequest apiKey;

    public GrantApiKeyRequest() {
        super(Set.of(PASSWORD_GRANT_TYPE, ACCESS_TOKEN_GRANT_TYPE, USER_MANAGED_SERVICE_ACCOUNT_GRANT_TYPE));
        this.apiKey = new CreateApiKeyRequest();
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
        return super.validate(apiKey.validate());
    }
}
