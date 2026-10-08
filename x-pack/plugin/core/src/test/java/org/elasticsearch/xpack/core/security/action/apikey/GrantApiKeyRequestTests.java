/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;

import static org.elasticsearch.xpack.core.security.action.Grant.ACCESS_TOKEN_GRANT_TYPE;
import static org.elasticsearch.xpack.core.security.action.Grant.USER_MANAGED_SERVICE_ACCOUNT_GRANT_TYPE;

/** Tests the API-key grant policy and preservation of errors from the API-key request. */
public class GrantApiKeyRequestTests extends ESTestCase {

    public void testUserManagedServiceAccountGrantIsSupported() {
        final GrantApiKeyRequest request = new GrantApiKeyRequest();
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
        request.getApiKeyRequest().setName(randomAlphaOfLength(8));
        request.getGrant().setType(USER_MANAGED_SERVICE_ACCOUNT_GRANT_TYPE);
        try (SecureString token = new SecureString(randomAlphaOfLength(16).toCharArray())) {
            request.getGrant().setServiceAccountToken(token);
            assertNull(request.validate());
        }
    }

    public void testUnsupportedGrantPreservesApiKeyValidationErrors() {
        assertCombinedValidationErrors("unknown", "grant_type [unknown] is not supported");
    }

    public void testMissingGrantTypePreservesApiKeyValidationErrors() {
        assertCombinedValidationErrors(null, "[grant_type] is required");
    }

    public void testMissingGrantCredentialPreservesApiKeyValidationErrors() {
        assertCombinedValidationErrors(ACCESS_TOKEN_GRANT_TYPE, "[access_token] is required for grant_type [access_token]");
    }

    private static void assertCombinedValidationErrors(String grantType, String grantError) {
        final GrantApiKeyRequest request = new GrantApiKeyRequest();
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
        request.getGrant().setType(grantType);
        final var apiKeyValidation = request.getApiKeyRequest().validate();
        assertNotNull(apiKeyValidation);
        final var expectedErrors = new ArrayList<>(apiKeyValidation.validationErrors());
        expectedErrors.add(grantError);
        assertEquals(expectedErrors, request.validate().validationErrors());
    }
}
