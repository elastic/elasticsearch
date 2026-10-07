/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.profile;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.core.security.action.Grant.ACCESS_TOKEN_GRANT_TYPE;
import static org.elasticsearch.xpack.core.security.action.Grant.PASSWORD_GRANT_TYPE;
import static org.elasticsearch.xpack.core.security.action.Grant.USER_MANAGED_SERVICE_ACCOUNT_GRANT_TYPE;
import static org.hamcrest.Matchers.contains;

/** Tests that profile activation accepts user grants and rejects service-account grants. */
public class ActivateProfileRequestTests extends ESTestCase {

    public void testPasswordGrantIsSupported() {
        final ActivateProfileRequest request = new ActivateProfileRequest();
        request.getGrant().setType(PASSWORD_GRANT_TYPE);
        request.getGrant().setUsername(randomAlphaOfLength(8));
        try (SecureString password = new SecureString(randomAlphaOfLength(16).toCharArray())) {
            request.getGrant().setPassword(password);
            assertNull(request.validate());
        }
    }

    public void testAccessTokenGrantIsSupported() {
        final ActivateProfileRequest request = new ActivateProfileRequest();
        request.getGrant().setType(ACCESS_TOKEN_GRANT_TYPE);
        try (SecureString token = new SecureString(randomAlphaOfLength(16).toCharArray())) {
            request.getGrant().setAccessToken(token);
            assertNull(request.validate());
        }
    }

    public void testUserManagedServiceAccountGrantWithoutTokenIsNotSupported() {
        final ActivateProfileRequest request = new ActivateProfileRequest();
        request.getGrant().setType(USER_MANAGED_SERVICE_ACCOUNT_GRANT_TYPE);
        assertUnsupportedGrantType(request);
    }

    public void testGrantTypeIsRequired() {
        final ActivateProfileRequest request = new ActivateProfileRequest();
        assertThat(request.validate().validationErrors(), contains("[grant_type] is required"));
    }

    public void testUnknownGrantTypeIsNotSupported() {
        final ActivateProfileRequest request = new ActivateProfileRequest();
        request.getGrant().setType("unknown");
        assertThat(request.validate().validationErrors(), contains("grant_type [unknown] is not supported"));
    }

    public void testAccessTokenGrantRequiresToken() {
        final ActivateProfileRequest request = new ActivateProfileRequest();
        request.getGrant().setType(ACCESS_TOKEN_GRANT_TYPE);
        assertThat(request.validate().validationErrors(), contains("[access_token] is required for grant_type [access_token]"));
    }

    public void testUserManagedServiceAccountGrantWithTokenIsNotSupported() {
        final ActivateProfileRequest request = new ActivateProfileRequest();
        request.getGrant().setType(USER_MANAGED_SERVICE_ACCOUNT_GRANT_TYPE);
        try (SecureString token = new SecureString("token".toCharArray())) {
            request.getGrant().setServiceAccountToken(token);
            assertUnsupportedGrantType(request);
        }
    }

    private static void assertUnsupportedGrantType(ActivateProfileRequest request) {
        final ActionRequestValidationException e = request.validate();
        assertNotNull(e);
        assertThat(e.validationErrors(), contains("grant_type [" + USER_MANAGED_SERVICE_ACCOUNT_GRANT_TYPE + "] is not supported"));
    }
}
