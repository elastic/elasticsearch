/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.namedcredentials;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class CredentialAuthTypeTests extends ESTestCase {

    public void testFromTypeNameRoundTrip() {
        for (CredentialAuthType type : CredentialAuthType.values()) {
            assertThat(CredentialAuthType.fromTypeName(type.typeName()), is(type));
        }
    }

    public void testFromTypeNameUnknownThrows() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> CredentialAuthType.fromTypeName("nope"));
        assertThat(e.getMessage(), containsString("unknown auth_type [nope]"));
        assertThat(e.getMessage(), containsString("basic"));
    }

    public void testValidateConfigRejectsUnknownFields() {
        List<String> errors = CredentialAuthType.OAUTH_CLIENT_CREDENTIALS.validateConfig(Map.of("token_url", "https://x", "bogus", "y"));
        assertThat(errors, hasSize(1));
        assertThat(errors.get(0), containsString("bogus"));
    }

    public void testValidateConfigRejectsAnyFieldForTypesWithoutConfig() {
        List<String> errors = CredentialAuthType.BASIC.validateConfig(Map.of("anything", "x"));
        assertThat(errors, hasSize(1));
    }

    public void testValidateConfigAcceptsKnownFields() {
        assertThat(CredentialAuthType.GCP_SERVICE_ACCOUNT.validateConfig(Map.of("scope", "s")), empty());
        assertThat(CredentialAuthType.BASIC.validateConfig(Map.of()), empty());
    }

    public void testValidateAuthRejectsUnknownAndMissingRequired() {
        List<String> errors = CredentialAuthType.BASIC.validateAuth(Map.of("username", "u", "extra", "x"));
        // "extra" unknown + "password" missing
        assertThat(errors, hasSize(2));
    }

    public void testValidateAuthAcceptsRequiredPlusOptional() {
        assertThat(
            CredentialAuthType.OAUTH_AUTHORIZATION_CODE.validateAuth(
                Map.of("client_id", "a", "client_secret", "b", "access_token", "c", "refresh_token", "d")
            ),
            empty()
        );
        assertThat(CredentialAuthType.API_KEY_HEADER.validateAuth(Map.of("api_key", "k")), empty());
    }

    public void testValidateAuthRejectsEmptyRequiredField() {
        // password is required but blank — must produce an error
        List<String> errors = CredentialAuthType.BASIC.validateAuth(Map.of("username", "u", "password", ""));
        assertThat(errors, hasSize(1));
        assertThat(errors.get(0), containsString("must not be empty"));
        assertThat(errors.get(0), containsString("password"));
    }

    public void testValidateAuthRejectsEmptyOptionalField() {
        // access_token is optional for oauth_authorization_code, but if provided it must be non-empty
        List<String> errors = CredentialAuthType.OAUTH_AUTHORIZATION_CODE.validateAuth(
            Map.of("client_id", "a", "client_secret", "b", "access_token", "")
        );
        assertThat(errors, hasSize(1));
        assertThat(errors.get(0), containsString("must not be empty"));
        assertThat(errors.get(0), containsString("access_token"));
    }
}
