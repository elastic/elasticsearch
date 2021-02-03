/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.ValidationException;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;

public class GetApiKeyRequestTests extends ESTestCase {

    public void testRequestValidation() {
        GetApiKeyRequest request = GetApiKeyRequest.usingApiKeyId(randomAlphaOfLength(5), randomBoolean());
        Optional<ValidationException> ve = request.validate();
        assertFalse(ve.isPresent());
        request = GetApiKeyRequest.usingApiKeyName(randomAlphaOfLength(5), randomBoolean());
        ve = request.validate();
        assertFalse(ve.isPresent());
        request = GetApiKeyRequest.usingRealmName(randomAlphaOfLength(5));
        ve = request.validate();
        assertFalse(ve.isPresent());
        request = GetApiKeyRequest.usingUserName(randomAlphaOfLength(5));
        ve = request.validate();
        assertFalse(ve.isPresent());
        request = GetApiKeyRequest.usingRealmAndUserName(randomAlphaOfLength(5), randomAlphaOfLength(7));
        ve = request.validate();
        assertFalse(ve.isPresent());
        request = GetApiKeyRequest.forOwnedApiKeys();
        ve = request.validate();
        assertFalse(ve.isPresent());
    }

    public void testRequestValidationFailureScenarios() throws IOException {
        String[][] inputs = new String[][] {
                { randomNullOrEmptyString(), "user", "api-kid", "api-kname", "false" },
                { "realm", randomNullOrEmptyString(), "api-kid", "api-kname", "false" },
                { "realm", "user", "api-kid", randomNullOrEmptyString(), "false" },
                { randomNullOrEmptyString(), randomNullOrEmptyString(), "api-kid", "api-kname", "false" },
                { "realm", randomNullOrEmptyString(), randomNullOrEmptyString(), randomNullOrEmptyString(), "true"},
                { randomNullOrEmptyString(), "user", randomNullOrEmptyString(), randomNullOrEmptyString(), "true"} };
        String[] expectedErrorMessages = new String[] {
                "username or realm name must not be specified when the api key id or api key name is specified",
                "username or realm name must not be specified when the api key id or api key name is specified",
                "username or realm name must not be specified when the api key id or api key name is specified",
                "only one of [api key id, api key name] can be specified",
                "neither username nor realm-name may be specified when retrieving owned API keys",
                "neither username nor realm-name may be specified when retrieving owned API keys" };

        for (int i = 0; i < inputs.length; i++) {
            final int caseNo = i;
            IllegalArgumentException ve = expectThrows(IllegalArgumentException.class,
                    () -> new GetApiKeyRequest(inputs[caseNo][0], inputs[caseNo][1], inputs[caseNo][2], inputs[caseNo][3],
                        Boolean.valueOf(inputs[caseNo][4])));
            assertNotNull(ve);
            assertThat(ve.getMessage(), equalTo(expectedErrorMessages[caseNo]));
        }
    }

    private static String randomNullOrEmptyString() {
        return randomBoolean() ? "" : null;
    }
}
