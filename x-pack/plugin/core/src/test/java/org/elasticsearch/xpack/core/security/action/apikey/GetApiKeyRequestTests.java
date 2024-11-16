/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.nullValue;

public class GetApiKeyRequestTests extends ESTestCase {

    public void testRequestValidation() {
        GetApiKeyRequest request = GetApiKeyRequest.builder()
            .apiKeyId(randomAlphaOfLength(5))
            .ownedByAuthenticatedUser(randomBoolean())
            .withProfileUid(randomBoolean())
            .build();
        ActionRequestValidationException ve = request.validate();
        assertNull(ve);
        request = GetApiKeyRequest.builder()
            .apiKeyName(randomAlphaOfLength(5))
            .ownedByAuthenticatedUser(randomBoolean())
            .withProfileUid(randomBoolean())
            .build();
        ve = request.validate();
        assertNull(ve);
        request = GetApiKeyRequest.builder()
            .realmName(randomAlphaOfLength(5))
            .activeOnly(randomBoolean())
            .withProfileUid(randomBoolean())
            .build();
        ve = request.validate();
        assertNull(ve);
        request = GetApiKeyRequest.builder()
            .userName(randomAlphaOfLength(5))
            .activeOnly(randomBoolean())
            .withProfileUid(randomBoolean())
            .build();
        ve = request.validate();
        assertNull(ve);
        request = GetApiKeyRequest.builder()
            .realmName(randomAlphaOfLength(5))
            .userName(randomAlphaOfLength(7))
            .activeOnly(randomBoolean())
            .withProfileUid(randomBoolean())
            .build();
        ve = request.validate();
        assertNull(ve);
    }

    public void testRequestValidationFailureScenarios() throws IOException {
        String[][] inputs = new String[][] {
            { randomNullOrEmptyString(), "user", "api-kid", "api-kname", "false" },
            { "realm", randomNullOrEmptyString(), "api-kid", "api-kname", "false" },
            { "realm", "user", "api-kid", randomNullOrEmptyString(), "false" },
            { randomNullOrEmptyString(), randomNullOrEmptyString(), "api-kid", "api-kname", "false" },
            { "realm", randomNullOrEmptyString(), randomNullOrEmptyString(), randomNullOrEmptyString(), "true" },
            { randomNullOrEmptyString(), "user", randomNullOrEmptyString(), randomNullOrEmptyString(), "true" } };
        String[][] expectedErrorMessages = new String[][] {
            {
                "username or realm name must not be specified when the api key id or api key name is specified",
                "only one of [api key id, api key name] can be specified" },
            {
                "username or realm name must not be specified when the api key id or api key name is specified",
                "only one of [api key id, api key name] can be specified" },
            { "username or realm name must not be specified when the api key id or api key name is specified" },
            { "only one of [api key id, api key name] can be specified" },
            { "neither username nor realm-name may be specified when retrieving owned API keys" },
            { "neither username nor realm-name may be specified when retrieving owned API keys" } };

        for (int caseNo = 0; caseNo < inputs.length; caseNo++) {
            GetApiKeyRequest request = GetApiKeyRequest.builder()
                .realmName(inputs[caseNo][0])
                .userName(inputs[caseNo][1])
                .apiKeyId(inputs[caseNo][2])
                .apiKeyName(inputs[caseNo][3])
                .ownedByAuthenticatedUser(Boolean.parseBoolean(inputs[caseNo][4]))
                .withProfileUid(randomBoolean())
                .build();
            ActionRequestValidationException ve = request.validate();
            assertNotNull(ve);
            assertEquals(expectedErrorMessages[caseNo].length, ve.validationErrors().size());
            assertThat(ve.validationErrors(), containsInAnyOrder(expectedErrorMessages[caseNo]));
        }
    }

    public void testEmptyStringsAreCoercedToNull() {
        Supplier<String> randomBlankString = () -> " ".repeat(randomIntBetween(0, 5));
        final GetApiKeyRequest request = GetApiKeyRequest.builder()
            .realmName(randomBlankString.get())
            .userName(randomBlankString.get())
            .apiKeyId(randomBlankString.get())
            .apiKeyName(randomBlankString.get())
            .ownedByAuthenticatedUser(randomBoolean())
            .withLimitedBy(randomBoolean())
            .withProfileUid(randomBoolean())
            .build();
        assertThat(request.getRealmName(), nullValue());
        assertThat(request.getUserName(), nullValue());
        assertThat(request.getApiKeyId(), nullValue());
        assertThat(request.getApiKeyName(), nullValue());
    }

    private static String randomNullOrEmptyString() {
        return randomBoolean() ? "" : null;
    }
}
