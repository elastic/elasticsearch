/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.ValidationException;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class InvalidateApiKeyRequestTests extends ESTestCase {

    public void testRequestValidation() {
        InvalidateApiKeyRequest request = InvalidateApiKeyRequest.usingApiKeyId(randomAlphaOfLength(5), randomBoolean());
        Optional<ValidationException> ve = request.validate();
        assertThat(ve.isPresent(), is(false));
        request = InvalidateApiKeyRequest.usingApiKeyName(randomAlphaOfLength(5), randomBoolean());
        ve = request.validate();
        assertThat(ve.isPresent(), is(false));
        request = InvalidateApiKeyRequest.usingRealmName(randomAlphaOfLength(5));
        ve = request.validate();
        assertThat(ve.isPresent(), is(false));
        request = InvalidateApiKeyRequest.usingUserName(randomAlphaOfLength(5));
        ve = request.validate();
        assertThat(ve.isPresent(), is(false));
        request = InvalidateApiKeyRequest.usingRealmAndUserName(randomAlphaOfLength(5), randomAlphaOfLength(7));
        ve = request.validate();
        assertThat(ve.isPresent(), is(false));
        request = InvalidateApiKeyRequest.forOwnedApiKeys();
        ve = request.validate();
        assertFalse(ve.isPresent());
    }

    public void testRequestValidationFailureScenarios() throws IOException {
        String[][] inputs = new String[][] {
                { randomNullOrEmptyString(), randomNullOrEmptyString(), randomNullOrEmptyString(), randomNullOrEmptyString(), "false" },
                { randomNullOrEmptyString(), "user", "api-kid", "api-kname", "false" },
                { "realm", randomNullOrEmptyString(), "api-kid", "api-kname", "false" },
                { "realm", "user", "api-kid", randomNullOrEmptyString(), "false" },
                { randomNullOrEmptyString(), randomNullOrEmptyString(), "api-kid", "api-kname", "false" },
                { "realm", randomNullOrEmptyString(), randomNullOrEmptyString(), randomNullOrEmptyString(), "true" },
                { randomNullOrEmptyString(), "user", randomNullOrEmptyString(), randomNullOrEmptyString(), "true" } };
        String[] expectedErrorMessages = new String[] {
                "One of [api key id, api key name, username, realm name] must be specified if [owner] flag is false",
                "username or realm name must not be specified when the api key id or api key name is specified",
                "username or realm name must not be specified when the api key id or api key name is specified",
                "username or realm name must not be specified when the api key id or api key name is specified",
                "only one of [api key id, api key name] can be specified",
                "neither username nor realm-name may be specified when invalidating owned API keys",
                "neither username nor realm-name may be specified when invalidating owned API keys" };

        for (int i = 0; i < inputs.length; i++) {
            final int caseNo = i;
            IllegalArgumentException ve = expectThrows(IllegalArgumentException.class,
                    () -> new InvalidateApiKeyRequest(inputs[caseNo][0], inputs[caseNo][1], inputs[caseNo][2], inputs[caseNo][3],
                        Boolean.valueOf(inputs[caseNo][4])));
            assertNotNull(ve);
            assertThat(ve.getMessage(), equalTo(expectedErrorMessages[caseNo]));
        }
    }

    private static String randomNullOrEmptyString() {
        return randomBoolean() ? "" : null;
    }
}
