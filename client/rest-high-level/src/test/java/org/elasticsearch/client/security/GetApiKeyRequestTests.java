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

public class GetApiKeyRequestTests extends ESTestCase {

    public void testRequestValidation() {
        GetApiKeyRequest request = GetApiKeyRequest.usingApiKeyId(randomAlphaOfLength(5));
        Optional<ValidationException> ve = request.validate();
        assertFalse(ve.isPresent());
        request = GetApiKeyRequest.usingApiKeyName(randomAlphaOfLength(5));
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
    }

    public void testRequestValidationFailureScenarios() throws IOException {
        String[][] inputs = new String[][] {
                { randomFrom(new String[] { null, "" }), randomFrom(new String[] { null, "" }), randomFrom(new String[] { null, "" }),
                        randomFrom(new String[] { null, "" }) },
                { randomFrom(new String[] { null, "" }), "user", "api-kid", "api-kname" },
                { "realm", randomFrom(new String[] { null, "" }), "api-kid", "api-kname" },
                { "realm", "user", "api-kid", randomFrom(new String[] { null, "" }) },
                { randomFrom(new String[] { null, "" }), randomFrom(new String[] { null, "" }), "api-kid", "api-kname" } };
        String[] expectedErrorMessages = new String[] { "One of [api key id, api key name, username, realm name] must be specified",
                "username or realm name must not be specified when the api key id or api key name is specified",
                "username or realm name must not be specified when the api key id or api key name is specified",
                "username or realm name must not be specified when the api key id or api key name is specified",
                "only one of [api key id, api key name] can be specified" };

        for (int i = 0; i < inputs.length; i++) {
            final int caseNo = i;
            IllegalArgumentException ve = expectThrows(IllegalArgumentException.class,
                    () -> new GetApiKeyRequest(inputs[caseNo][0], inputs[caseNo][1], inputs[caseNo][2], inputs[caseNo][3]));
            assertNotNull(ve);
            assertThat(ve.getMessage(), equalTo(expectedErrorMessages[caseNo]));
        }
    }
}
