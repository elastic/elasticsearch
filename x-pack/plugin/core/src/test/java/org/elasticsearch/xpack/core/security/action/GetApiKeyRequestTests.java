/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.elasticsearch.test.VersionUtils.randomVersionBetween;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class GetApiKeyRequestTests extends ESTestCase {

    public void testRequestValidation() {
        GetApiKeyRequest request = GetApiKeyRequest.usingApiKeyId(randomAlphaOfLength(5), randomBoolean());
        ActionRequestValidationException ve = request.validate();
        assertNull(ve);
        request = GetApiKeyRequest.usingApiKeyName(randomAlphaOfLength(5), randomBoolean());
        ve = request.validate();
        assertNull(ve);
        request = GetApiKeyRequest.usingRealmName(randomAlphaOfLength(5));
        ve = request.validate();
        assertNull(ve);
        request = GetApiKeyRequest.usingUserName(randomAlphaOfLength(5));
        ve = request.validate();
        assertNull(ve);
        request = GetApiKeyRequest.usingRealmAndUserName(randomAlphaOfLength(5), randomAlphaOfLength(7));
        ve = request.validate();
        assertNull(ve);
    }

    public void testRequestValidationFailureScenarios() throws IOException {
        class Dummy extends ActionRequest {
            String realm;
            String user;
            String apiKeyId;
            String apiKeyName;
            boolean ownedByAuthenticatedUser;

            Dummy(String[] a) {
                realm = a[0];
                user = a[1];
                apiKeyId = a[2];
                apiKeyName = a[3];
                ownedByAuthenticatedUser = Boolean.parseBoolean(a[4]);
            }

            @Override
            public ActionRequestValidationException validate() {
                return null;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
                out.writeOptionalString(realm);
                out.writeOptionalString(user);
                out.writeOptionalString(apiKeyId);
                out.writeOptionalString(apiKeyName);
                out.writeOptionalBoolean(ownedByAuthenticatedUser);
            }
        }

        String[][] inputs = new String[][]{
            {randomNullOrEmptyString(), "user", "api-kid", "api-kname", "false"},
            {"realm", randomNullOrEmptyString(), "api-kid", "api-kname", "false"},
            {"realm", "user", "api-kid", randomNullOrEmptyString(), "false"},
            {randomNullOrEmptyString(), randomNullOrEmptyString(), "api-kid", "api-kname", "false"},
            {"realm", randomNullOrEmptyString(), randomNullOrEmptyString(), randomNullOrEmptyString(), "true"},
            {randomNullOrEmptyString(), "user", randomNullOrEmptyString(), randomNullOrEmptyString(), "true"}
        };
        String[][] expectedErrorMessages = new String[][]{
            {"username or realm name must not be specified when the api key id or api key name is specified",
                "only one of [api key id, api key name] can be specified"},
            {"username or realm name must not be specified when the api key id or api key name is specified",
                "only one of [api key id, api key name] can be specified"},
            {"username or realm name must not be specified when the api key id or api key name is specified"},
            {"only one of [api key id, api key name] can be specified"},
            {"neither username nor realm-name may be specified when retrieving owned API keys"},
            {"neither username nor realm-name may be specified when retrieving owned API keys"}
        };

        for (int caseNo = 0; caseNo < inputs.length; caseNo++) {
            try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    OutputStreamStreamOutput osso = new OutputStreamStreamOutput(bos)) {
                Dummy d = new Dummy(inputs[caseNo]);
                d.writeTo(osso);

                ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
                InputStreamStreamInput issi = new InputStreamStreamInput(bis);

                GetApiKeyRequest request = new GetApiKeyRequest(issi);
                ActionRequestValidationException ve = request.validate();
                assertNotNull(ve);
                assertEquals(expectedErrorMessages[caseNo].length, ve.validationErrors().size());
                assertThat(ve.validationErrors(), containsInAnyOrder(expectedErrorMessages[caseNo]));
            }
        }
    }

    public void testSerialization() throws IOException {
        final String apiKeyId = randomAlphaOfLength(5);
        final boolean ownedByAuthenticatedUser = true;
        GetApiKeyRequest getApiKeyRequest = GetApiKeyRequest.usingApiKeyId(apiKeyId, ownedByAuthenticatedUser);
        {
            ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
            OutputStreamStreamOutput out = new OutputStreamStreamOutput(outBuffer);
            out.setVersion(randomVersionBetween(random(), Version.V_7_0_0, Version.V_7_3_0));
            getApiKeyRequest.writeTo(out);

            InputStreamStreamInput inputStreamStreamInput = new InputStreamStreamInput(new ByteArrayInputStream(outBuffer.toByteArray()));
            inputStreamStreamInput.setVersion(randomVersionBetween(random(), Version.V_7_0_0, Version.V_7_3_0));
            GetApiKeyRequest requestFromInputStream = new GetApiKeyRequest(inputStreamStreamInput);

            assertThat(requestFromInputStream.getApiKeyId(), equalTo(getApiKeyRequest.getApiKeyId()));
            // old version so the default for `ownedByAuthenticatedUser` is false
            assertThat(requestFromInputStream.ownedByAuthenticatedUser(), is(false));
        }
        {
            ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
            OutputStreamStreamOutput out = new OutputStreamStreamOutput(outBuffer);
            out.setVersion(randomVersionBetween(random(), Version.V_7_4_0, Version.CURRENT));
            getApiKeyRequest.writeTo(out);

            InputStreamStreamInput inputStreamStreamInput = new InputStreamStreamInput(new ByteArrayInputStream(outBuffer.toByteArray()));
            inputStreamStreamInput.setVersion(randomVersionBetween(random(), Version.V_7_4_0, Version.CURRENT));
            GetApiKeyRequest requestFromInputStream = new GetApiKeyRequest(inputStreamStreamInput);

            assertThat(requestFromInputStream, equalTo(getApiKeyRequest));
        }
    }

    private static String randomNullOrEmptyString() {
        return randomBoolean() ? "" : null;
    }
}
