/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.hamcrest.Matchers.containsInAnyOrder;

public class InvalidateApiKeyRequestTests extends ESTestCase {

    public void testRequestValidation() {
        InvalidateApiKeyRequest request = InvalidateApiKeyRequest.usingApiKeyId(randomAlphaOfLength(5));
        ActionRequestValidationException ve = request.validate();
        assertNull(ve);
        request = InvalidateApiKeyRequest.usingApiKeyName(randomAlphaOfLength(5));
        ve = request.validate();
        assertNull(ve);
        request = InvalidateApiKeyRequest.usingRealmName(randomAlphaOfLength(5));
        ve = request.validate();
        assertNull(ve);
        request = InvalidateApiKeyRequest.usingUserName(randomAlphaOfLength(5));
        ve = request.validate();
        assertNull(ve);
        request = InvalidateApiKeyRequest.usingRealmAndUserName(randomAlphaOfLength(5), randomAlphaOfLength(7));
        ve = request.validate();
        assertNull(ve);
    }

    public void testRequestValidationFailureScenarios() throws IOException {
        class Dummy extends ActionRequest {
            String realm;
            String user;
            String apiKeyId;
            String apiKeyName;

            Dummy(String[] a) {
                realm = a[0];
                user = a[1];
                apiKeyId = a[2];
                apiKeyName = a[3];
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
            }
        }

        String[][] inputs = new String[][] {
                { randomFrom(new String[] { null, "" }), randomFrom(new String[] { null, "" }), randomFrom(new String[] { null, "" }),
                        randomFrom(new String[] { null, "" }) },
                { randomFrom(new String[] { null, "" }), "user", "api-kid", "api-kname" },
                { "realm", randomFrom(new String[] { null, "" }), "api-kid", "api-kname" },
                { "realm", "user", "api-kid", randomFrom(new String[] { null, "" }) },
                { randomFrom(new String[] { null, "" }), randomFrom(new String[] { null, "" }), "api-kid", "api-kname" } };
        String[][] expectedErrorMessages = new String[][] { { "One of [api key id, api key name, username, realm name] must be specified" },
                { "username or realm name must not be specified when the api key id or api key name is specified",
                        "only one of [api key id, api key name] can be specified" },
                { "username or realm name must not be specified when the api key id or api key name is specified",
                        "only one of [api key id, api key name] can be specified" },
                { "username or realm name must not be specified when the api key id or api key name is specified" },
                { "only one of [api key id, api key name] can be specified" } };


        for (int caseNo = 0; caseNo < inputs.length; caseNo++) {
            try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    OutputStreamStreamOutput osso = new OutputStreamStreamOutput(bos)) {
                Dummy d = new Dummy(inputs[caseNo]);
                d.writeTo(osso);

                ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
                InputStreamStreamInput issi = new InputStreamStreamInput(bis);

                InvalidateApiKeyRequest request = new InvalidateApiKeyRequest(issi);
                ActionRequestValidationException ve = request.validate();
                assertNotNull(ve);
                assertEquals(expectedErrorMessages[caseNo].length, ve.validationErrors().size());
                assertThat(ve.validationErrors(), containsInAnyOrder(expectedErrorMessages[caseNo]));
            }
        }
    }
}
