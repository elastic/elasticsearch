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

import static org.hamcrest.Matchers.containsString;

public class GetApiKeyRequestTests extends ESTestCase {

    public void testRequestValidation() {
        GetApiKeyRequest request = GetApiKeyRequest.usingApiKeyId(randomAlphaOfLength(5));
        ActionRequestValidationException ve = request.validate();
        assertNull(ve);
        request = GetApiKeyRequest.usingApiKeyName(randomAlphaOfLength(5));
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

        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); OutputStreamStreamOutput osso = new OutputStreamStreamOutput(bos)) {
            Dummy d = new Dummy(new String[] { "realm", "user", "api-kid", "api-kname" });
            d.writeTo(osso);

            ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
            InputStreamStreamInput issi = new InputStreamStreamInput(bis);

            GetApiKeyRequest request = new GetApiKeyRequest(issi);
            ActionRequestValidationException ve = request.validate();
            assertNotNull(ve);
            assertEquals(2, ve.validationErrors().size());
            assertThat(ve.validationErrors().get(0),
                    containsString("api key id must not be specified when username or realm name is specified"));
            assertThat(ve.validationErrors().get(1),
                    containsString("api key name must not be specified when username or realm name is specified"));
        }

        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); OutputStreamStreamOutput osso = new OutputStreamStreamOutput(bos)) {
            Dummy d = new Dummy(new String[] { null, null, "api-kid", "api-kname" });
            d.writeTo(osso);

            ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
            InputStreamStreamInput issi = new InputStreamStreamInput(bis);

            GetApiKeyRequest request = new GetApiKeyRequest(issi);
            ActionRequestValidationException ve = request.validate();
            assertNotNull(ve);
            assertEquals(1, ve.validationErrors().size());
            assertThat(ve.validationErrors().get(0),
                    containsString("api key name must not be specified when api key id is specified"));
        }

        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); OutputStreamStreamOutput osso = new OutputStreamStreamOutput(bos)) {
            Dummy d = new Dummy(new String[] { "realm", null, null, "api-kname" });
            d.writeTo(osso);

            ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
            InputStreamStreamInput issi = new InputStreamStreamInput(bis);

            GetApiKeyRequest request = new GetApiKeyRequest(issi);
            ActionRequestValidationException ve = request.validate();
            assertNotNull(ve);
            assertEquals(1, ve.validationErrors().size());
            assertThat(ve.validationErrors().get(0),
                    containsString("api key name must not be specified when username or realm name is specified"));
        }
    }
}
