/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.function.Supplier;

import static org.elasticsearch.test.VersionUtils.getPreviousVersion;
import static org.elasticsearch.test.VersionUtils.randomVersionBetween;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class InvalidateApiKeyRequestTests extends ESTestCase {

    public void testNonNullIdsCannotBeEmptyNorContainBlankId() {
        ActionRequestValidationException validationException = expectThrows(
            ActionRequestValidationException.class,
            () -> new InvalidateApiKeyRequest(
                randomFrom(randomNullOrEmptyString(), randomAlphaOfLength(8)),
                randomFrom(randomNullOrEmptyString(), randomAlphaOfLength(8)),
                randomFrom(randomNullOrEmptyString(), randomAlphaOfLength(8)),
                false,
                new String[] {}
            )
        );
        assertThat(validationException.getMessage(), containsString("Field [ids] cannot be an empty array"));

        validationException = expectThrows(
            ActionRequestValidationException.class,
            () -> new InvalidateApiKeyRequest(
                randomFrom(randomNullOrEmptyString(), randomAlphaOfLength(8)),
                randomFrom(randomNullOrEmptyString(), randomAlphaOfLength(8)),
                randomFrom(randomNullOrEmptyString(), randomAlphaOfLength(8)),
                false,
                new String[] { randomAlphaOfLength(12), null }
            )
        );
        assertThat(
            validationException.getMessage(),
            containsString("Field [ids] must not contain blank id, " + "but got blank id at index position: [1]")
        );
    }

    public void testEmptyStringsAreCoercedToNull() {
        Supplier<String> randomBlankString = () -> " ".repeat(randomIntBetween(0, 5));
        final InvalidateApiKeyRequest request = new InvalidateApiKeyRequest(
            randomBlankString.get(), // realm name
            randomBlankString.get(), // user name
            randomBlankString.get(), // key name
            randomBoolean(), // owned by user
            null
        );
        assertThat(request.getRealmName(), nullValue());
        assertThat(request.getUserName(), nullValue());
        assertThat(request.getIds(), nullValue());
        assertThat(request.getName(), nullValue());
    }

    public void testRequestValidation() {
        InvalidateApiKeyRequest request = InvalidateApiKeyRequest.usingApiKeyId(randomAlphaOfLength(5), randomBoolean());
        ActionRequestValidationException ve = request.validate();
        assertNull(ve);
        request = InvalidateApiKeyRequest.usingApiKeyName(randomAlphaOfLength(5), randomBoolean());
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
                if (out.getVersion().onOrAfter(Version.V_7_10_0)) {
                    if (Strings.hasText(apiKeyId)) {
                        out.writeOptionalStringArray(new String[] { apiKeyId });
                    } else {
                        out.writeOptionalStringArray(null);
                    }
                } else {
                    out.writeOptionalString(apiKeyId);
                }
                out.writeOptionalString(apiKeyName);
                out.writeOptionalBoolean(ownedByAuthenticatedUser);
            }
        }

        String[][] inputs = new String[][] {
            { randomNullOrEmptyString(), randomNullOrEmptyString(), randomNullOrEmptyString(), randomNullOrEmptyString(), "false" },
            { randomNullOrEmptyString(), "user", "api-kid", "api-kname", "false" },
            { "realm", randomNullOrEmptyString(), "api-kid", "api-kname", "false" },
            { "realm", "user", "api-kid", randomNullOrEmptyString(), "false" },
            { randomNullOrEmptyString(), randomNullOrEmptyString(), "api-kid", "api-kname", "false" },
            { "realm", randomNullOrEmptyString(), randomNullOrEmptyString(), randomNullOrEmptyString(), "true" },
            { randomNullOrEmptyString(), "user", randomNullOrEmptyString(), randomNullOrEmptyString(), "true" }, };
        String[][] expectedErrorMessages = new String[][] {
            { "One of [api key id(s), api key name, username, realm name] must be specified if [owner] flag is false" },
            {
                "username or realm name must not be specified when the api key id(s) or api key name are specified",
                "only one of [api key id(s), api key name] can be specified" },
            {
                "username or realm name must not be specified when the api key id(s) or api key name are specified",
                "only one of [api key id(s), api key name] can be specified" },
            { "username or realm name must not be specified when the api key id(s) or api key name are specified" },
            { "only one of [api key id(s), api key name] can be specified" },
            { "neither username nor realm-name may be specified when invalidating owned API keys" },
            { "neither username nor realm-name may be specified when invalidating owned API keys" } };

        for (int caseNo = 0; caseNo < inputs.length; caseNo++) {
            try (
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                OutputStreamStreamOutput osso = new OutputStreamStreamOutput(bos)
            ) {
                final Version streamVersion = randomVersionBetween(random(), Version.V_7_4_0, getPreviousVersion(Version.V_7_10_0));
                Dummy d = new Dummy(inputs[caseNo]);
                osso.setVersion(streamVersion);
                d.writeTo(osso);

                ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
                InputStreamStreamInput issi = new InputStreamStreamInput(bis);
                issi.setVersion(streamVersion);

                InvalidateApiKeyRequest request = new InvalidateApiKeyRequest(issi);
                ActionRequestValidationException ve = request.validate();
                assertNotNull(ve.getMessage(), ve);
                assertEquals(expectedErrorMessages[caseNo].length, ve.validationErrors().size());
                assertThat(ve.validationErrors(), containsInAnyOrder(expectedErrorMessages[caseNo]));
            }
        }
    }

    public void testSerialization() throws IOException {
        final String apiKeyId = randomAlphaOfLength(5);
        final boolean ownedByAuthenticatedUser = true;
        InvalidateApiKeyRequest invalidateApiKeyRequest = InvalidateApiKeyRequest.usingApiKeyId(apiKeyId, ownedByAuthenticatedUser);
        {
            ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
            OutputStreamStreamOutput out = new OutputStreamStreamOutput(outBuffer);
            out.setVersion(randomVersionBetween(random(), Version.V_7_0_0, Version.V_7_3_0));
            invalidateApiKeyRequest.writeTo(out);

            InputStreamStreamInput inputStreamStreamInput = new InputStreamStreamInput(new ByteArrayInputStream(outBuffer.toByteArray()));
            inputStreamStreamInput.setVersion(randomVersionBetween(random(), Version.V_7_0_0, Version.V_7_3_0));
            InvalidateApiKeyRequest requestFromInputStream = new InvalidateApiKeyRequest(inputStreamStreamInput);

            assertThat(requestFromInputStream.getIds(), equalTo(invalidateApiKeyRequest.getIds()));
            // old version so the default for `ownedByAuthenticatedUser` is false
            assertThat(requestFromInputStream.ownedByAuthenticatedUser(), is(false));
        }
        {
            ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
            OutputStreamStreamOutput out = new OutputStreamStreamOutput(outBuffer);
            out.setVersion(randomVersionBetween(random(), Version.V_7_4_0, Version.V_7_9_0));
            invalidateApiKeyRequest.writeTo(out);

            InputStreamStreamInput inputStreamStreamInput = new InputStreamStreamInput(new ByteArrayInputStream(outBuffer.toByteArray()));
            inputStreamStreamInput.setVersion(randomVersionBetween(random(), Version.V_7_4_0, Version.V_7_9_0));
            InvalidateApiKeyRequest requestFromInputStream = new InvalidateApiKeyRequest(inputStreamStreamInput);

            assertThat(requestFromInputStream, equalTo(invalidateApiKeyRequest));
        }
        {
            ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
            OutputStreamStreamOutput out = new OutputStreamStreamOutput(outBuffer);
            out.setVersion(randomVersionBetween(random(), Version.V_7_10_0, Version.CURRENT));
            invalidateApiKeyRequest.writeTo(out);

            InputStreamStreamInput inputStreamStreamInput = new InputStreamStreamInput(new ByteArrayInputStream(outBuffer.toByteArray()));
            inputStreamStreamInput.setVersion(randomVersionBetween(random(), Version.V_7_10_0, Version.CURRENT));
            InvalidateApiKeyRequest requestFromInputStream = new InvalidateApiKeyRequest(inputStreamStreamInput);

            assertThat(requestFromInputStream, equalTo(invalidateApiKeyRequest));
        }
    }

    public void testSerializationWillThrowWhenMultipleIdsAndOldVersionStream() {
        final InvalidateApiKeyRequest invalidateApiKeyRequest = new InvalidateApiKeyRequest(
            randomFrom(randomNullOrEmptyString(), randomAlphaOfLength(8)),
            randomFrom(randomNullOrEmptyString(), randomAlphaOfLength(8)),
            randomFrom(randomNullOrEmptyString(), randomAlphaOfLength(8)),
            false,
            new String[] { randomAlphaOfLength(12), randomAlphaOfLength(12) }
        );
        ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        OutputStreamStreamOutput out = new OutputStreamStreamOutput(outBuffer);
        out.setVersion(randomVersionBetween(random(), Version.V_7_4_0, getPreviousVersion(Version.V_7_10_0)));
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> invalidateApiKeyRequest.writeTo(out));
        assertThat(e.getMessage(), containsString("a request with multi-valued field [ids] cannot be sent to an older version"));
    }

    private static String randomNullOrEmptyString() {
        return randomFrom(new String[] { "", null });
    }

}
