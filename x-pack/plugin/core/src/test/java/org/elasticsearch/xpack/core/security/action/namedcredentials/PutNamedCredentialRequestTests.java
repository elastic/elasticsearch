/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.namedcredentials;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class PutNamedCredentialRequestTests extends ESTestCase {

    private PutNamedCredentialAction.Request parse(String name, String body) throws IOException {
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), body)) {
            return PutNamedCredentialAction.Request.fromXContent(name, parser);
        }
    }

    public void testParseFullBody() throws IOException {
        PutNamedCredentialAction.Request request = parse("my-servicenow", """
            {
              "auth_type": "oauth_client_credentials",
              "url": "https://instance.service-now.com",
              "config": { "token_url": "https://instance.service-now.com/oauth_token.do", "scope": "read write" },
              "auth": { "client_id": "my-client-id", "client_secret": "super-secret" }
            }""");
        assertThat(request.credentialName(), equalTo("my-servicenow"));
        assertThat(request.authType(), is(CredentialAuthType.OAUTH_CLIENT_CREDENTIALS));
        assertThat(request.url(), equalTo("https://instance.service-now.com"));
        assertThat(request.config(), hasEntry("scope", "read write"));
        assertThat(request.auth(), hasEntry("client_secret", "super-secret"));
        assertThat(request.validate(), nullValue());
    }

    public void testParseMinimalBodyOmittingAuth() throws IOException {
        PutNamedCredentialAction.Request request = parse("c1", """
            { "auth_type": "basic" }""");
        assertThat(request.auth(), nullValue());
        // config is never null in PUT; absent means empty map
        assertThat(request.config(), equalTo(Map.of()));
        // validate() should fail: auth is required for PUT
        assertThat(request.validate(), notNullValue());
    }

    public void testValidateRequiresAuth() throws IOException {
        PutNamedCredentialAction.Request request = parse("my-cred", """
            { "auth_type": "basic", "url": "https://example.com" }""");
        ActionRequestValidationException e = request.validate();
        assertThat(e, notNullValue());
        assertThat(e.getMessage(), containsString("auth is required for PUT"));
    }

    public void testUnknownAuthTypeFailsParsing() {
        Exception e = expectThrows(Exception.class, () -> parse("c1", """
            { "auth_type": "carrier_pigeon" }"""));
        assertThat(e.getMessage(), containsString("carrier_pigeon"));
    }

    public void testValidateRejectsBadNames() throws IOException {
        for (String bad : new String[] { "Uppercase", "_leading", "-leading", "has space", "a".repeat(257) }) {
            PutNamedCredentialAction.Request request = new PutNamedCredentialAction.Request(
                bad,
                CredentialAuthType.BASIC,
                null,
                null,
                Map.of("username", "u", "password", "p")
            );
            assertThat("expected validation error for [" + bad + "]", request.validate(), notNullValue());
        }
    }

    public void testValidateRejectsUnknownConfigField() {
        PutNamedCredentialAction.Request request = new PutNamedCredentialAction.Request(
            "c1",
            CredentialAuthType.BASIC,
            null,
            Map.of("bogus", "x"),
            Map.of("username", "u", "password", "p")
        );
        ActionRequestValidationException e = request.validate();
        assertThat(e, notNullValue());
        assertThat(e.getMessage(), containsString("bogus"));
    }

    public void testValidateRejectsIncompleteAuthWhenProvided() {
        PutNamedCredentialAction.Request request = new PutNamedCredentialAction.Request(
            "c1",
            CredentialAuthType.BASIC,
            null,
            null,
            Map.of("username", "u")
        );
        ActionRequestValidationException e = request.validate();
        assertThat(e, notNullValue());
        assertThat(e.getMessage(), containsString("password"));
    }

    public void testValidateRejectsEmptyAuthMap() {
        PutNamedCredentialAction.Request request = new PutNamedCredentialAction.Request(
            "c1",
            CredentialAuthType.BASIC,
            null,
            null,
            Map.of()
        );
        assertThat(request.validate(), notNullValue());
    }

    public void testResponseCreatedFlagAndStatus() throws IOException {
        PutNamedCredentialAction.Response created = new PutNamedCredentialAction.Response(true);
        assertThat(created.created(), is(true));
        assertThat(created.status(), equalTo(RestStatus.CREATED));
        assertThat(Strings.toString(created), containsString("\"created\":true"));

        PutNamedCredentialAction.Response updated = new PutNamedCredentialAction.Response(false);
        assertThat(updated.created(), is(false));
        assertThat(updated.status(), equalTo(RestStatus.OK));
        assertThat(Strings.toString(updated), containsString("\"created\":false"));
    }
}
