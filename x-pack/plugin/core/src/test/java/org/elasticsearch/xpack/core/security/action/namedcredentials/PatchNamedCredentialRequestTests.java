/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.namedcredentials;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class PatchNamedCredentialRequestTests extends ESTestCase {

    private PatchNamedCredentialAction.Request parse(String name, String body) throws IOException {
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), body)) {
            return PatchNamedCredentialAction.Request.fromXContent(name, parser);
        }
    }

    public void testParseFullBody() throws IOException {
        PatchNamedCredentialAction.Request request = parse("my-cred", """
            {
              "auth_type": "basic",
              "url": "https://example.com",
              "config": {},
              "auth": { "username": "u", "password": "p" }
            }""");
        assertThat(request.credentialName(), equalTo("my-cred"));
        assertThat(request.authType(), is(CredentialAuthType.BASIC));
        assertThat(request.url(), equalTo("https://example.com"));
        assertThat(request.config(), equalTo(Map.of()));
        assertThat(request.auth(), equalTo(Map.of("username", "u", "password", "p")));
        assertThat(request.validate(), nullValue());
    }

    public void testParseMinimalBodyOnlyAuthType() throws IOException {
        PatchNamedCredentialAction.Request request = parse("c1", """
            { "auth_type": "bearer" }""");
        assertThat(request.authType(), is(CredentialAuthType.BEARER));
        assertThat(request.url(), nullValue());
        assertThat(request.config(), nullValue());
        assertThat(request.auth(), nullValue());
        assertThat(request.validate(), nullValue());
    }

    public void testParseMinimalBodyOnlyAuth() throws IOException {
        PatchNamedCredentialAction.Request request = parse("c1", """
            { "auth": { "token": "my-token" } }""");
        assertThat(request.authType(), nullValue());
        assertThat(request.auth(), equalTo(Map.of("token", "my-token")));
        // auth fields can't be validated without authType; validate() should pass
        assertThat(request.validate(), nullValue());
    }

    public void testEmptyBodyFailsValidation() throws IOException {
        PatchNamedCredentialAction.Request request = parse("c1", "{}");
        ActionRequestValidationException e = request.validate();
        assertThat(e, notNullValue());
        assertThat(e.getMessage(), containsString("at least one field must be provided"));
    }

    public void testBadCredentialNameFailsValidation() {
        PatchNamedCredentialAction.Request request = new PatchNamedCredentialAction.Request(
            "Uppercase",
            null,
            "https://example.com",
            null,
            null
        );
        assertThat(request.validate(), notNullValue());
    }

    public void testUnknownAuthTypeFailsParsing() {
        Exception e = expectThrows(Exception.class, () -> parse("c1", """
            { "auth_type": "carrier_pigeon" }"""));
        assertThat(e.getMessage(), containsString("carrier_pigeon"));
    }

    public void testInvalidAuthFieldsWhenAuthTypeProvided() throws IOException {
        // auth_type is basic; providing unknown field should fail
        PatchNamedCredentialAction.Request request = parse("c1", """
            {
              "auth_type": "basic",
              "auth": { "username": "u", "token": "bad-field" }
            }""");
        ActionRequestValidationException e = request.validate();
        assertThat(e, notNullValue());
        assertThat(e.getMessage(), containsString("token"));
    }

    public void testEmptyAuthFailsValidation() throws IOException {
        PatchNamedCredentialAction.Request request = parse("c1", """
            { "auth": {} }""");
        ActionRequestValidationException e = request.validate();
        assertThat(e, notNullValue());
        assertThat(e.getMessage(), containsString("auth must not be empty when provided"));
    }
}
