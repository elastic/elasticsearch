/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.namedcredentials;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class NamedCredentialTests extends ESTestCase {

    public void testToXContentRedactsAuthWhenNull() throws IOException {
        NamedCredential cred = new NamedCredential(
            "my-cred",
            CredentialAuthType.BASIC,
            "https://example.com",
            Map.of(),
            null,
            1000L,
            2000L
        );
        String json = Strings.toString(cred);
        assertThat(json, containsString("\"auth\":\"::es_redacted::\""));
        assertThat(json, containsString("\"name\":\"my-cred\""));
        assertThat(json, containsString("\"auth_type\":\"basic\""));
        assertThat(json, containsString("\"url\":\"https://example.com\""));
    }

    public void testToXContentIncludesPlaintextAuthWhenPresent() throws IOException {
        NamedCredential cred = new NamedCredential(
            "my-cred",
            CredentialAuthType.BASIC,
            null,
            Map.of(),
            Map.of("username", "u", "password", "p"),
            1000L,
            2000L
        );
        String json = Strings.toString(cred);
        assertThat(json, containsString("\"username\":\"u\""));
        assertThat(json, not(containsString("::es_redacted::")));
        assertThat(json, not(containsString("\"url\"")));
    }

    public void testTimestampsRenderedAsIso8601() throws IOException {
        NamedCredential cred = new NamedCredential("c", CredentialAuthType.BEARER, null, Map.of(), null, 0L, 0L);
        String json = Strings.toString(cred);
        assertThat(json, containsString("\"created_at\":\"1970-01-01T00:00:00Z\""));
    }
}
