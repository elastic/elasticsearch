/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License v 1".
 */
package org.elasticsearch.plugin.security.cloudiam;

import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;

import static org.hamcrest.Matchers.is;

public class CloudIamTokenTests extends ESTestCase {
    public void testParseValidHeader() {
        String json = """
            {
              "Action": "GetCallerIdentity",
              "Version": "2015-04-01",
              "AccessKeyId": "AKID",
              "Signature": "mock",
              "SignatureMethod": "HMAC-SHA1",
              "SignatureVersion": "1.0",
              "SignatureNonce": "abc",
              "Timestamp": "2025-01-01T00:00:00Z"
            }
            """;
        String header = Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
        CloudIamToken token = CloudIamToken.fromHeaders(header, 8192);
        assertThat(token.isValid(), is(true));
        assertThat(token.accessKeyId(), is("AKID"));
        assertThat(token.timestamp(), is(Instant.parse("2025-01-01T00:00:00Z")));
        assertThat(token.nonce(), is("abc"));
        assertThat(token.signature(), is("mock"));
    }

    public void testParseMissingFields() {
        String json = """
            {
              "Action": "GetCallerIdentity",
              "Version": "2015-04-01",
              "AccessKeyId": "AKID",
              "SignatureMethod": "HMAC-SHA1",
              "SignatureVersion": "1.0",
              "SignatureNonce": "abc",
              "Timestamp": "2025-01-01T00:00:00Z"
            }
            """;
        String header = Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
        CloudIamToken token = CloudIamToken.fromHeaders(header, 8192);
        assertThat(token.isValid(), is(false));
        assertThat(token.validationError(), is("missing required signed fields"));
    }

    public void testRejectsInvalidBase64() {
        CloudIamToken token = CloudIamToken.fromHeaders("not-base64!", 1024);
        assertThat(token.isValid(), is(false));
        assertThat(token.validationError(), is("invalid signed header"));
    }

    public void testRejectsInvalidJson() {
        String header = Base64.getEncoder().encodeToString("not-json".getBytes(StandardCharsets.UTF_8));
        CloudIamToken token = CloudIamToken.fromHeaders(header, 1024);
        assertThat(token.isValid(), is(false));
        assertThat(token.validationError(), is("invalid signed json"));
    }

    public void testRejectsInvalidTimestamp() {
        String json = """
            {
              "Action": "GetCallerIdentity",
              "Version": "2015-04-01",
              "AccessKeyId": "AKID",
              "Signature": "mock",
              "SignatureMethod": "HMAC-SHA1",
              "SignatureVersion": "1.0",
              "SignatureNonce": "abc",
              "Timestamp": "not-a-time"
            }
            """;
        String header = Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
        CloudIamToken token = CloudIamToken.fromHeaders(header, 8192);
        assertThat(token.isValid(), is(false));
        assertThat(token.validationError(), is("invalid timestamp"));
    }

    public void testRejectsOversizedHeader() {
        String json = """
            {
              "Action": "GetCallerIdentity",
              "Version": "2015-04-01",
              "AccessKeyId": "AKID",
              "Signature": "mock",
              "SignatureMethod": "HMAC-SHA1",
              "SignatureVersion": "1.0",
              "SignatureNonce": "abc",
              "Timestamp": "2025-01-01T00:00:00Z"
            }
            """;
        String header = Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
        CloudIamToken token = CloudIamToken.fromHeaders(header, 8);
        assertThat(token.isValid(), is(false));
        assertThat(token.validationError(), is("signed header too large"));
    }

    public void testParsesSessionToken() {
        String json = """
            {
              "Action": "GetCallerIdentity",
              "Version": "2015-04-01",
              "AccessKeyId": "AKID",
              "Signature": "mock",
              "SignatureMethod": "HMAC-SHA1",
              "SignatureVersion": "1.0",
              "SignatureNonce": "abc",
              "Timestamp": "2025-01-01T00:00:00Z",
              "SecurityToken": "sts-token"
            }
            """;
        String header = Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
        CloudIamToken token = CloudIamToken.fromHeaders(header, 8192);
        assertThat(token.isValid(), is(true));
        assertThat(token.sessionToken(), is("sts-token"));
    }
}
