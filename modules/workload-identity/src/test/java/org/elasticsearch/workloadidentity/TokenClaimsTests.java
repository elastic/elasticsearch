/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workloadidentity;

import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class TokenClaimsTests extends ESTestCase {

    public void testDecodesSubjectAndJti() {
        TokenClaims claims = TokenClaims.decode(jwt("{\"sub\":\"deployment:abc\",\"jti\":\"session-1\"}"));
        assertEquals("deployment:abc", claims.subject());
        assertEquals("session-1", claims.sessionId());
    }

    public void testNoSessionClaim() {
        TokenClaims claims = TokenClaims.decode(jwt("{\"sub\":\"deployment:abc\"}"));
        assertEquals("deployment:abc", claims.subject());
        assertNull(claims.sessionId());
    }

    public void testNonStringAndEmptyClaimsIgnored() {
        TokenClaims claims = TokenClaims.decode(jwt("{\"sub\":42,\"jti\":[\"x\"]}"));
        assertNull(claims.subject());
        assertNull(claims.sessionId());
        assertNull(TokenClaims.decode(jwt("{\"jti\":\"\"}")).sessionId());
    }

    public void testMalformedInputsYieldEmpty() {
        for (String malformed : new String[] {
            "",
            "not-a-jwt",
            "one.dot",
            "a.!!!not-base64!!!.c",
            "a." + base64Url("{\"sub\":") + ".c",
            "a." + base64Url("[1,2]") + ".c" }) {
            assertEquals("input: " + malformed, TokenClaims.EMPTY, TokenClaims.decode(malformed));
        }
    }

    private static String jwt(String payloadJson) {
        return base64Url("{\"alg\":\"RS256\"}") + "." + base64Url(payloadJson) + ".signature";
    }

    private static String base64Url(String value) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }
}
