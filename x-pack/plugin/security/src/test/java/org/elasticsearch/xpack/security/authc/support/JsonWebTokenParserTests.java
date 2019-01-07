/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.security.authc.oidc.RPConfiguration;
import org.elasticsearch.xpack.security.authc.support.jwt.JsonWebToken;
import org.elasticsearch.xpack.security.authc.support.jwt.JsonWebTokenParser;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class JsonWebTokenParserTests extends ESTestCase {

    public void testIdTokenParsing() throws IOException {
        final String serializedJwt = "eyJhbGciOiJIUzI1NiIsImtpZCI6IjFlOWdkazcifQ.eyJpc3MiOiJodHRwOi8vc2VydmVyLmV4YW1wbGUuY29tIiwic3ViIjo" +
            "iMjQ4Mjg5NzYxMDAxIiwiYXVkIjoiczZCaGRSa3F0MyIsIm5vbmNlIjoibi0wUzZfV3pBMk1qIiwiZXhwIjoxMzExMjgxOTcwLCJpYXQiOjEzMTEyODA5NzAsIm" +
            "5hbWUiOiJKYW5lIERvZSIsImdpdmVuX25hbWUiOiJKYW5lIiwiZmFtaWx5X25hbWUiOiJEb2UiLCJnZW5kZXIiOiJmZW1hbGUiLCJiaXJ0aGRhdGUiOiIxOTk0L" +
            "TEwLTMxIiwiZW1haWwiOiJqYW5lZG9lQGV4YW1wbGUuY29tIiwicGljdHVyZSI6Imh0dHA6Ly9leGFtcGxlLmNvbS9qYW5lZG9lL21lLmpwZyJ9.XY8hKQ6nx8K" +
            "EfuB907SuImosemSt7qPlg3HAJH85JKI";

        JsonWebTokenParser jwtParser = new JsonWebTokenParser(new RPConfiguration("clientId", "redirectUri", "code", null, null));
        final SecretKeySpec keySpec = new SecretKeySpec("ffff".getBytes(), "HmacSHA256");
        JsonWebToken jwt = jwtParser.parseAndValidateJwt(serializedJwt, keySpec);
        assertTrue(jwt.getPayload().containsKey("iss"));
        assertThat(jwt.getPayload().get("iss"), equalTo("http://server.example.com"));
        assertTrue(jwt.getPayload().containsKey("sub"));
        assertThat(jwt.getPayload().get("sub"), equalTo("248289761001"));
        assertTrue(jwt.getPayload().containsKey("aud"));
        List<String> aud = (List<String>) jwt.getPayload().get("aud");
        assertThat(aud.size(), equalTo(1));
        assertTrue(aud.contains("s6BhdRkqt3"));
        assertTrue(jwt.getPayload().containsKey("nonce"));
        assertThat(jwt.getPayload().get("nonce"), equalTo("n-0S6_WzA2Mj"));
        assertTrue(jwt.getPayload().containsKey("exp"));
        assertThat(jwt.getPayload().get("exp"), equalTo(1311281970L));
        assertTrue(jwt.getPayload().containsKey("iat"));
        assertThat(jwt.getPayload().get("iat"), equalTo(1311280970L));
        assertTrue(jwt.getPayload().containsKey("name"));
        assertThat(jwt.getPayload().get("name"), equalTo("Jane Doe"));
        assertTrue(jwt.getPayload().containsKey("given_name"));
        assertThat(jwt.getPayload().get("given_name"), equalTo("Jane"));
        assertTrue(jwt.getPayload().containsKey("family_name"));
        assertThat(jwt.getPayload().get("family_name"), equalTo("Doe"));
        assertTrue(jwt.getPayload().containsKey("gender"));
        assertThat(jwt.getPayload().get("gender"), equalTo("female"));
        assertTrue(jwt.getPayload().containsKey("birthdate"));
        assertThat(jwt.getPayload().get("birthdate"), equalTo("1994-10-31"));
        assertTrue(jwt.getPayload().containsKey("email"));
        assertThat(jwt.getPayload().get("email"), equalTo("janedoe@example.com"));
        assertTrue(jwt.getPayload().containsKey("picture"));
        assertThat(jwt.getPayload().get("picture"), equalTo("http://example.com/janedoe/me.jpg"));
        assertTrue(jwt.getHeader().containsKey("alg"));
        assertThat(jwt.getHeader().get("alg"), equalTo("HS256"));
        assertTrue(jwt.getHeader().containsKey("kid"));
        assertThat(jwt.getHeader().get("kid"), equalTo("1e9gdk7"));

    }

    public void testIdTokenWithPrivateClaimsParsing() throws IOException {
        final String serializedJwt = "eyJhbGciOiJIUzI1NiIsImtpZCI6IjFlOWdkazcifQ.eyJpc3MiOiJodHRwOi8vc2VydmVyLmV4YW1wbGUuY29tIiwic3ViI" +
            "joiMjQ4Mjg5NzYxMDAxIiwiYXVkIjoiczZCaGRSa3F0MyIsIm5vbmNlIjoibi0wUzZfV3pBMk1qIiwiZXhwIjoxMzExMjgxOTcwLCJpYXQiOjEzMTEyODA5Nz" +
            "AsIm5hbWUiOiJKYW5lIERvZSIsImdpdmVuX25hbWUiOiJKYW5lIiwiZmFtaWx5X25hbWUiOiJEb2UiLCJnZW5kZXIiOiJmZW1hbGUiLCJjbGFpbTEiOiJ2YWx" +
            "1ZTEiLCJjbGFpbTIiOiJ2YWx1ZTIiLCJjbGFpbTMiOiJ2YWx1ZTMiLCJjbGFpbTQiOiJ2YWx1ZTQiLCJiaXJ0aGRhdGUiOiIxOTk0LTEwLTMxIiwiZW1haWwi" +
            "OiJqYW5lZG9lQGV4YW1wbGUuY29tIiwicGljdHVyZSI6Imh0dHA6Ly9leGFtcGxlLmNvbS9qYW5lZG9lL21lLmpwZyIsImFkZHJlc3MiOnsiY291bnRyeSI6I" +
            "kdyZWVjZSIsInJlZ2lvbiI6IkV2aWEifX0.K9nnZaiuF0z8wJUrJQSJSMKQtql3O6xMPYxyEOa7uC4";
        JsonWebTokenParser jwtParser = new JsonWebTokenParser(new RPConfiguration("clientId", "redirectUri", "code", null,
            Arrays.asList("claim1", "claim2", "claim3", "claim4")));
        final SecretKeySpec keySpec = new SecretKeySpec("ffff".getBytes(), "HmacSHA256");
        JsonWebToken jwt = jwtParser.parseAndValidateJwt(serializedJwt, keySpec);
        assertTrue(jwt.getPayload().containsKey("iss"));
        assertThat(jwt.getPayload().get("iss"), equalTo("http://server.example.com"));
        assertTrue(jwt.getPayload().containsKey("sub"));
        assertThat(jwt.getPayload().get("sub"), equalTo("248289761001"));
        assertTrue(jwt.getPayload().containsKey("aud"));
        List<String> aud = (List<String>) jwt.getPayload().get("aud");
        assertThat(aud.size(), equalTo(1));
        assertTrue(aud.contains("s6BhdRkqt3"));
        assertTrue(jwt.getPayload().containsKey("nonce"));
        assertThat(jwt.getPayload().get("nonce"), equalTo("n-0S6_WzA2Mj"));
        assertTrue(jwt.getPayload().containsKey("exp"));
        assertThat(jwt.getPayload().get("exp"), equalTo(1311281970L));
        assertTrue(jwt.getPayload().containsKey("iat"));
        assertThat(jwt.getPayload().get("iat"), equalTo(1311280970L));
        assertTrue(jwt.getPayload().containsKey("name"));
        assertThat(jwt.getPayload().get("name"), equalTo("Jane Doe"));
        assertTrue(jwt.getPayload().containsKey("given_name"));
        assertThat(jwt.getPayload().get("given_name"), equalTo("Jane"));
        assertTrue(jwt.getPayload().containsKey("family_name"));
        assertThat(jwt.getPayload().get("family_name"), equalTo("Doe"));
        assertTrue(jwt.getPayload().containsKey("gender"));
        assertThat(jwt.getPayload().get("gender"), equalTo("female"));
        assertTrue(jwt.getPayload().containsKey("birthdate"));
        assertThat(jwt.getPayload().get("birthdate"), equalTo("1994-10-31"));
        assertTrue(jwt.getPayload().containsKey("email"));
        assertThat(jwt.getPayload().get("email"), equalTo("janedoe@example.com"));
        assertTrue(jwt.getPayload().containsKey("picture"));
        assertThat(jwt.getPayload().get("picture"), equalTo("http://example.com/janedoe/me.jpg"));
        assertTrue(jwt.getPayload().containsKey("claim1"));
        assertThat(jwt.getPayload().get("claim1"), equalTo("value1"));
        assertTrue(jwt.getPayload().containsKey("claim2"));
        assertThat(jwt.getPayload().get("claim2"), equalTo("value2"));
        assertTrue(jwt.getPayload().containsKey("claim3"));
        assertThat(jwt.getPayload().get("claim3"), equalTo("value3"));
        assertTrue(jwt.getPayload().containsKey("claim4"));
        assertThat(jwt.getPayload().get("claim4"), equalTo("value4"));
        assertTrue(jwt.getPayload().containsKey("address"));
        Map<String, Object> expectedAddress = new HashMap<>();
        expectedAddress.put("country", "Greece");
        expectedAddress.put("region", "Evia");
        assertThat(jwt.getPayload().get("address"), equalTo(expectedAddress));
        assertTrue(jwt.getHeader().containsKey("alg"));
        assertThat(jwt.getHeader().get("alg"), equalTo("HS256"));
        assertTrue(jwt.getHeader().containsKey("kid"));
        assertThat(jwt.getHeader().get("kid"), equalTo("1e9gdk7"));
    }

    public void testIdTokenWithMutipleAudiencesParsing() throws IOException {
        final String serializedJwt = "eyJhbGciOiJIUzI1NiIsImtpZCI6IjFlOWdkazcifQ.eyJpc3MiOiJodHRwOi8vc2VydmVyLmV4YW1wbGUuY29tIiwic3ViI" +
            "joiMjQ4Mjg5NzYxMDAxIiwiYXVkIjpbInM2QmhkUmtxdDMiLCJvdGhlcl9hdWRpZW5jZSJdLCJub25jZSI6Im4tMFM2X1d6QTJNaiIsImV4cCI6MTMxMTI4MT" +
            "k3MCwiaWF0IjoxMzExMjgwOTcwLCJuYW1lIjoiSmFuZSBEb2UiLCJnaXZlbl9uYW1lIjoiSmFuZSIsImZhbWlseV9uYW1lIjoiRG9lIiwiZ2VuZGVyIjoiZmV" +
            "tYWxlIiwiY2xhaW0xIjoidmFsdWUxIiwiY2xhaW0yIjoidmFsdWUyIiwiY2xhaW0zIjoidmFsdWUzIiwiY2xhaW00IjoidmFsdWU0IiwiYmlydGhkYXRlIjoi" +
            "MTk5NC0xMC0zMSIsImVtYWlsIjoiamFuZWRvZUBleGFtcGxlLmNvbSIsInBpY3R1cmUiOiJodHRwOi8vZXhhbXBsZS5jb20vamFuZWRvZS9tZS5qcGcifQ.xn" +
            "HQXmN17lnkkBM-DX3kFRfr7Edk1OYoAPpCwCFOsvA";
        JsonWebTokenParser jwtParser = new JsonWebTokenParser(new RPConfiguration("clientId", "redirectUri", "code", null,
            Arrays.asList("claim1", "claim2", "claim3", "claim4")));
        final SecretKeySpec keySpec = new SecretKeySpec("ffff".getBytes(), "HmacSHA256");
        JsonWebToken jwt = jwtParser.parseAndValidateJwt(serializedJwt, keySpec);
        assertTrue(jwt.getPayload().containsKey("iss"));
        assertThat(jwt.getPayload().get("iss"), equalTo("http://server.example.com"));
        assertTrue(jwt.getPayload().containsKey("sub"));
        assertThat(jwt.getPayload().get("sub"), equalTo("248289761001"));
        assertTrue(jwt.getPayload().containsKey("aud"));
        List<String> aud = (List<String>) jwt.getPayload().get("aud");
        assertThat(aud.size(), equalTo(2));
        assertTrue(aud.contains("s6BhdRkqt3"));
        assertTrue(aud.contains("other_audience"));
        assertTrue(jwt.getPayload().containsKey("nonce"));
        assertThat(jwt.getPayload().get("nonce"), equalTo("n-0S6_WzA2Mj"));
        assertTrue(jwt.getPayload().containsKey("exp"));
        assertThat(jwt.getPayload().get("exp"), equalTo(1311281970L));
        assertTrue(jwt.getPayload().containsKey("iat"));
        assertThat(jwt.getPayload().get("iat"), equalTo(1311280970L));
        assertTrue(jwt.getPayload().containsKey("name"));
        assertThat(jwt.getPayload().get("name"), equalTo("Jane Doe"));
        assertTrue(jwt.getPayload().containsKey("given_name"));
        assertThat(jwt.getPayload().get("given_name"), equalTo("Jane"));
        assertTrue(jwt.getPayload().containsKey("family_name"));
        assertThat(jwt.getPayload().get("family_name"), equalTo("Doe"));
        assertTrue(jwt.getPayload().containsKey("gender"));
        assertThat(jwt.getPayload().get("gender"), equalTo("female"));
        assertTrue(jwt.getPayload().containsKey("birthdate"));
        assertThat(jwt.getPayload().get("birthdate"), equalTo("1994-10-31"));
        assertTrue(jwt.getPayload().containsKey("email"));
        assertThat(jwt.getPayload().get("email"), equalTo("janedoe@example.com"));
        assertTrue(jwt.getPayload().containsKey("picture"));
        assertThat(jwt.getPayload().get("picture"), equalTo("http://example.com/janedoe/me.jpg"));
        assertTrue(jwt.getPayload().containsKey("claim1"));
        assertThat(jwt.getPayload().get("claim1"), equalTo("value1"));
        assertTrue(jwt.getPayload().containsKey("claim2"));
        assertThat(jwt.getPayload().get("claim2"), equalTo("value2"));
        assertTrue(jwt.getPayload().containsKey("claim3"));
        assertThat(jwt.getPayload().get("claim3"), equalTo("value3"));
        assertTrue(jwt.getPayload().containsKey("claim4"));
        assertThat(jwt.getPayload().get("claim4"), equalTo("value4"));
        assertTrue(jwt.getHeader().containsKey("alg"));
        assertThat(jwt.getHeader().get("alg"), equalTo("HS256"));
        assertTrue(jwt.getHeader().containsKey("kid"));
        assertThat(jwt.getHeader().get("kid"), equalTo("1e9gdk7"));
    }
}
