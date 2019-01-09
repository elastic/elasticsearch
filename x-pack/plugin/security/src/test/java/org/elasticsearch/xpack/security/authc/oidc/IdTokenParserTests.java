/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.oidc;

import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ssl.PemUtils;
import org.elasticsearch.xpack.security.authc.support.jwt.JsonWebToken;
import org.elasticsearch.xpack.security.authc.support.jwt.JsonWebTokenBuilder;
import org.elasticsearch.xpack.security.authc.support.jwt.SignatureAlgorithm;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.PublicKey;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class IdTokenParserTests extends ESTestCase {

    public void testIdTokenParsing() throws IOException {
        final String serializedJwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwOi8vb3AuZXhhbXBsZS5jb20iLCJzdWIiOiI" +
            "yNDgyODk3NjEwMDEiLCJuYW1lIjoiSmFuZSBEb2UiLCJhdWQiOiJzNkJoZFJrcXQzIiwibm9uY2UiOiJuLTBTNl9XekEyTWoiLCJpYXQiOjE1MTYy" +
            "MzkwMjIsImV4cCI6MTUxNjMzOTAyMiwiZ2l2ZW5fbmFtZSI6IkphbmUiLCJmYW1pbHlfbmFtZSI6IkRvZSIsImdlbmRlciI6ImZlbWFsZSIsImJpc" +
            "nRoZGF0ZSI6IjE5OTQtMTAtMzEiLCJlbWFpbCI6ImphbmVkb2VAZXhhbXBsZS5jb20iLCJwaWN0dXJlIjoiaHR0cDovL2V4YW1wbGUuY29tL2phbm" +
            "Vkb2UvbWUuanBnIn0.bpG9QZk9uykstyn2rv2w_7NkS-rerdX78_ehxli8RTM";
        RPConfiguration rpConfig = new RPConfiguration("clientId", "redirectUri", "code", SignatureAlgorithm.getAllNames(), null, null);
        IdTokenParser idTokenParser = new IdTokenParser(rpConfig);
        final SecretKeySpec keySpec =
            new SecretKeySpec("144753a689a6508d7c7cd02752d7138e".getBytes(StandardCharsets.UTF_8.name()), "HmacSHA256");
        IdToken idToken = idTokenParser.parseAndValidateIdToken(serializedJwt, keySpec);
        assertThat(idToken.getIssuer(), equalTo("http://op.example.com"));
        assertThat(idToken.getSubject(), equalTo("248289761001"));
        List<String> aud = idToken.getAudiences();
        assertThat(aud.size(), equalTo(1));
        assertTrue(aud.contains("s6BhdRkqt3"));
        assertTrue(idToken.getPayload().containsKey("nonce"));
        assertThat(idToken.getNonce(), equalTo("n-0S6_WzA2Mj"));
        assertThat(idToken.getExpiration(), equalTo(1516339022L));
        assertThat(idToken.getIssuedAt(), equalTo(1516239022L));
        assertTrue(idToken.getPayload().containsKey("name"));
        assertThat(idToken.getPayload().get("name"), equalTo("Jane Doe"));
        assertTrue(idToken.getPayload().containsKey("given_name"));
        assertThat(idToken.getPayload().get("given_name"), equalTo("Jane"));
        assertTrue(idToken.getPayload().containsKey("family_name"));
        assertThat(idToken.getPayload().get("family_name"), equalTo("Doe"));
        assertTrue(idToken.getPayload().containsKey("gender"));
        assertThat(idToken.getPayload().get("gender"), equalTo("female"));
        assertTrue(idToken.getPayload().containsKey("birthdate"));
        assertThat(idToken.getPayload().get("birthdate"), equalTo("1994-10-31"));
        assertTrue(idToken.getPayload().containsKey("email"));
        assertThat(idToken.getPayload().get("email"), equalTo("janedoe@example.com"));
        assertTrue(idToken.getPayload().containsKey("picture"));
        assertThat(idToken.getPayload().get("picture"), equalTo("http://example.com/janedoe/me.jpg"));
        assertTrue(idToken.getHeader().containsKey("alg"));
        assertThat(idToken.getHeader().get("alg"), equalTo("HS256"));
    }

    public void testIdTokenWithPrivateClaimsParsing() throws IOException {
        final String serializedJwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwOi8vb3AuZXhhbXBsZS5jb20iLCJzdWIiOiI" +
            "yNDgyODk3NjEwMDEiLCJuYW1lIjoiSmFuZSBEb2UiLCJhdWQiOiJzNkJoZFJrcXQzIiwibm9uY2UiOiJuLTBTNl9XekEyTWoiLCJpYXQiOjE1MTYy" +
            "MzkwMjIsImV4cCI6MTUxNjMzOTAyMiwiZ2l2ZW5fbmFtZSI6IkphbmUiLCJmYW1pbHlfbmFtZSI6IkRvZSIsImdlbmRlciI6ImZlbWFsZSIsImJpc" +
            "nRoZGF0ZSI6IjE5OTQtMTAtMzEiLCJlbWFpbCI6ImphbmVkb2VAZXhhbXBsZS5jb20iLCJwaWN0dXJlIjoiaHR0cDovL2V4YW1wbGUuY29tL2phbm" +
            "Vkb2UvbWUuanBnIiwiY2xhaW0xIjoidmFsdWUxIiwiY2xhaW0yIjoidmFsdWUyIiwiY2xhaW0zIjoidmFsdWUzIiwiY2xhaW00IjoidmFsdWU0Iiw" +
            "iYWRkcmVzcyI6eyJjb3VudHJ5IjoiR3JlZWNlIiwicmVnaW9uIjoiRXZpYSJ9fQ.hvG90pJHvjPkZaf_ll3WMeSvfHIzx82zYs5iuygopQo";
        RPConfiguration rpConfig = new RPConfiguration("clientId", "redirectUri", "code", SignatureAlgorithm.getAllNames(),
            null, Arrays.asList("claim1", "claim2", "claim3", "claim4"));
        IdTokenParser idTokenParser = new IdTokenParser(rpConfig);
        final SecretKeySpec keySpec =
            new SecretKeySpec("144753a689a6508d7c7cd02752d7138e".getBytes(StandardCharsets.UTF_8.name()), "HmacSHA256");
        IdToken idToken = idTokenParser.parseAndValidateIdToken(serializedJwt, keySpec);
        assertThat(idToken.getIssuer(), equalTo("http://op.example.com"));
        assertThat(idToken.getSubject(), equalTo("248289761001"));
        List<String> aud = idToken.getAudiences();
        assertThat(aud.size(), equalTo(1));
        assertTrue(aud.contains("s6BhdRkqt3"));
        assertTrue(idToken.getPayload().containsKey("nonce"));
        assertThat(idToken.getNonce(), equalTo("n-0S6_WzA2Mj"));
        assertThat(idToken.getExpiration(), equalTo(1516339022L));
        assertThat(idToken.getIssuedAt(), equalTo(1516239022L));
        assertTrue(idToken.getPayload().containsKey("name"));
        assertThat(idToken.getPayload().get("name"), equalTo("Jane Doe"));
        assertTrue(idToken.getPayload().containsKey("given_name"));
        assertThat(idToken.getPayload().get("given_name"), equalTo("Jane"));
        assertTrue(idToken.getPayload().containsKey("family_name"));
        assertThat(idToken.getPayload().get("family_name"), equalTo("Doe"));
        assertTrue(idToken.getPayload().containsKey("gender"));
        assertThat(idToken.getPayload().get("gender"), equalTo("female"));
        assertTrue(idToken.getPayload().containsKey("birthdate"));
        assertThat(idToken.getPayload().get("birthdate"), equalTo("1994-10-31"));
        assertTrue(idToken.getPayload().containsKey("email"));
        assertThat(idToken.getPayload().get("email"), equalTo("janedoe@example.com"));
        assertTrue(idToken.getPayload().containsKey("picture"));
        assertThat(idToken.getPayload().get("picture"), equalTo("http://example.com/janedoe/me.jpg"));
        assertTrue(idToken.getPayload().containsKey("claim1"));
        assertThat(idToken.getPayload().get("claim1"), equalTo("value1"));
        assertTrue(idToken.getPayload().containsKey("claim2"));
        assertThat(idToken.getPayload().get("claim2"), equalTo("value2"));
        assertTrue(idToken.getPayload().containsKey("claim3"));
        assertThat(idToken.getPayload().get("claim3"), equalTo("value3"));
        assertTrue(idToken.getPayload().containsKey("claim4"));
        assertThat(idToken.getPayload().get("claim4"), equalTo("value4"));
        assertTrue(idToken.getPayload().containsKey("address"));
        Map<String, Object> expectedAddress = new HashMap<>();
        expectedAddress.put("country", "Greece");
        expectedAddress.put("region", "Evia");
        assertThat(idToken.getPayload().get("address"), equalTo(expectedAddress));
        assertTrue(idToken.getHeader().containsKey("alg"));
        assertThat(idToken.getHeader().get("alg"), equalTo("HS256"));
    }

    public void testIdTokenWithMutipleAudiencesParsing() throws IOException {
        final String serializedJwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwOi8vb3AuZXhhbXBsZS5jb20iLCJzdWIiOiIyNDgyOD" +
            "k3NjEwMDEiLCJuYW1lIjoiSmFuZSBEb2UiLCJhdWQiOlsiczZCaGRSa3F0MyIsIm90aGVyX2F1ZGllbmNlIl0sIm5vbmNlIjoibi0wUzZfV3pBMk1qIiwiaW" +
            "F0IjoxNTE2MjM5MDIyLCJleHAiOjE1MTYzMzkwMjIsImdpdmVuX25hbWUiOiJKYW5lIiwiZmFtaWx5X25hbWUiOiJEb2UiLCJnZW5kZXIiOiJmZW1hbGUiLC" +
            "JiaXJ0aGRhdGUiOiIxOTk0LTEwLTMxIiwiZW1haWwiOiJqYW5lZG9lQGV4YW1wbGUuY29tIiwicGljdHVyZSI6Imh0dHA6Ly9leGFtcGxlLmNvbS9qYW5lZG" +
            "9lL21lLmpwZyIsImNsYWltMSI6InZhbHVlMSIsImNsYWltMiI6InZhbHVlMiIsImNsYWltMyI6InZhbHVlMyIsImNsYWltNCI6InZhbHVlNCIsImFkZHJlc3" +
            "MiOnsiY291bnRyeSI6IkdyZWVjZSIsInJlZ2lvbiI6IkV2aWEifX0.bo2s5D0i87Ij5TSdWnoCmwgM_0dagvscCOqs-luM1yI";
        RPConfiguration rpConfig = new RPConfiguration("clientId", "redirectUri", "code", SignatureAlgorithm.getAllNames(),
            null, Arrays.asList("claim1", "claim2", "claim3", "claim4"));
        IdTokenParser idTokenParser = new IdTokenParser(rpConfig);
        final SecretKeySpec keySpec =
            new SecretKeySpec("144753a689a6508d7c7cd02752d7138e".getBytes(StandardCharsets.UTF_8.name()), "HmacSHA256");
        IdToken idToken = idTokenParser.parseAndValidateIdToken(serializedJwt, keySpec);
        assertThat(idToken.getIssuer(), equalTo("http://op.example.com"));
        assertThat(idToken.getSubject(), equalTo("248289761001"));
        List<String> aud = idToken.getAudiences();
        assertThat(aud.size(), equalTo(2));
        assertTrue(aud.contains("s6BhdRkqt3"));
        assertTrue(aud.contains("other_audience"));
        assertTrue(idToken.getPayload().containsKey("nonce"));
        assertThat(idToken.getNonce(), equalTo("n-0S6_WzA2Mj"));
        assertThat(idToken.getExpiration(), equalTo(1516339022L));
        assertThat(idToken.getIssuedAt(), equalTo(1516239022L));
        assertTrue(idToken.getPayload().containsKey("name"));
        assertThat(idToken.getPayload().get("name"), equalTo("Jane Doe"));
        assertTrue(idToken.getPayload().containsKey("given_name"));
        assertThat(idToken.getPayload().get("given_name"), equalTo("Jane"));
        assertTrue(idToken.getPayload().containsKey("family_name"));
        assertThat(idToken.getPayload().get("family_name"), equalTo("Doe"));
        assertTrue(idToken.getPayload().containsKey("gender"));
        assertThat(idToken.getPayload().get("gender"), equalTo("female"));
        assertTrue(idToken.getPayload().containsKey("birthdate"));
        assertThat(idToken.getPayload().get("birthdate"), equalTo("1994-10-31"));
        assertTrue(idToken.getPayload().containsKey("email"));
        assertThat(idToken.getPayload().get("email"), equalTo("janedoe@example.com"));
        assertTrue(idToken.getPayload().containsKey("picture"));
        assertThat(idToken.getPayload().get("picture"), equalTo("http://example.com/janedoe/me.jpg"));
        assertTrue(idToken.getPayload().containsKey("claim1"));
        assertThat(idToken.getPayload().get("claim1"), equalTo("value1"));
        assertTrue(idToken.getPayload().containsKey("claim2"));
        assertThat(idToken.getPayload().get("claim2"), equalTo("value2"));
        assertTrue(idToken.getPayload().containsKey("claim3"));
        assertThat(idToken.getPayload().get("claim3"), equalTo("value3"));
        assertTrue(idToken.getPayload().containsKey("claim4"));
        assertThat(idToken.getPayload().get("claim4"), equalTo("value4"));
        assertTrue(idToken.getHeader().containsKey("alg"));
        assertThat(idToken.getHeader().get("alg"), equalTo("HS256"));
    }

    public void testHmacSignatureVerification() throws IOException {
        final String serializedJwt = "eyJhbGciOiJIUzM4NCIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwOi8vb3AuZXhhbXBsZS5jb20iLCJzdWIiOiI" +
            "yNDgyODk3NjEwMDEiLCJuYW1lIjoiSmFuZSBEb2UiLCJhdWQiOiJzNkJoZFJrcXQzIiwibm9uY2UiOiJuLTBTNl9XekEyTWoiLCJpYXQiOjE1MTYy" +
            "MzkwMjIsImV4cCI6MTUxNjMzOTAyMiwiZ2l2ZW5fbmFtZSI6IkphbmUiLCJmYW1pbHlfbmFtZSI6IkRvZSIsImdlbmRlciI6ImZlbWFsZSIsImJpc" +
            "nRoZGF0ZSI6IjE5OTQtMTAtMzEiLCJlbWFpbCI6ImphbmVkb2VAZXhhbXBsZS5jb20iLCJwaWN0dXJlIjoiaHR0cDovL2V4YW1wbGUuY29tL2phbm" +
            "Vkb2UvbWUuanBnIn0.BALROUxYSPxhNeETrsn51f6UT7lksaAwCVoBxwj3Yd7L1Dxyzm-Dfhyv0GvJp3Ip";

        RPConfiguration rpConfig = new RPConfiguration("clientId", "redirectUri", "code", SignatureAlgorithm.getAllNames(),
            null, Arrays.asList("claim1", "claim2", "claim3", "claim4"));
        IdTokenParser jwtParser = new IdTokenParser(rpConfig);
        final SecretKeySpec keySpec =
            new SecretKeySpec("144753a689a6508d7c7cd02752d7138e".getBytes(StandardCharsets.UTF_8.name()), "HmacSHA384");
        JsonWebToken jwt = jwtParser.parseAndValidateIdToken(serializedJwt, keySpec);
        assertTrue(jwt.getPayload().containsKey("iss"));
        assertThat(jwt.getPayload().get("iss"), equalTo("http://op.example.com"));
        assertTrue(jwt.getPayload().containsKey("sub"));
        assertThat(jwt.getPayload().get("sub"), equalTo("248289761001"));
        assertTrue(jwt.getHeader().containsKey("alg"));
        assertThat(jwt.getHeader().get("alg"), equalTo("HS384"));

        final String serializedJwt512 = "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwOi8vb3AuZXhhbXBsZS5jb20iLCJzdWIiOiI" +
            "yNDgyODk3NjEwMDEiLCJuYW1lIjoiSmFuZSBEb2UiLCJhdWQiOiJzNkJoZFJrcXQzIiwibm9uY2UiOiJuLTBTNl9XekEyTWoiLCJpYXQiOjE1MTYyMzk" +
            "wMjIsImV4cCI6MTUxNjMzOTAyMiwiZ2l2ZW5fbmFtZSI6IkphbmUiLCJmYW1pbHlfbmFtZSI6IkRvZSIsImdlbmRlciI6ImZlbWFsZSIsImJpcnRoZGF" +
            "0ZSI6IjE5OTQtMTAtMzEiLCJlbWFpbCI6ImphbmVkb2VAZXhhbXBsZS5jb20iLCJwaWN0dXJlIjoiaHR0cDovL2V4YW1wbGUuY29tL2phbmVkb2UvbWU" +
            "uanBnIn0.b-wg-whI_4hzmSn_lVmAfBt2YHjeeX9800jYBsiRLpGJ_WB8sCIIASTUpHiwT8RxqXAgn_nr0JsKTQkhJT6frg";

        final SecretKeySpec keySpec512 =
            new SecretKeySpec("144753a689a6508d7c7cd02752d7138e".getBytes(StandardCharsets.UTF_8.name()), "HmacSHA512");
        JsonWebToken jwt512 = jwtParser.parseAndValidateIdToken(serializedJwt512, keySpec512);
        assertTrue(jwt512.getPayload().containsKey("iss"));
        assertThat(jwt512.getPayload().get("iss"), equalTo("http://op.example.com"));
        assertTrue(jwt512.getPayload().containsKey("sub"));
        assertThat(jwt512.getPayload().get("sub"), equalTo("248289761001"));
        assertTrue(jwt512.getHeader().containsKey("alg"));
        assertThat(jwt512.getHeader().get("alg"), equalTo("HS512"));
    }

    public void testRsaSignatureVerification() throws Exception {
        Path keyPath = PathUtils.get(IdTokenParserTests.class.getResource
            ("/org/elasticsearch/xpack/security/authc/oidc/rsa_public_key.pem").toURI());
        final PublicKey publicKey = PemUtils.readPublicKey(keyPath);
        final String serliazedJwt256 = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwOi8vb3AuZXhhbXBsZS5jb20iLCJzdWIiOiI" +
            "yNDgyODk3NjEwMDEiLCJuYW1lIjoiSmFuZSBEb2UiLCJhdWQiOiJzNkJoZFJrcXQzIiwibm9uY2UiOiJuLTBTNl9XekEyTWoiLCJpYXQiOjE1MTYyMz" +
            "kwMjIsImV4cCI6MTUxNjMzOTAyMiwiZ2l2ZW5fbmFtZSI6IkphbmUiLCJmYW1pbHlfbmFtZSI6IkRvZSIsImdlbmRlciI6ImZlbWFsZSIsImJpcnRoZ" +
            "GF0ZSI6IjE5OTQtMTAtMzEiLCJlbWFpbCI6ImphbmVkb2VAZXhhbXBsZS5jb20iLCJwaWN0dXJlIjoiaHR0cDovL2V4YW1wbGUuY29tL2phbmVkb2Uv" +
            "bWUuanBnIn0.EaczUHQedtRRjeolzutBNQop4CeDz2K-W5sYC7OSLB3dCeUE4DBcP7V-f6ekmpz_QACK-uK9X2qAYrUsHworddBGPy-19TMrA7Lz8so" +
            "ZVvhDy9EeBr4QxtV63Oj8tZVn6ThnZoIvTXZDOwrEs1lcHrYpnubzdSH0pzC1kZQNC8FYwi7BnAG9T-c_mo1qgGRkzGZ-TE7mtJVeQcKepmenm9kzdF" +
            "-fap22rHzW5bWr-DAtyXP14BgqeeXz0ZM3YlOOzIqmRBrpP77mxQXDe8cwxgpR2fk0SIw8hkDyYhb3Y_KnufuT1nV2xhgDh7B3e8aJQNItddI8bp3FA" +
            "tS75HeVFzRdfCscm0Huoci9MBsEP57YENjEW6MMGni0ukhygWTXQmSWEkvFmDMpRXQQjdBIjvW8wZVjwxUSa3Krp4z08GVx2NoZlcFYUT8_3NrRTKnl" +
            "BunJdLWJG0lcFRGHwX4PQoCofO-jZdRPqfeULb8pMP5I6D2Ra0atV_1UFm6awSdngpvS-mP7v3dZALsT13nRdwnMEUfVwTyjOAGiiN01gDalKPVGCsO" +
            "5idqMp1xxt-JIwKgaegghXpqDApKzCeyY5Z672GJHBNhrgugJr-WSGrZIpsm_xhfs4ZlrsjKUqmp8M0AapRtz4x4Z61qTkSXgVctcsoa0xkQppLybwo" +
            "4ASgo";
        RPConfiguration rpConfig = new RPConfiguration("clientId", "redirectUri", "code", SignatureAlgorithm.getAllNames(),
            null, Arrays.asList("claim1", "claim2", "claim3", "claim4"));
        IdTokenParser jwtParser = new IdTokenParser(rpConfig);
        JsonWebToken jwt256 = jwtParser.parseAndValidateIdToken(serliazedJwt256, publicKey);
        assertTrue(jwt256.getPayload().containsKey("iss"));
        assertThat(jwt256.getPayload().get("iss"), equalTo("http://op.example.com"));
        assertTrue(jwt256.getPayload().containsKey("sub"));
        assertThat(jwt256.getPayload().get("sub"), equalTo("248289761001"));
        assertTrue(jwt256.getHeader().containsKey("alg"));
        assertThat(jwt256.getHeader().get("alg"), equalTo("RS256"));

        // RS384
        final String serliazedJwt384 = "eyJhbGciOiJSUzM4NCIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwOi8vb3AuZXhhbXBsZS5jb20iLCJzdWIiOiI" +
            "yNDgyODk3NjEwMDEiLCJuYW1lIjoiSmFuZSBEb2UiLCJhdWQiOiJzNkJoZFJrcXQzIiwibm9uY2UiOiJuLTBTNl9XekEyTWoiLCJpYXQiOjE1MTYyMz" +
            "kwMjIsImV4cCI6MTUxNjMzOTAyMiwiZ2l2ZW5fbmFtZSI6IkphbmUiLCJmYW1pbHlfbmFtZSI6IkRvZSIsImdlbmRlciI6ImZlbWFsZSIsImJpcnRoZ" +
            "GF0ZSI6IjE5OTQtMTAtMzEiLCJlbWFpbCI6ImphbmVkb2VAZXhhbXBsZS5jb20iLCJwaWN0dXJlIjoiaHR0cDovL2V4YW1wbGUuY29tL2phbmVkb2Uv" +
            "bWUuanBnIn0.QDiQQ2LGfeCd4BwMOqWB-cjxWh-Pp4OXj3EM9HCG94OL-vxZxyj76QqYIOpLHzAarfCmGdN4FHkODkI6XmA2yOIWJeElWKXnDqAKqoX" +
            "d6UoAdl9qoPagkDayrQ0y-KaioZMBvzbf6r2nTS5lbnVOcZ5r7HLxu_WWVed8r4GwyQzx9ZHlkHlvv7d5n47f6LQ5ngvHaA7rDQC9SJIYicaDhHYxoq" +
            "WBMdk2J31zpUAjdxFQ1TiqRzOm9-RD2gj2254GzqBhvgB4xShLYaESpZ4neRF-yOvxfoVen1ZjEkRhlGY6baFC8fScOtOGJQoL1wllNDdm-CQ4ZrCZW" +
            "huyMrGzSCZ1Q2zzUiGtaY2H8M47g8xK6q8kEqCIWO_nQa1G8464HLpJDkBF7GTAG36lEb2kuLOElK_p2xmrmAMoyEn9-PVjpN8vhFXG0lIzR3FzjvJD" +
            "h3Arz8djUxIuv4dJjDiS-50hNkpVD6rhKW5a6hjywyvsFenQmT9SLf_iu9m5Es0JNU3zzlbM3H2zHaPmj4ACiXSWwgphzgnpCbGhTjqEb6uBTxUXpku" +
            "zlDo2UW5Pb1j0aoJ0cMPpz9NJgrdiJq1vzvhBHnXQuKTV7TcdumihoZMLIKPWUtLuYbF1rTLxE0c7IZW_qE7hXp4IZDBtp62IpW9mIdNQbYBta5lVE-" +
            "GkYL0";

        JsonWebToken jwt384 = jwtParser.parseAndValidateIdToken(serliazedJwt384, publicKey);
        assertTrue(jwt384.getPayload().containsKey("iss"));
        assertThat(jwt384.getPayload().get("iss"), equalTo("http://op.example.com"));
        assertTrue(jwt384.getPayload().containsKey("sub"));
        assertThat(jwt384.getPayload().get("sub"), equalTo("248289761001"));
        assertTrue(jwt384.getHeader().containsKey("alg"));
        assertThat(jwt384.getHeader().get("alg"), equalTo("RS384"));
        assertThat(jwt256.getPayload(), equalTo(jwt384.getPayload()));

        // RS512
        final String serliazedJwt512 = "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwOi8vb3AuZXhhbXBsZS5jb20iLCJzdWIiOiI" +
            "yNDgyODk3NjEwMDEiLCJuYW1lIjoiSmFuZSBEb2UiLCJhdWQiOiJzNkJoZFJrcXQzIiwibm9uY2UiOiJuLTBTNl9XekEyTWoiLCJpYXQiOjE1MTYyMz" +
            "kwMjIsImV4cCI6MTUxNjMzOTAyMiwiZ2l2ZW5fbmFtZSI6IkphbmUiLCJmYW1pbHlfbmFtZSI6IkRvZSIsImdlbmRlciI6ImZlbWFsZSIsImJpcnRoZ" +
            "GF0ZSI6IjE5OTQtMTAtMzEiLCJlbWFpbCI6ImphbmVkb2VAZXhhbXBsZS5jb20iLCJwaWN0dXJlIjoiaHR0cDovL2V4YW1wbGUuY29tL2phbmVkb2Uv" +
            "bWUuanBnIn0.j1yxslZtV3Cgd3pqrgtA9ysAAAq0-WyPzSmjTUWp3N-wepTW6lV4DuCdTBVYGuYsMmTzts5AdFCEwmDxKX5fno63vt1gwxM1cS9VSxD" +
            "4OhvzIoGoOoQNKPLPNC40hTlh-qOpwTl8WpTAMn_bEzykcIagFEt-MuQJ_0uTAYsW3PdumE8vJKROJrOnoG6085r8VaNfuNzWOyRlZlu_y_xRgOFYG1" +
            "2dJseIMPIuf3BRVM2768fZirJVk_N6N1SAIeZOs3l7nDZ5qiB7wHiH_LURBXO4dZKo0TKpXx8XZfzbNBwk7yC5ftXeXeOPkUEODw2Iy4dO_Pm_-rDX0" +
            "WqZID8f8fs69qc8_uqcBb6zGEN1iGuMe-FttvATTxdtfG912850wvLu-TBUBN_1UUw19k9T7KrKiLeIxUmxTONB9kIet0ga83ByIW0c72SSPBKPITZR" +
            "mMy6ZaIRtW4gKovRfvRzhKhmSHWGrx9MHMabCFm3uQYo39Lai7QOymZqXlUBfsig1yicRwkc3JCQ9IOJXtu1SyLw0g01oU8OPpc9ziTkmCw-SiKB9Aw" +
            "mqhqFxmMTd1o44ItKbTvCPep1Ss66Vku_zv4VprsdalDdy4gcKXglWeviStMa4jIoR6UalPBAT44Lb3zvNhAQlkfH2avhkUZwFKlANuO4PKLhKlJtV7" +
            "SJ59E";

        JsonWebToken jwt512 = jwtParser.parseAndValidateIdToken(serliazedJwt512, publicKey);
        assertTrue(jwt512.getPayload().containsKey("iss"));
        assertThat(jwt512.getPayload().get("iss"), equalTo("http://op.example.com"));
        assertTrue(jwt512.getPayload().containsKey("sub"));
        assertThat(jwt512.getPayload().get("sub"), equalTo("248289761001"));
        assertTrue(jwt512.getHeader().containsKey("alg"));
        assertThat(jwt512.getHeader().get("alg"), equalTo("RS512"));
        assertThat(jwt512.getPayload(), equalTo(jwt384.getPayload()));
        assertThat(jwt512.getPayload(), equalTo(jwt256.getPayload()));
    }

    public void testEcSignatureVerification() throws Exception {
        RPConfiguration rpConfig = new RPConfiguration("clientId", "redirectUri", "code", SignatureAlgorithm.getAllNames(),
            null, Arrays.asList("claim1", "claim2", "claim3", "claim4"));
        IdTokenParser jwtParser = new IdTokenParser(rpConfig);
        Path keyPath256 = PathUtils.get(IdTokenParserTests.class.getResource
            ("/org/elasticsearch/xpack/security/authc/oidc/ec_public_key_256.pem").toURI());
        Path keyPath384 = PathUtils.get(IdTokenParserTests.class.getResource
            ("/org/elasticsearch/xpack/security/authc/oidc/ec_public_key_384.pem").toURI());
        Path keyPath512 = PathUtils.get(IdTokenParserTests.class.getResource
            ("/org/elasticsearch/xpack/security/authc/oidc/ec_public_key_512.pem").toURI());
        final PublicKey publicKey256 = PemUtils.readPublicKey(keyPath256);
        final PublicKey publicKey384 = PemUtils.readPublicKey(keyPath384);
        final PublicKey publicKey512 = PemUtils.readPublicKey(keyPath512);
        // ES256
        final String serliazedJwt256 = "eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIyNDgyODk3NjEwMDEiLCJiaXJ0aGRhdGUiOiIxOTk0LT" +
            "EwLTMxIiwiZ2VuZGVyIjoiZmVtYWxlIiwiaXNzIjoiaHR0cDovL29wLmV4YW1wbGUuY29tIiwiZ2l2ZW5fbmFtZSI6IkphbmUiLCJub25jZSI6Im4tMFM2" +
            "X1d6QTJNaiIsInBpY3R1cmUiOiJodHRwOi8vZXhhbXBsZS5jb20vamFuZWRvZS9tZS5qcGciLCJhdWQiOiJzNkJoZFJrcXQzIiwibmFtZSI6IkphbmUgRG" +
            "9lIiwiZXhwIjoxNTE2MzM5MDIyLCJpYXQiOjE1MTYyMzkwMjIsImZhbWlseV9uYW1lIjoiRG9lIiwiZW1haWwiOiJqYW5lZG9lQGV4YW1wbGUuY29tIn0." +
            "SRRZ4EUe4GG6iZeAkGVuAhhTunfd8xwfd0DYn8SIu3TpiU-jcfrIsYl2Eqv1K3STyRvKUxCFXgb8ziTtA8iqIQ";


        JsonWebToken jwt256 = jwtParser.parseAndValidateIdToken(serliazedJwt256, publicKey256);
        assertTrue(jwt256.getPayload().containsKey("iss"));
        assertThat(jwt256.getPayload().get("iss"), equalTo("http://op.example.com"));
        assertTrue(jwt256.getPayload().containsKey("sub"));
        assertThat(jwt256.getPayload().get("sub"), equalTo("248289761001"));
        assertTrue(jwt256.getHeader().containsKey("alg"));
        assertThat(jwt256.getHeader().get("alg"), equalTo("ES256"));
        // ES384
        final String serliazedJwt384 = "eyJhbGciOiJFUzM4NCIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIyNDgyODk3NjEwMDEiLCJiaXJ0aGRhdGUiOiIxOTk0LT" +
            "EwLTMxIiwiZ2VuZGVyIjoiZmVtYWxlIiwiaXNzIjoiaHR0cDovL29wLmV4YW1wbGUuY29tIiwiZ2l2ZW5fbmFtZSI6IkphbmUiLCJub25jZSI6Im4tMFM2" +
            "X1d6QTJNaiIsInBpY3R1cmUiOiJodHRwOi8vZXhhbXBsZS5jb20vamFuZWRvZS9tZS5qcGciLCJhdWQiOiJzNkJoZFJrcXQzIiwibmFtZSI6IkphbmUgRG" +
            "9lIiwiZXhwIjoxNTE2MzM5MDIyLCJpYXQiOjE1MTYyMzkwMjIsImZhbWlseV9uYW1lIjoiRG9lIiwiZW1haWwiOiJqYW5lZG9lQGV4YW1wbGUuY29tIn0." +
            "vyRAaD8ThjK9EvFt4_Lwqe2So_ZmEA4BeZiLoTSVCxlgYLNHFI6Ip01IB-5oN1pCPsId-SXZyFq-YMcgP_bIyLGCPUc5faU8XpHNhtloj4WBR_k1ZSH23g" +
            "gk6hps2JjD";

        JsonWebToken jwt384 = jwtParser.parseAndValidateIdToken(serliazedJwt384, publicKey384);
        assertTrue(jwt384.getPayload().containsKey("iss"));
        assertThat(jwt384.getPayload().get("iss"), equalTo("http://op.example.com"));
        assertTrue(jwt384.getPayload().containsKey("sub"));
        assertThat(jwt384.getPayload().get("sub"), equalTo("248289761001"));
        assertTrue(jwt384.getHeader().containsKey("alg"));
        assertThat(jwt384.getHeader().get("alg"), equalTo("ES384"));
        // ES512
        final String serliazedJwt512 = "eyJhbGciOiJFUzUxMiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIyNDgyODk3NjEwMDEiLCJiaXJ0aGRhdGUiOiIxOTk0LTEwLT" +
            "MxIiwiZ2VuZGVyIjoiZmVtYWxlIiwiaXNzIjoiaHR0cDovL29wLmV4YW1wbGUuY29tIiwiZ2l2ZW5fbmFtZSI6IkphbmUiLCJub25jZSI6Im4tMFM2X1d6QTJN" +
            "aiIsInBpY3R1cmUiOiJodHRwOi8vZXhhbXBsZS5jb20vamFuZWRvZS9tZS5qcGciLCJhdWQiOiJzNkJoZFJrcXQzIiwibmFtZSI6IkphbmUgRG9lIiwiZXhwIj" +
            "oxNTE2MzM5MDIyLCJpYXQiOjE1MTYyMzkwMjIsImZhbWlseV9uYW1lIjoiRG9lIiwiZW1haWwiOiJqYW5lZG9lQGV4YW1wbGUuY29tIn0.AU_7xDKGYupo9nt9" +
            "-RrD7RSurkVX-ntyHycjH1SLKCTT8eWLfXHnklEhqCUTPwAmG2iKolsRN6C07fctAcsYeGkSAFw9QhW0rvPNXClAB4wIZiNU1CI2l0I0vpY43L0o6Eaucx-s42" +
            "avqYalxOHimkxxzI1LlhDjz8XebkXWDtbN-AB2";

        JsonWebToken jwt512 = jwtParser.parseAndValidateIdToken(serliazedJwt512, publicKey512);
        assertTrue(jwt512.getPayload().containsKey("iss"));
        assertThat(jwt512.getPayload().get("iss"), equalTo("http://op.example.com"));
        assertTrue(jwt512.getPayload().containsKey("sub"));
        assertThat(jwt512.getPayload().get("sub"), equalTo("248289761001"));
        assertTrue(jwt512.getHeader().containsKey("alg"));
        assertThat(jwt512.getHeader().get("alg"), equalTo("ES512"));
    }

    public void testNotAllowedSignatureAlgorithm() throws Exception {
        RPConfiguration rpConfig = new RPConfiguration("clientId", "redirectUri", "code", Collections.singletonList("HS512"),
            null, Arrays.asList("claim1", "claim2", "claim3", "claim4"));
        IdTokenParser jwtParser = new IdTokenParser(rpConfig);
        final String serliazedJwt384 = "eyJhbGciOiJFUzM4NCIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIyNDgyODk3NjEwMDEiLCJiaXJ0aGRhdGUiOiIxOTk0LT" +
            "EwLTMxIiwiZ2VuZGVyIjoiZmVtYWxlIiwiaXNzIjoiaHR0cDovL29wLmV4YW1wbGUuY29tIiwiZ2l2ZW5fbmFtZSI6IkphbmUiLCJub25jZSI6Im4tMFM2" +
            "X1d6QTJNaiIsInBpY3R1cmUiOiJodHRwOi8vZXhhbXBsZS5jb20vamFuZWRvZS9tZS5qcGciLCJhdWQiOiJzNkJoZFJrcXQzIiwibmFtZSI6IkphbmUgRG" +
            "9lIiwiZXhwIjoxNTE2MzM5MDIyLCJpYXQiOjE1MTYyMzkwMjIsImZhbWlseV9uYW1lIjoiRG9lIiwiZW1haWwiOiJqYW5lZG9lQGV4YW1wbGUuY29tIn0." +
            "vyRAaD8ThjK9EvFt4_Lwqe2So_ZmEA4BeZiLoTSVCxlgYLNHFI6Ip01IB-5oN1pCPsId-SXZyFq-YMcgP_bIyLGCPUc5faU8XpHNhtloj4WBR_k1ZSH23g" +
            "gk6hps2JjD";
        Path keyPath384 = PathUtils.get(IdTokenParserTests.class.getResource
            ("/org/elasticsearch/xpack/security/authc/oidc/ec_public_key_384.pem").toURI());
        final PublicKey publicKey384 = PemUtils.readPublicKey(keyPath384);
        Exception e = expectThrows(IllegalStateException.class, () -> jwtParser.parseAndValidateIdToken(serliazedJwt384, publicKey384));
        assertThat(e.getMessage(), containsString("ID Token is signed with an unsupported algorithm"));
    }

    public void testNoneAlgorithmNotAllowed() throws Exception {
        IdToken idToken = new IdToken(new JsonWebTokenBuilder()
            .algorithm("NONE")
            .type("JWT")
            .issuer("issuer")
            .audience("audience")
            .expirationTime(1516339022L)
            .issuedAt(1516239022L)
            .build());
        RPConfiguration rpConfig = new RPConfiguration("clientId", "redirectUri", "code", SignatureAlgorithm.getAllNames(), null, null);
        IdTokenParser idTokenParser = new IdTokenParser(rpConfig);
        final SecretKeySpec keySpec =
            new SecretKeySpec("144753a689a6508d7c7cd02752d7138e".getBytes(StandardCharsets.UTF_8.name()), "HmacSHA256");
        IllegalStateException e = expectThrows(IllegalStateException.class,
            () -> idTokenParser.parseAndValidateIdToken(idToken.encode(), keySpec));
        assertThat(e.getMessage(), containsString("ID Token is not signed or the signing algorithm is unsupported"));
    }
}
