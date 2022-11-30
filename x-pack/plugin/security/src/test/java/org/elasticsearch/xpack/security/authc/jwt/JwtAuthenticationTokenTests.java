/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jwt.SignedJWT;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;
import org.junit.Assert;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class JwtAuthenticationTokenTests extends JwtTestCase {

    public void testJwtAuthenticationTokenParse() throws Exception {
        final String signatureAlgorithm = randomFrom(JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS);
        final JWK jwk = JwtTestCase.randomJwk(signatureAlgorithm, randomBoolean());

        final SecureString jwt = JwtTestCase.randomBespokeJwt(jwk, signatureAlgorithm); // bespoke JWT, not tied to any JWT realm
        final SecureString clientSharedSecret = randomBoolean() ? null : new SecureString(randomAlphaOfLengthBetween(10, 20).toCharArray());

        final List<String> principalClaimNames = List.of(randomAlphaOfLength(4), "sub", randomAlphaOfLength(4));
        final JwtAuthenticationToken jwtAuthenticationToken = new JwtAuthenticationToken(principalClaimNames, jwt, clientSharedSecret);
        final SecureString endUserSignedJwt = jwtAuthenticationToken.getEndUserSignedJwt();
        final SecureString clientAuthenticationSharedSecret = jwtAuthenticationToken.getClientAuthenticationSharedSecret();

        Assert.assertEquals(jwt, endUserSignedJwt);
        Assert.assertEquals(clientSharedSecret, clientAuthenticationSharedSecret);

        jwtAuthenticationToken.clearCredentials();

        // verify references to SecureString throw exception when calling their methods
        final Exception exception1 = expectThrows(IllegalStateException.class, endUserSignedJwt::length);
        assertThat(exception1.getMessage(), equalTo("SecureString has already been closed"));
        if (clientAuthenticationSharedSecret != null) {
            final Exception exception2 = expectThrows(IllegalStateException.class, clientAuthenticationSharedSecret::length);
            assertThat(exception2.getMessage(), equalTo("SecureString has already been closed"));
        }

        // verify token returns nulls
        assertThat(jwtAuthenticationToken.principal(), is(nullValue()));
        assertThat(jwtAuthenticationToken.credentials(), is(nullValue()));
        assertThat(jwtAuthenticationToken.getEndUserSignedJwt(), is(nullValue()));
        assertThat(jwtAuthenticationToken.getClientAuthenticationSharedSecret(), is(nullValue()));
    }

    public void testPrincipalForJwtWithoutSub() throws Exception {
        final String issuer = randomAlphaOfLengthBetween(8, 24);
        final String audience = randomAlphaOfLengthBetween(6, 12);

        final String principalClaimName = randomValueOtherThan("sub", () -> randomAlphaOfLength(3));
        final String principalClaimValue = randomAlphaOfLengthBetween(8, 32);

        final String signatureAlgorithm = randomFrom(JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS);
        final JWK jwk = JwtTestCase.randomJwk(signatureAlgorithm, randomBoolean());

        final Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        final SignedJWT unsignedJwt = JwtTestCase.buildUnsignedJwt(
            randomBoolean() ? null : JOSEObjectType.JWT.toString(), // kty
            randomBoolean() ? null : jwk.getKeyID(), // kid
            signatureAlgorithm, // alg
            null, // jwtID
            issuer, // iss
            List.of(audience), // aud
            null, // sub claim value
            principalClaimName, // principal claim name
            principalClaimValue, // principal claim value
            null, // groups claim
            List.of(), // groups
            Date.from(now.minusSeconds(60 * randomLongBetween(10, 20))), // auth_time
            Date.from(now.minusSeconds(randomBoolean() ? 0 : 60 * randomLongBetween(5, 10))), // iat
            Date.from(now), // nbf
            Date.from(now.plusSeconds(60 * randomLongBetween(3600, 7200))), // exp
            null, // nonce
            Map.of() // other claims
        );
        final SecureString jwt = JwtValidateUtil.signJwt(jwk, unsignedJwt);

        final List<String> principalClaimNames = List.of(randomAlphaOfLength(4), "sub", principalClaimName);
        final JwtAuthenticationToken jwtAuthenticationToken = new JwtAuthenticationToken(principalClaimNames, jwt, null);
        Assert.assertEquals(issuer + "/" + audience + "/" + principalClaimValue, jwtAuthenticationToken.principal());
    }
}
