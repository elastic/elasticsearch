/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.jwk.JWK;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;
import org.junit.Assert;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class JwtAuthenticationTokenTests extends JwtTestCase {

    public void testJwtAuthenticationTokenParse() throws Exception {
        final String signatureAlgorithm = randomFrom(JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS);
        final JWK jwk = JwtTestCase.randomJwk(JWSAlgorithm.parse(signatureAlgorithm));
        final JWSSigner jwsSigner = JwtUtil.createJwsSigner(jwk);
        final String serializedJWTOriginal = JwtTestCase.randomValidSignedJWT(jwsSigner, signatureAlgorithm).serialize();

        final SecureString jwt = new SecureString(serializedJWTOriginal.toCharArray());
        final SecureString clientSharedSecret = randomBoolean() ? null : new SecureString(randomAlphaOfLengthBetween(10, 20).toCharArray());

        final JwtAuthenticationToken jwtAuthenticationToken = new JwtAuthenticationToken(jwt, clientSharedSecret);
        final SecureString endUserSignedJwt = jwtAuthenticationToken.getEndUserSignedJwt();
        final SecureString clientAuthorizationSharedSecret = jwtAuthenticationToken.getClientAuthorizationSharedSecret();

        Assert.assertEquals(serializedJWTOriginal, endUserSignedJwt.toString());
        Assert.assertEquals(serializedJWTOriginal, jwtAuthenticationToken.getSignedJwt().serialize());
        Assert.assertEquals(clientSharedSecret, clientAuthorizationSharedSecret);

        jwtAuthenticationToken.clearCredentials();

        // verify references to SecureString throw exception when calling their methods
        final Exception exception1 = expectThrows(IllegalStateException.class, endUserSignedJwt::length);
        assertThat(exception1.getMessage(), equalTo("SecureString has already been closed"));
        if (clientAuthorizationSharedSecret != null) {
            final Exception exception2 = expectThrows(IllegalStateException.class, clientAuthorizationSharedSecret::length);
            assertThat(exception2.getMessage(), equalTo("SecureString has already been closed"));
        }

        // verify token returns nulls
        assertThat(jwtAuthenticationToken.principal(), is(nullValue()));
        assertThat(jwtAuthenticationToken.credentials(), is(nullValue()));
        assertThat(jwtAuthenticationToken.getEndUserSignedJwt(), is(nullValue()));
        assertThat(jwtAuthenticationToken.getClientAuthorizationSharedSecret(), is(nullValue()));
        assertThat(jwtAuthenticationToken.getSignedJwt(), is(nullValue()));
        assertThat(jwtAuthenticationToken.getJwsHeader(), is(nullValue()));
        assertThat(jwtAuthenticationToken.getJwtClaimsSet(), is(nullValue()));
        assertThat(jwtAuthenticationToken.getJwtSignature(), is(nullValue()));
        assertThat(jwtAuthenticationToken.getIssuerClaim(), is(nullValue()));
        assertThat(jwtAuthenticationToken.getAudiencesClaim(), is(nullValue()));
        assertThat(jwtAuthenticationToken.getSubjectClaim(), is(nullValue()));
    }
}
