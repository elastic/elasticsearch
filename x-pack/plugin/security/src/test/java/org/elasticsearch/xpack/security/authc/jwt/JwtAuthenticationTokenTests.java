/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

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
        final JWK jwk = JwtTestCase.randomJwk(signatureAlgorithm);

        final SecureString jwt = JwtTestCase.randomJwt(jwk, signatureAlgorithm);
        final SecureString clientSharedSecret = randomBoolean() ? null : new SecureString(randomAlphaOfLengthBetween(10, 20).toCharArray());

        final JwtAuthenticationToken jwtAuthenticationToken = new JwtAuthenticationToken(jwt, clientSharedSecret);
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
}
