/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import org.elasticsearch.common.settings.SecureString;
import org.junit.Assert;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class JwtAuthenticationTokenTests extends JwtTestCase {

    public void testJwtAuthenticationTokenParse() throws Exception {
        final String serializedJWTOriginal = super.generateValidSignedJWT().serialize();
        final SecureString jwt = new SecureString(serializedJWTOriginal.toCharArray());
        final SecureString clientSharedSecret = randomBoolean() ? null : new SecureString(randomAlphaOfLengthBetween(10, 20).toCharArray());

        final JwtAuthenticationToken jwtAuthenticationToken = new JwtAuthenticationToken(jwt, clientSharedSecret);
        Assert.assertEquals(serializedJWTOriginal, jwtAuthenticationToken.getSerializedJwt().toString());
        Assert.assertEquals(serializedJWTOriginal, jwtAuthenticationToken.getSignedJwt().serialize());
        Assert.assertEquals(clientSharedSecret, jwtAuthenticationToken.getClientAuthorizationSharedSecret());

        jwtAuthenticationToken.clearCredentials();

        final Exception exception = expectThrows(
            IllegalStateException.class,
            () -> { jwtAuthenticationToken.getSerializedJwt().length(); }
        );
        assertThat(exception.getMessage(), equalTo("SecureString has already been closed"));

        assertThat(jwtAuthenticationToken.getSignedJwt(), is(nullValue()));

        if (clientSharedSecret != null) {
            final Exception exception2 = expectThrows(
                IllegalStateException.class,
                () -> { jwtAuthenticationToken.getClientAuthorizationSharedSecret().length(); }
            );
            assertThat(exception2.getMessage(), equalTo("SecureString has already been closed"));
        }
    }
}
