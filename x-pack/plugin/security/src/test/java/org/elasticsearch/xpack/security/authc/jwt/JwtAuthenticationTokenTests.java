/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jwt.SignedJWT;

import org.elasticsearch.common.settings.SecureString;
import org.junit.Assert;

public class JwtAuthenticationTokenTests extends JwtTestCase {

    public void testJwtAuthenticationTokenParse() throws Exception {
        final SignedJWT signedJWT = super.generateValidSignedJWT();
        final SecureString jwt = new SecureString(signedJWT.serialize().toCharArray());
        final SecureString clientSharedSecret = randomFrom(null, new SecureString(randomAlphaOfLengthBetween(10, 20).toCharArray()));

        final JwtAuthenticationToken jwtAuthenticationToken = new JwtAuthenticationToken(jwt, signedJWT, clientSharedSecret);
        jwtAuthenticationToken.clearCredentials();

        Assert.assertEquals(jwt, jwtAuthenticationToken.getJwt());
        Assert.assertEquals(clientSharedSecret, jwtAuthenticationToken.getClientAuthorizationSharedSecret());

        Assert.assertTrue(jwtAuthenticationToken.getJwt().chars().allMatch(c -> c == '\0'));
        Assert.assertTrue(jwtAuthenticationToken.getClientAuthorizationSharedSecret().chars().allMatch(c -> c == '\0'));
        Assert.assertTrue(jwt.chars().allMatch(c -> c == '\0'));
        Assert.assertTrue((clientSharedSecret == null) || clientSharedSecret.chars().allMatch(c -> c == '\0'));
    }
}
