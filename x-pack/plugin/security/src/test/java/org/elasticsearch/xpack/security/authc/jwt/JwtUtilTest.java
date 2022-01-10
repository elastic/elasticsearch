/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jwt.SignedJWT;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.Tuple;

public class JwtUtilTest extends JwtTestCase {

    private static final Logger LOGGER = LogManager.getLogger(JwtUtilTest.class);

    public void testSignedJwtGenerateSignVerify() throws Exception {
        // All JWSAlgorithm values in Family.HMAC_SHA, Family.RSA, and Family.EC (except ES256K)
        for (final JWSAlgorithm signatureAlgorithm : JwtUtil.SUPPORTED_JWS_ALGORITHMS) {
            LOGGER.info("Testing signature algorithm " + signatureAlgorithm);
            final Object secretKeyOrKeyPair = JwtUtil.generateSecretKeyOrKeyPair(signatureAlgorithm.toString());
            final Tuple<JWSSigner, JWSVerifier> jwsSignerAndVerifier = JwtUtil.createJwsSignerJWSVerifier(secretKeyOrKeyPair);
            final String serializedJWTOriginal = JwtUtil.generateValidSignedJWT(jwsSignerAndVerifier.v1(), signatureAlgorithm.toString())
                .serialize();
            final SignedJWT parsedSignedJWT = SignedJWT.parse(serializedJWTOriginal);
            JwtUtil.verifySignedJWT(jwsSignerAndVerifier.v2(), parsedSignedJWT);
        }

    }
}
