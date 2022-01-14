/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jwt.SignedJWT;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.Tuple;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class JwtUtilTests extends JwtTestCase {

    private static final Logger LOGGER = LogManager.getLogger(JwtUtilTests.class);

    public void testCheckIfJavaDisabledES256K1() throws Exception {
        if (inFipsJvm()) {
            // Expect ES256K to succeed because it is not deprecated in BC-FIPS yet
            assertThat(this.helpTestSignatureAlgorithm(JWSAlgorithm.ES256K), is(true));
        } else {
            // Expect ES256K to fail because it is deprecated and disabled by default in Java 11.0.9/11.0.10 and 17
            final Exception exception = expectThrows(JOSEException.class, () -> this.helpTestSignatureAlgorithm(JWSAlgorithm.ES256K));
            assertThat(exception.getMessage(), is(equalTo("Curve not supported: secp256k1 (1.3.132.0.10)")));
        }
    }

    // All JWSAlgorithm values in Family.HMAC_SHA, Family.RSA, and Family.EC (except ES256K, handled separately)
    public void testSignedJwtGenerateSignVerify() throws Exception {
        for (final JWSAlgorithm signatureAlgorithm : JwtUtil.SUPPORTED_JWS_ALGORITHMS) {
            assertThat(this.helpTestSignatureAlgorithm(signatureAlgorithm), is(true));
        }
    }

    private boolean helpTestSignatureAlgorithm(final JWSAlgorithm signatureAlgorithm) throws Exception {
        LOGGER.info("Testing signature algorithm " + signatureAlgorithm);
        // randomSecretOrSecretKeyOrKeyPair() randomizes which JwtUtil methods to call, so it indirectly covers most JwtUtil code
        final Object secretOrSecretKeyOrKeyPair = JwtUtilTests.randomSecretOrSecretKeyOrKeyPair(signatureAlgorithm.toString());
        final Tuple<JWSSigner, JWSVerifier> jwsSignerAndVerifier = JwtUtil.createJwsSignerJwsVerifier(secretOrSecretKeyOrKeyPair);
        final String serializedJWTOriginal = JwtUtilTests.randomValidSignedJWT(jwsSignerAndVerifier.v1(), signatureAlgorithm.toString())
            .serialize();
        final SignedJWT parsedSignedJWT = SignedJWT.parse(serializedJWTOriginal);
        return JwtUtil.verifySignedJWT(jwsSignerAndVerifier.v2(), parsedSignedJWT);
    }
}
