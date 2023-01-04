/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jwt.SignedJWT;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class JwtValidateUtilTests extends JwtTestCase {

    private static final Logger LOGGER = LogManager.getLogger(JwtValidateUtilTests.class);

    private boolean helpTestSignatureAlgorithm(final String signatureAlgorithm, final boolean requireOidcSafe) throws Exception {
        LOGGER.trace("Testing signature algorithm " + signatureAlgorithm);
        final JWK jwk = JwtTestCase.randomJwk(signatureAlgorithm, requireOidcSafe);
        final SecureString serializedJWTOriginal = JwtTestCase.randomBespokeJwt(jwk, signatureAlgorithm);
        final SignedJWT parsedSignedJWT = SignedJWT.parse(serializedJWTOriginal.toString());
        return JwtValidateUtil.verifyJwt(jwk, parsedSignedJWT);
    }

    public void testJwtSignVerifyPassedForAllSupportedAlgorithms() throws Exception {
        // Pass: "ES256", "ES384", "ES512", RS256", "RS384", "RS512", "PS256", "PS384", "PS512, "HS256", "HS384", "HS512"
        for (final String signatureAlgorithm : JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS) {
            assertThat(this.helpTestSignatureAlgorithm(signatureAlgorithm, false), is(true));
        }
        // Fail: "ES256K"
        final Exception exp1 = expectThrows(
            JOSEException.class,
            () -> this.helpTestSignatureAlgorithm(JWSAlgorithm.ES256K.getName(), false)
        );
        final String msg1 = "Unsupported signature algorithm ["
            + JWSAlgorithm.ES256K
            + "]. Supported signature algorithms are "
            + JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS
            + ".";
        assertThat(exp1.getMessage(), is(equalTo(msg1)));
    }
}
