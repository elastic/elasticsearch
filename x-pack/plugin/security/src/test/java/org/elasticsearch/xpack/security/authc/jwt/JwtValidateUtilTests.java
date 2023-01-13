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

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class JwtValidateUtilTests extends JwtTestCase {

    private void helpTestSignatureAlgorithm(final String signatureAlgorithm, final boolean requireOidcSafe) throws Exception {
        logger.trace("Testing signature algorithm " + signatureAlgorithm);
        final JWK jwk = JwtTestCase.randomJwk(signatureAlgorithm, requireOidcSafe);
        final SecureString serializedJWTOriginal = JwtTestCase.randomBespokeJwt(jwk, signatureAlgorithm);
        final SignedJWT parsedSignedJWT = SignedJWT.parse(serializedJWTOriginal.toString());
        JwtValidateUtil.validateSignature(parsedSignedJWT, List.of(jwk));
    }

    public void testJwtSignVerifyPassedForAllSupportedAlgorithms() throws Exception {
        // Pass: "ES256", "ES384", "ES512", RS256", "RS384", "RS512", "PS256", "PS384", "PS512, "HS256", "HS384", "HS512"
        for (final String signatureAlgorithm : JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS) {
            try {
                helpTestSignatureAlgorithm(signatureAlgorithm, false);
            } catch (Exception e) {
                fail("signature validation with algorithm [" + signatureAlgorithm + "] should have succeeded");
            }
        }
        // Fail: "ES256K"
        final Exception exp1 = expectThrows(JOSEException.class, () -> helpTestSignatureAlgorithm(JWSAlgorithm.ES256K.getName(), false));
        final String msg1 = "Unsupported signature algorithm ["
            + JWSAlgorithm.ES256K
            + "]. Supported signature algorithms are "
            + JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS
            + ".";
        assertThat(exp1.getMessage(), equalTo(msg1));
    }
}
