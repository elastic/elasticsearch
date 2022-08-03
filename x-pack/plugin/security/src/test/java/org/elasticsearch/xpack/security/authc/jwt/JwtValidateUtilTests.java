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

import java.time.Instant;
import java.util.Date;

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

    public void testValidationOfJwtTimeValues() throws Exception {
        final int skewSeconds = 1;
        final Instant instant = Instant.now();
        final Date now = Date.from(instant);
        final Date before = Date.from(instant.minusSeconds(skewSeconds));
        final Date after = Date.from(instant.plusSeconds(skewSeconds));

        // auth_time parameter checks
        JwtValidateUtil.validateAuthTime((Date) null, now, 0);
        expectThrows(Exception.class, () -> JwtValidateUtil.validateAuthTime(before, null, 0));
        expectThrows(Exception.class, () -> JwtValidateUtil.validateAuthTime(before, now, -1));
        // iat parameter checks
        expectThrows(Exception.class, () -> JwtValidateUtil.validateIssuedAtTime((Date) null, now, 0));
        expectThrows(Exception.class, () -> JwtValidateUtil.validateIssuedAtTime(before, null, 0));
        expectThrows(Exception.class, () -> JwtValidateUtil.validateIssuedAtTime(before, now, -1));
        // nbf parameter checks
        JwtValidateUtil.validateNotBeforeTime((Date) null, now, 0);
        expectThrows(Exception.class, () -> JwtValidateUtil.validateNotBeforeTime(before, null, 0));
        expectThrows(Exception.class, () -> JwtValidateUtil.validateNotBeforeTime(before, now, -1));
        // exp parameter checks
        expectThrows(Exception.class, () -> JwtValidateUtil.validateExpiredTime((Date) null, now, 0));
        expectThrows(Exception.class, () -> JwtValidateUtil.validateExpiredTime(after, null, 0));
        expectThrows(Exception.class, () -> JwtValidateUtil.validateExpiredTime(after, now, -1));

        // validate auth_time
        JwtValidateUtil.validateAuthTime(before, now, 0);
        JwtValidateUtil.validateAuthTime(now, now, 0);
        JwtValidateUtil.validateAuthTime(after, now, skewSeconds);
        expectThrows(Exception.class, () -> JwtValidateUtil.validateAuthTime(after, now, 0));
        // validate iat
        JwtValidateUtil.validateIssuedAtTime(before, now, 0);
        JwtValidateUtil.validateIssuedAtTime(now, now, 0);
        JwtValidateUtil.validateIssuedAtTime(after, now, skewSeconds);
        expectThrows(Exception.class, () -> JwtValidateUtil.validateIssuedAtTime(after, now, 0));
        // validate nbf
        JwtValidateUtil.validateNotBeforeTime(before, now, 0);
        JwtValidateUtil.validateNotBeforeTime(now, now, 0);
        JwtValidateUtil.validateNotBeforeTime(after, now, skewSeconds);
        expectThrows(Exception.class, () -> JwtValidateUtil.validateNotBeforeTime(after, now, 0));
        // validate exp
        expectThrows(Exception.class, () -> JwtValidateUtil.validateExpiredTime(before, now, 0));
        expectThrows(Exception.class, () -> JwtValidateUtil.validateExpiredTime(now, now, 0));
        JwtValidateUtil.validateExpiredTime(now, now, skewSeconds);
        JwtValidateUtil.validateExpiredTime(after, now, 0);
    }
}
