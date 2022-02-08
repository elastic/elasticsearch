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
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jwt.SignedJWT;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class JwtValidateUtilTests extends JwtTestCase {

    private static final Logger LOGGER = LogManager.getLogger(JwtValidateUtilTests.class);

    private boolean helpTestSignatureAlgorithm(final String signatureAlgorithm) throws Exception {
        LOGGER.info("Testing signature algorithm " + signatureAlgorithm);
        // randomSecretOrSecretKeyOrKeyPair() randomizes which JwtUtil methods to call, so it indirectly covers most JwtUtil code
        final JWK jwk = JwtTestCase.randomJwk(signatureAlgorithm);
        final JWSSigner jwsSigner = JwtValidateUtil.createJwsSigner(jwk);
        final JWSVerifier jwkVerifier = JwtValidateUtil.createJwsVerifier(jwk);
        final String serializedJWTOriginal = JwtTestCase.randomValidSignedJWT(jwsSigner, signatureAlgorithm).serialize();
        final SignedJWT parsedSignedJWT = SignedJWT.parse(serializedJWTOriginal);
        return JwtValidateUtil.verifyJWT(jwkVerifier, parsedSignedJWT);
    }

    public void testJwtSignVerifyPassedForAllSupportedAlgorithms() throws Exception {
        // Pass: "ES256", "ES384", "ES512", RS256", "RS384", "RS512", "PS256", "PS384", "PS512, "HS256", "HS384", "HS512"
        for (final String signatureAlgorithm : JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS) {
            assertThat(this.helpTestSignatureAlgorithm(signatureAlgorithm), is(true));
        }
        // Fail: "ES256K"
        final Exception exp1 = expectThrows(JOSEException.class, () -> this.helpTestSignatureAlgorithm(JWSAlgorithm.ES256K.getName()));
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

    public void testClientAuthenticationTypeValidation() {
        final String clientAuthenticationTypeKey = JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE.getKey();
        final String clientAuthenticationSharedSecretKey = JwtRealmSettings.CLIENT_AUTHENTICATION_SHARED_SECRET.getKey();
        final SecureString sharedSecretNonEmpty = new SecureString(randomAlphaOfLengthBetween(1, 32).toCharArray());
        final SecureString sharedSecretNullOrEmpty = randomBoolean() ? new SecureString("".toCharArray()) : null;

        // If type is None, verify null or empty is accepted
        JwtUtil.validateClientAuthenticationSettings(
            clientAuthenticationTypeKey,
            JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE_NONE,
            clientAuthenticationSharedSecretKey,
            sharedSecretNullOrEmpty
        );
        // If type is None, verify non-empty is rejected
        final Exception exception1 = expectThrows(
            SettingsException.class,
            () -> JwtUtil.validateClientAuthenticationSettings(
                clientAuthenticationTypeKey,
                JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE_NONE,
                clientAuthenticationSharedSecretKey,
                sharedSecretNonEmpty
            )
        );
        assertThat(
            exception1.getMessage(),
            is(
                equalTo(
                    "Setting ["
                        + clientAuthenticationSharedSecretKey
                        + "] is not supported, because setting ["
                        + clientAuthenticationTypeKey
                        + "] is ["
                        + JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE_NONE
                        + "]"
                )
            )
        );

        // If type is SharedSecret, verify non-empty is accepted
        JwtUtil.validateClientAuthenticationSettings(
            clientAuthenticationTypeKey,
            JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE_SHARED_SECRET,
            clientAuthenticationSharedSecretKey,
            sharedSecretNonEmpty
        );
        // If type is SharedSecret, verify null or empty is rejected
        final Exception exception2 = expectThrows(
            SettingsException.class,
            () -> JwtUtil.validateClientAuthenticationSettings(
                clientAuthenticationTypeKey,
                JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE_SHARED_SECRET,
                clientAuthenticationSharedSecretKey,
                sharedSecretNullOrEmpty
            )
        );
        assertThat(
            exception2.getMessage(),
            is(
                equalTo(
                    "Missing setting for ["
                        + clientAuthenticationSharedSecretKey
                        + "]. It is required when setting ["
                        + clientAuthenticationTypeKey
                        + "] is ["
                        + JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE_SHARED_SECRET
                        + "]"
                )
            )
        );
    }

    public void testValidateAlgsJwksHmac() throws Exception {
        final List<String> algs = randomOf(randomIntBetween(1, 3), JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_HMAC);
        final Map<String, List<JWK>> algsToJwks = JwtTestCase.randomJwks(algs);
        final List<JWK> jwks = algsToJwks.values().stream().flatMap(List::stream).toList();
        // If HMAC JWKSet and algorithms are present, verify they are accepted
        final Tuple<List<String>, List<JWK>> filtered = JwkValidateUtil.filterJwksAndAlgorithms(jwks, algs);
        assertThat(algs.size(), equalTo(filtered.v1().size()));
        assertThat(jwks.size(), equalTo(filtered.v2().size()));
    }

    public void testValidateAlgsJwksPkc() throws Exception {
        final List<String> algs = randomOf(randomIntBetween(1, 3), JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_PKC);
        final Map<String, List<JWK>> algsToJwks = JwtTestCase.randomJwks(algs);
        final List<JWK> jwks = algsToJwks.values().stream().flatMap(List::stream).toList();
        // If RSA/EC JWKSet and algorithms are present, verify they are accepted
        final Tuple<List<String>, List<JWK>> filtered = JwkValidateUtil.filterJwksAndAlgorithms(jwks, algs);
        assertThat(algs.size(), equalTo(filtered.v1().size()));
        assertThat(jwks.size(), equalTo(filtered.v2().size()));
    }
}
