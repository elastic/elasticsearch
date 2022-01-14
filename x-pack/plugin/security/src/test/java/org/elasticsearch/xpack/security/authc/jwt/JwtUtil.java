/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.nimbusds.jose.crypto.ECDSAVerifier;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jose.crypto.MACVerifier;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.crypto.RSASSAVerifier;
import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.ECKeyGenerator;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jose.util.ArrayUtils;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.oauth2.sdk.auth.Secret;

import org.elasticsearch.core.Tuple;

import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.Base64;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

/**
 * Utilities for JWT JWS create, sign, and verify, as well as generate JWT JWS credential.
 */
public class JwtUtil {
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    // Expect all JWSAlgorithm values to work in Family.HMAC_SHA, Family.RSA, and Family.EC (except ES256K)
    // Non-FIPS: Expect ES256K to fail because it is deprecated and disabled by default in Java 11.0.9/11.0.10 and 17
    // FIPS mode: Expect ES256K to succeed because it is still supported by BC-FIPS; not deprecated or disabled yet
    private static final JWSAlgorithm.Family EC_NON_FIPS = new JWSAlgorithm.Family(
        JWSAlgorithm.Family.EC.stream().filter(a -> (a.equals(JWSAlgorithm.ES256K) == false)).toArray(JWSAlgorithm[]::new)
    );

    public static final JWSAlgorithm.Family SUPPORTED_JWS_ALGORITHMS = new JWSAlgorithm.Family(
        ArrayUtils.concat(
            JWSAlgorithm.Family.HMAC_SHA.toArray(new JWSAlgorithm[] {}),
            JWSAlgorithm.Family.RSA.toArray(new JWSAlgorithm[] {}),
            EC_NON_FIPS.toArray(new JWSAlgorithm[] {})
        )
    );

    public static final JWSAlgorithm.Family SUPPORTED_JWS_ALGORITHMS_PUBLIC_KEY = new JWSAlgorithm.Family(
        ArrayUtils.concat(JWSAlgorithm.Family.RSA.toArray(new JWSAlgorithm[] {}), EC_NON_FIPS.toArray(new JWSAlgorithm[] {}))
    );

    public static SignedJWT signSignedJwt(final JWSSigner jwtSigner, final JWSHeader jwsHeader, final JWTClaimsSet jwtClaimsSet)
        throws JOSEException {
        final SignedJWT signedJwt = new SignedJWT(jwsHeader, jwtClaimsSet);
        signedJwt.sign(jwtSigner);
        return signedJwt;
    }

    public static boolean verifySignedJWT(final JWSVerifier jwtVerifier, final SignedJWT signedJwt) throws Exception {
        return signedJwt.verify(jwtVerifier);
    }

    public static Tuple<JWSSigner, JWSVerifier> createJwsSignerJwsVerifier(final Object secretOrSecretKeyOrKeyPair) throws JOSEException {
        if (secretOrSecretKeyOrKeyPair instanceof Secret secret) {
            return createJwsSignerJwsVerifier(secret);
        } else if (secretOrSecretKeyOrKeyPair instanceof SecretKey hmacKey) {
            return createJwsSignerJwsVerifier(hmacKey);
        } else if (secretOrSecretKeyOrKeyPair instanceof KeyPair keyPair) {
            return createJwsSignerJwsVerifier(keyPair);
        }
        throw new JOSEException(
            "Unsupported class ["
                + (secretOrSecretKeyOrKeyPair == null ? "null" : secretOrSecretKeyOrKeyPair.getClass().getCanonicalName())
                + "]. Supported classes are ["
                + Secret.class.getCanonicalName()
                + ", "
                + SecretKey.class.getCanonicalName()
                + ", "
                + KeyPair.class.getCanonicalName()
                + "]."
        );
    }

    public static Tuple<JWSSigner, JWSVerifier> createJwsSignerJwsVerifier(final KeyPair keyPair) throws JOSEException {
        final PrivateKey privateKey = keyPair.getPrivate();
        if (privateKey instanceof RSAPrivateKey rsaPrivateKey) {
            return new Tuple<>(new RSASSASigner(rsaPrivateKey), new RSASSAVerifier((RSAPublicKey) keyPair.getPublic()));
        } else if (privateKey instanceof ECPrivateKey ecPrivateKey) {
            return new Tuple<>(new ECDSASigner(ecPrivateKey), new ECDSAVerifier((ECPublicKey) keyPair.getPublic()));
        }
        throw new JOSEException(
            "Unsupported class ["
                + (privateKey == null ? "null" : privateKey.getClass().getCanonicalName())
                + "]. Supported classes are ["
                + RSAPrivateKey.class.getCanonicalName()
                + ", "
                + ECPrivateKey.class.getCanonicalName()
                + "]."
        );
    }

    public static Tuple<JWSSigner, JWSVerifier> createJwsSignerJwsVerifier(final SecretKey hmacKey) throws JOSEException {
        return new Tuple<>(new MACSigner(hmacKey), new MACVerifier(hmacKey));
    }

    public static Tuple<JWSSigner, JWSVerifier> createJwsSignerJwsVerifier(final Secret secret) throws JOSEException {
        final byte[] secretBytes = secret.getValueBytes();
        return new Tuple<>(new MACSigner(secretBytes), new MACVerifier(secretBytes));
    }

    // Nimbus JOSE+JWT requires HMAC keys to match or exceed digest strength.
    public static Secret generateSecret(final int hmacLengthBits) {
        final byte[] hmacKeyBytes = new byte[hmacLengthBits];
        SECURE_RANDOM.nextBytes(hmacKeyBytes);
        // Secret class in Nimbus JOSE JWT requires a UTF-8 String. Convert from random byte[] to a string.
        final String secretBytesAsString = Base64.getEncoder().encodeToString(hmacKeyBytes);
        return new Secret(secretBytesAsString);
    }

    // Nimbus JOSE+JWT requires HMAC keys to match or exceed digest strength.
    public static SecretKey generateSecretKey(final int hmacLengthBits) {
        final Secret secret = generateSecret(hmacLengthBits);
        return new SecretKeySpec(secret.getValueBytes(), "HMAC");
    }

    // Nimbus JOSE+JWT requires RSA keys to match or exceed 2048-bit strength. No dependency on digest strength.
    public static KeyPair generateRsaKeyPair(final int rsaLengthBits) throws JOSEException {
        final RSAKey rsaKeyPair = new RSAKeyGenerator(rsaLengthBits, false).generate();
        return new KeyPair(rsaKeyPair.toPublicKey(), rsaKeyPair.toPrivateKey());
    }

    // Nimbus JOSE+JWT requires EC curves to match digest strength (as per RFC 7519).
    public static KeyPair generateEcKeyPair(final Curve ecCurve) throws JOSEException {
        final ECKey ecKeyPair = new ECKeyGenerator(ecCurve).generate();
        return new KeyPair(ecKeyPair.toPublicKey(), ecKeyPair.toPrivateKey());
    }
}
