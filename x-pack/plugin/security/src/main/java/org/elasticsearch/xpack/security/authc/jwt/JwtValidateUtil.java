/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.crypto.ECDSAVerifier;
import com.nimbusds.jose.crypto.MACVerifier;
import com.nimbusds.jose.crypto.RSASSAVerifier;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.OctetSequenceKey;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jwt.SignedJWT;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;

import java.util.List;

/**
 * Utilities for JWT validation.
 */
public class JwtValidateUtil {
    private static final Logger LOGGER = LogManager.getLogger(JwtValidateUtil.class);

    // TODO: rationalise the exceptions, differentiate between 400 and 500
    /**
     * Look through each JWK in the JWKSet to see if they can validate the Signed JWT signature.
     * Apply JWT kid and JWT alg filters to the JWKs to skip unnecessary signature checking.
     *
     * If JWT kid is present, and any JWK kid matches, only use the matching subset of JWKs. Ignore the rest.
     * Note: JWK kid should be unique. However, this method does not assume they are unique. Each match will be tried.
     *
     * Depending on the JWT alg, certain HMAC/RSA/EC JWKs can be excluded.
     * HMAC JWKs that do not meet the minimum length requirement are ignored.
     * RSA JWKs that do not meet the minimum length requirement are ignored.
     * EC JWKs that do not meet the exact curve requirement are ignored.
     *
     * @param jwt Signed JWT to be validated.
     * @param jwks JWKSet of HMAC/RSA/EC JWKs. At least one JWK is required to succeed.
     * @throws Exception Error if JWKs fail to validate the Signed JWT.
     */
    public static void validateSignature(final SignedJWT jwt, final List<JWK> jwks) throws Exception {
        assert jwks != null : "Verify requires a non-null JWK list";
        if (jwks.isEmpty()) {
            throw new ElasticsearchException("Verify requires a non-empty JWK list");
        }
        final String id = jwt.getHeader().getKeyID();
        final JWSAlgorithm alg = jwt.getHeader().getAlgorithm();
        LOGGER.trace("JWKs [{}], JWT KID [{}], and JWT Algorithm [{}] before filters.", jwks.size(), id, alg.getName());

        // If JWT has optional kid header, and realm JWKs have optional kid attribute, any mismatches JWT.kid vs JWK.kid can be ignored.
        // Keep any JWKs if JWK optional kid attribute is missing. Keep all JWKs if JWT optional kid header is missing.
        final List<JWK> jwksKid = jwks.stream().filter(j -> ((id == null) || (j.getKeyID() == null) || (id.equals(j.getKeyID())))).toList();
        LOGGER.trace("JWKs [{}] after KID [{}](|null) filter.", jwksKid.size(), id);

        // JWT has mandatory alg header. If realm JWKs have optional alg attribute, any mismatches JWT.alg vs JWK.alg can be ignored.
        // Keep any JWKs if JWK optional alg attribute is missing.
        final List<JWK> jwksAlg = jwksKid.stream().filter(j -> (j.getAlgorithm() == null) || (alg.equals(j.getAlgorithm()))).toList();
        LOGGER.trace("JWKs [{}] after Algorithm [{}](|null) filter.", jwksAlg.size(), alg.getName());

        // PKC Example: Realm has five PKC JWKs RSA-2048, RSA-3072, EC-P256, EC-P384, and EC-P512. JWT alg allows ignoring some.
        // - If JWT alg is RS256, only RSA-2048 and RSA-3072 are valid for a JWT RS256 signature. Ignore three EC JWKs.
        // - If JWT alg is ES512, only EC-P512 is valid for a JWT ES512 signature. Ignore four JWKs (two RSA, two EC).
        // - If JWT alg is ES384, only EC-P384 is valid for a JWT ES384 signature. Ignore four JWKs (two RSA, two EC).
        // - If JWT alg is ES256, only EC-P256 is valid for a JWT ES256 signature. Ignore four JWKs (two RSA, two EC).
        //
        // HMAC Example: Realm has six HMAC JWKs of bit lengths 256, 320, 384, 400, 512, and 1000. JWT alg allows ignoring some.
        // - If JWT alg is HS256, all are valid for a JWT HS256 signature. Don't ignore any HMAC JWKs.
        // - If JWT alg is HS384, only 384, 400, 512, and 1000 are valid for a JWT HS384 signature. Ignore two HMAC JWKs.
        // - If JWT alg is HS512, only 512 and 1000 are valid for a JWT HS512 signature. Ignore four HMAC JWKs.
        final List<JWK> jwksStrength = jwksAlg.stream().filter(j -> JwkValidateUtil.isMatch(j, alg.getName())).toList();
        LOGGER.debug("JWKs [{}] after Algorithm [{}] match filter.", jwksStrength.size(), alg);

        // No JWKs passed the kid, alg, and strength checks, so nothing left to use in verifying the JWT signature
        if (jwksStrength.isEmpty()) {
            throw new ElasticsearchException("Verify failed because all " + jwks.size() + " provided JWKs were filtered.");
        }

        for (final JWK jwk : jwksStrength) {
            if (jwt.verify(createJwsVerifier(jwk))) {
                LOGGER.trace(
                    "JWT signature validation succeeded with JWK kty=[{}], jwtAlg=[{}], jwtKid=[{}], use=[{}], ops=[{}]",
                    jwk.getKeyType(),
                    jwk.getAlgorithm(),
                    jwk.getKeyID(),
                    jwk.getKeyUse(),
                    jwk.getKeyOperations()
                );
                return;
            } else {
                LOGGER.trace(
                    "JWT signature validation failed with JWK kty=[{}], jwtAlg=[{}], jwtKid=[{}], use=[{}], ops={}",
                    jwk.getKeyType(),
                    jwk.getAlgorithm(),
                    jwk.getKeyID(),
                    jwk.getKeyUse(),
                    jwk.getKeyOperations() == null ? "[null]" : jwk.getKeyOperations()
                );
            }
        }

        throw new ElasticsearchException("Verify failed using " + jwksStrength.size() + " of " + jwks.size() + " provided JWKs.");
    }

    public static JWSVerifier createJwsVerifier(final JWK jwk) throws JOSEException {
        if (jwk instanceof RSAKey rsaKey) {
            return new RSASSAVerifier(rsaKey);
        } else if (jwk instanceof ECKey ecKey) {
            return new ECDSAVerifier(ecKey);
        } else if (jwk instanceof OctetSequenceKey octetSequenceKey) {
            return new MACVerifier(octetSequenceKey);
        }
        throw new JOSEException(
            "Unsupported JWK class ["
                + (jwk == null ? "null" : jwk.getClass().getCanonicalName())
                + "]. Supported classes are ["
                + RSAKey.class.getCanonicalName()
                + ", "
                + ECKey.class.getCanonicalName()
                + ", "
                + OctetSequenceKey.class.getCanonicalName()
                + "]."
        );
    }
}
