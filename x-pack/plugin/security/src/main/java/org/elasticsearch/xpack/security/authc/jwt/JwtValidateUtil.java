/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JOSEObjectType;
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
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.OctetSequenceKey;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.proc.DefaultJOSEObjectTypeVerifier;
import com.nimbusds.jose.proc.JOSEObjectTypeVerifier;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jose.util.Base64URL;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.SecureString;

import java.util.Date;
import java.util.List;

/**
 * Utilities for JWT validation.
 */
public class JwtValidateUtil {
    private static final Logger LOGGER = LogManager.getLogger(JwtValidateUtil.class);

    private static final JOSEObjectTypeVerifier<SecurityContext> JWT_HEADER_TYPE_VERIFIER = new DefaultJOSEObjectTypeVerifier<>(
        JOSEObjectType.JWT,
        null
    );

    public static void validateType(final SignedJWT jwt) throws Exception {
        final JOSEObjectType jwtHeaderType = jwt.getHeader().getType();
        try {
            JwtValidateUtil.JWT_HEADER_TYPE_VERIFIER.verify(jwtHeaderType, null);
        } catch (Exception e) {
            throw new Exception("Invalid JWT type [" + jwtHeaderType + "].", e);
        }
    }

    public static void validateIssuer(final SignedJWT jwt, String allowedIssuer) throws Exception {
        final String issuer = jwt.getJWTClaimsSet().getIssuer();
        if ((issuer == null) || (allowedIssuer.equals(issuer) == false)) {
            throw new Exception("Rejected issuer [" + issuer + "]. Allowed [" + allowedIssuer + "]");
        }
    }

    public static void validateAudiences(final SignedJWT jwt, List<String> allowedAudiences) throws Exception {
        final List<String> audiences = jwt.getJWTClaimsSet().getAudience();
        if ((audiences == null) || (allowedAudiences.stream().anyMatch(audiences::contains) == false)) {
            final String audiencesString = (audiences == null) ? "null" : String.join(",", audiences);
            throw new Exception("Rejected audiences [" + audiencesString + "]. Allowed [" + allowedAudiences + "]");
        }
    }

    public static void validateSignatureAlgorithm(final SignedJWT jwt, final List<String> allowedAlgorithms) throws Exception {
        final JWSAlgorithm algorithm = jwt.getHeader().getAlgorithm();
        if ((algorithm == null) || (allowedAlgorithms.contains(algorithm.getName()) == false)) {
            throw new Exception("Rejected algorithm [" + algorithm + "]. Allowed [" + String.join(",", allowedAlgorithms) + "]");
        }
    }

    public static void validateAuthTime(final SignedJWT jwt, final Date now, final long allowedClockSkewSeconds) throws Exception {
        JwtValidateUtil.validateAuthTime(jwt.getJWTClaimsSet().getDateClaim("auth_time"), now, allowedClockSkewSeconds);
    }

    // package private, so this logic can be called from unit tests without constructing a SignedJWT
    static void validateAuthTime(final Date authTime, final Date now, final long allowedClockSkewSeconds) throws Exception {
        if (authTime == null) {
            return; // optional
        } else if (now == null) {
            throw new Exception("Invalid now [null].");
        } else if (allowedClockSkewSeconds < 0L) {
            throw new Exception("Invalid negative allowedClockSkewSeconds [" + allowedClockSkewSeconds + "].");
        }
        // skewSec=0 auth_time=3:00:00.000 now=2:59:59.999 --> fail
        // skewSec=0 auth_time=3:00:00.000 now=3:00:00.000 --> pass
        // skewSec=1 auth_time=3:00:00.000 now=2:59:59.999 --> pass (subtract skew from auth_time)
        if ((authTime.getTime() - (allowedClockSkewSeconds * 1000L)) > now.getTime()) {
            throw new Exception(
                "Invalid auth_time ["
                    + authTime.getTime()
                    + "ms/"
                    + authTime
                    + "] > now ["
                    + now.getTime()
                    + "ms/"
                    + now
                    + "] with skew ["
                    + (allowedClockSkewSeconds * 1000L)
                    + "ms]."
            );
        }
    }

    public static void validateIssuedAtTime(final SignedJWT jwt, final Date now, final long allowedClockSkewSeconds) throws Exception {
        JwtValidateUtil.validateIssuedAtTime(jwt.getJWTClaimsSet().getIssueTime(), now, allowedClockSkewSeconds);
    }

    // package private, so this logic can be called from unit tests without constructing a SignedJWT
    static void validateIssuedAtTime(final Date iat, final Date now, final long allowedClockSkewSeconds) throws Exception {
        if (iat == null) {
            throw new Exception("Invalid iat [null].");
        } else if (now == null) {
            throw new Exception("Invalid now [null].");
        } else if (allowedClockSkewSeconds < 0L) {
            throw new Exception("Invalid negative allowedClockSkewSeconds [" + allowedClockSkewSeconds + "].");
        }
        // skewSec=0 iat=3:00:00.000 now=2:59:59.999 --> fail
        // skewSec=0 iat=3:00:00.000 now=3:00:00.000 --> pass
        // skewSec=1 iat=3:00:00.000 now=2:59:59.999 --> pass (subtract skew from iat)
        if ((iat.getTime() - (allowedClockSkewSeconds * 1000L)) > now.getTime()) {
            throw new Exception(
                "Invalid iat ["
                    + iat.getTime()
                    + "ms/"
                    + iat
                    + "] > now ["
                    + now.getTime()
                    + "ms/"
                    + now
                    + "] with skew ["
                    + (allowedClockSkewSeconds * 1000L)
                    + "ms]."
            );
        }
    }

    public static void validateNotBeforeTime(final SignedJWT jwt, final Date now, final long allowedClockSkewSeconds) throws Exception {
        JwtValidateUtil.validateNotBeforeTime(jwt.getJWTClaimsSet().getNotBeforeTime(), now, allowedClockSkewSeconds);
    }

    // package private, so this logic can be called from unit tests without constructing a SignedJWT
    static void validateNotBeforeTime(final Date nbf, final Date now, final long allowedClockSkewSeconds) throws Exception {
        if (nbf == null) {
            return; // optional
        } else if (now == null) {
            throw new Exception("Invalid now [null].");
        } else if (allowedClockSkewSeconds < 0L) {
            throw new Exception("Invalid negative allowedClockSkewSeconds [" + allowedClockSkewSeconds + "].");
        }
        // skewSec=0 nbf=3:00:00.000 now=2:59:59.999 --> fail
        // skewSec=0 nbf=3:00:00.000 now=3:00:00.000 --> pass
        // skewSec=1 nbf=3:00:00.000 now=2:59:59.999 --> pass (subtract skew from nbf)
        if ((nbf.getTime() - (allowedClockSkewSeconds * 1000L)) > now.getTime()) {
            throw new Exception(
                "Invalid nbf ["
                    + nbf.getTime()
                    + "ms/"
                    + nbf
                    + "] > now ["
                    + now.getTime()
                    + "ms/"
                    + now
                    + "] with skew ["
                    + (allowedClockSkewSeconds * 1000L)
                    + "ms]."
            );
        }
    }

    public static void validateExpiredTime(final SignedJWT jwt, final Date now, final long allowedClockSkewSeconds) throws Exception {
        JwtValidateUtil.validateExpiredTime(jwt.getJWTClaimsSet().getExpirationTime(), now, allowedClockSkewSeconds);
    }

    // package private, so this logic can be called from unit tests without constructing a SignedJWT
    static void validateExpiredTime(final Date exp, final Date now, final long allowedClockSkewSeconds) throws Exception {
        if (exp == null) {
            throw new Exception("Invalid exp [null].");
        } else if (now == null) {
            throw new Exception("Invalid now [null].");
        } else if (allowedClockSkewSeconds < 0L) {
            throw new Exception("Invalid allowedClockSkewSeconds [" + allowedClockSkewSeconds + "] < 0.");
        }
        // skewSec=0 now=2:59:59.999 exp=3:00:00.000 --> pass
        // skewSec=0 now=3:00:00.000 exp=3:00:00.000 --> fail
        // skewSec=1 now=3:00:00.000 exp=3:00:00.000 --> pass (subtract skew from now)
        if (now.getTime() - (allowedClockSkewSeconds * 1000L) >= exp.getTime()) {
            throw new Exception(
                "Invalid exp ["
                    + exp.getTime()
                    + "ms/"
                    + exp
                    + "] < now ["
                    + now.getTime()
                    + "ms/"
                    + now
                    + "] with skew ["
                    + (allowedClockSkewSeconds * 1000L)
                    + "ms]."
            );
        }
    }

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
            if (jwt.verify(JwtValidateUtil.createJwsVerifier(jwk))) {
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
        throw JwtValidateUtil.createExceptionInvalidJwkClass(jwk);
    }

    public static JWSSigner createJwsSigner(final JWK jwk) throws JOSEException {
        if (jwk instanceof RSAKey rsaKey) {
            return new RSASSASigner(rsaKey);
        } else if (jwk instanceof ECKey ecKey) {
            return new ECDSASigner(ecKey);
        } else if (jwk instanceof OctetSequenceKey octetSequenceKey) {
            return new MACSigner(octetSequenceKey);
        }
        throw JwtValidateUtil.createExceptionInvalidJwkClass(jwk);
    }

    private static JOSEException createExceptionInvalidJwkClass(final JWK jwk) {
        return new JOSEException(
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

    // Build from Base64 components. Signature may or may not be valid. Useful for negative test cases.
    public static SecureString buildJwt(final JWSHeader header, final JWTClaimsSet claims, final Base64URL signature) throws Exception {
        final SignedJWT signedJwt = new SignedJWT(header.toBase64URL(), claims.toPayload().toBase64URL(), signature);
        return new SecureString(signedJwt.serialize().toCharArray());
    }

    public static SignedJWT buildUnsignedJwt(final JWSHeader jwtHeader, final JWTClaimsSet jwtClaimsSet) {
        return new SignedJWT(jwtHeader, jwtClaimsSet);
    }

    // Convenience method to construct JWSVerifier from JWK, and verify the signed JWT
    public static boolean verifyJwt(final JWK jwk, final SignedJWT signedJwt) throws Exception {
        return signedJwt.verify(JwtValidateUtil.createJwsVerifier(jwk));
    }

    // Convenience method to construct JWSSigner from JWK, sign the JWT, and return serialized SecureString
    public static SecureString signJwt(final JWK jwk, final SignedJWT unsignedJwt) throws Exception {
        // Copy the header and claims set to a new unsigned JWT, in case JWT is being re-signing
        final SignedJWT signedJwt = new SignedJWT(unsignedJwt.getHeader(), unsignedJwt.getJWTClaimsSet());
        signedJwt.sign(JwtValidateUtil.createJwsSigner(jwk));
        return new SecureString(signedJwt.serialize().toCharArray());
    }
}
