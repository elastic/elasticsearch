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
import com.nimbusds.jose.jwk.KeyOperation;
import com.nimbusds.jose.jwk.KeyUse;
import com.nimbusds.jose.jwk.OctetSequenceKey;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.proc.DefaultJOSEObjectTypeVerifier;
import com.nimbusds.jose.proc.JOSEObjectTypeVerifier;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Date;
import java.util.List;

/**
 * Utilities for JWT validation.
 */
@SuppressWarnings({ "checkstyle:MissingJavadocType", "checkstyle:MissingJavadocMethod" })
public class JwtValidateUtil {
    private static final Logger LOGGER = LogManager.getLogger(JwtValidateUtil.class);

    private static final JOSEObjectTypeVerifier<SecurityContext> JWT_HEADER_TYPE_VERIFIER = new DefaultJOSEObjectTypeVerifier<>(
        JOSEObjectType.JWT,
        null
    );

    /**
     * Validate a SignedJWT. Use iss/aud/alg filters for those claims, JWKSet for signature, and skew seconds for time claims.
     * @param jwt Signed JWT to be validated.
     * @param jwks JWKs of HMAC secret keys or RSA/EC public keys.
     * @param allowedIssuer Filter for the "iss" claim.
     * @param allowedAudiences Filter for the "aud" claim.
     * @param allowedSignatureAlgorithms Filter for the "aud" header.
     * @param allowedClockSkewSeconds Skew tolerance for the "auth_time", "iat", "nbf", and "exp" claims.
     * @throws Exception Error for the first validation to fail.
     */
    public static void validate(
        final SignedJWT jwt,
        final List<JWK> jwks,
        final String allowedIssuer,
        final List<String> allowedAudiences,
        final List<String> allowedSignatureAlgorithms,
        final long allowedClockSkewSeconds
    ) throws Exception {
        final Date now = new Date();
        LOGGER.debug(
            "Validating JWT, now=["
                + now
                + "], alg=["
                + jwt.getHeader().getAlgorithm()
                + "], issuer=["
                + jwt.getJWTClaimsSet().getIssuer()
                + "], audiences=["
                + jwt.getJWTClaimsSet().getAudience()
                + "], typ=["
                + jwt.getHeader().getType()
                + "], auth_time=["
                + jwt.getJWTClaimsSet().getDateClaim("auth_time")
                + "], iat=["
                + jwt.getJWTClaimsSet().getIssueTime()
                + "], nbf=["
                + jwt.getJWTClaimsSet().getIssueTime()
                + "], exp=["
                + jwt.getJWTClaimsSet().getExpirationTime()
                + "], kid=["
                + jwt.getHeader().getKeyID()
                + "], jti=["
                + jwt.getJWTClaimsSet().getJWTID()
        );
        // validate claims before signature, because log messages about rejected claims can be more helpful than rejected signatures
        JwtValidateUtil.validateIssuer(jwt, allowedIssuer);
        JwtValidateUtil.validateAudiences(jwt, allowedAudiences);
        JwtValidateUtil.validateType(jwt);
        JwtValidateUtil.validateSignatureAlgorithm(jwt, allowedSignatureAlgorithms);
        JwtValidateUtil.validateAuthTime(jwt, now, allowedClockSkewSeconds);
        JwtValidateUtil.validateIssuedAtTime(jwt, now, allowedClockSkewSeconds);
        JwtValidateUtil.validateNotBeforeTime(jwt, now, allowedClockSkewSeconds);
        JwtValidateUtil.validateExpiredTime(jwt, now, allowedClockSkewSeconds);
        JwtValidateUtil.validateSignature(jwt, jwks);
    }

    public static void validateSignatureAlgorithm(final SignedJWT jwt, final List<String> allowedSignatureAlgorithms) throws Exception {
        final JWSAlgorithm jwtHeaderAlgorithm = jwt.getHeader().getAlgorithm();
        if ((jwtHeaderAlgorithm == null) || (allowedSignatureAlgorithms.contains(jwtHeaderAlgorithm.getName()) == false)) {
            throw new Exception(
                "Rejected signature algorithm ["
                    + jwtHeaderAlgorithm
                    + "]. Allowed signature algorithms are ["
                    + String.join(",", allowedSignatureAlgorithms)
                    + "]"
            );
        }
    }

    public static void validateIssuer(final SignedJWT jwt, String allowedIssuer) throws Exception {
        final String issuer = jwt.getJWTClaimsSet().getIssuer();
        if ((issuer == null) || (allowedIssuer.equals(issuer) == false)) {
            throw new Exception("Rejected issuer [" + issuer + "]. Allowed issuer is [" + allowedIssuer + "]");
        }
    }

    public static void validateAudiences(final SignedJWT jwt, List<String> allowedAudiences) throws Exception {
        final List<String> audiences = jwt.getJWTClaimsSet().getAudience();
        if ((audiences == null) || (allowedAudiences.stream().anyMatch(audiences::contains) == false)) {
            throw new Exception("Rejected audiences [" + audiences + "]. Allowed audiences are [" + allowedAudiences + "]");
        }
    }

    public static void validateType(final SignedJWT jwt) throws Exception {
        final JOSEObjectType jwtHeaderType = jwt.getHeader().getType();
        try {
            JwtValidateUtil.JWT_HEADER_TYPE_VERIFIER.verify(jwtHeaderType, null);
        } catch (Exception e) {
            throw new Exception("Invalid JWT type [" + jwtHeaderType + "].", e);
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
        JwtValidateUtil.validateNotBeforeTime(jwt.getJWTClaimsSet().getIssueTime(), now, allowedClockSkewSeconds);
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
        // skewSec=0 nfb=3:00:00.000 now=2:59:59.999 --> fail
        // skewSec=0 nfb=3:00:00.000 now=3:00:00.000 --> pass
        // skewSec=1 nfb=3:00:00.000 now=2:59:59.999 --> pass (subtract skew from nfb)
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
        // Filter JWKs by optional KID attribute. See JwtTestCase.randomSettingsForJwkGenerator for example of JWK optional attributes.
        final List<JWK> filteredByKid = JwkValidateUtil.removeJwkKidMismatches(jwks, jwt.getHeader().getKeyID());
        LOGGER.trace(
            "JWK count " + jwks.size() + " reduced to " + filteredByKid.size() + " after KID filter [" + jwt.getHeader().getKeyID() + "]."
        );

        // Filter JWKs by optional ALG attribute. See JwtTestCase.randomSettingsForJwkGenerator for example of JWK optional attributes.
        final List<JWK> filteredByAlg = JwkValidateUtil.removeJwksAlgMismatches(filteredByKid, jwt.getHeader().getAlgorithm());
        LOGGER.trace(
            "JWK count "
                + filteredByKid.size()
                + " reduced to "
                + filteredByAlg.size()
                + " after Algorithm filter ["
                + jwt.getHeader().getAlgorithm()
                + "]."
        );

        // Filter JWKs by optional USE attribute. See JwtTestCase.randomSettingsForJwkGenerator for example of JWK optional attributes.
        final List<JWK> filteredByUse = JwkValidateUtil.removeJwksKeyUseMismatches(filteredByAlg, KeyUse.SIGNATURE);
        LOGGER.trace(
            "JWK count " + filteredByAlg.size() + " reduced to " + filteredByUse.size() + " after KeyUse filter [" + KeyUse.SIGNATURE + "]."
        );

        // Filter JWKs by optional OPS attribute. See JwtTestCase.randomSettingsForJwkGenerator for example of JWK optional attributes.
        final List<JWK> filteredByOp = JwkValidateUtil.removeJwksKeyOperationMismatches(filteredByUse, KeyOperation.VERIFY);
        LOGGER.trace(
            "JWK count "
                + filteredByUse.size()
                + " reduced to "
                + filteredByOp.size()
                + " after KeyOperation filter ["
                + KeyOperation.VERIFY
                + "]."
        );

        // Filter JWKs by type and size/curve. See JwtTestCase.randomSettingsForJwkGenerator for example of JWK optional attributes.
        final List<JWK> filteredByType = JwkValidateUtil.removeJwkTypeOrStrengthMismatches(filteredByOp, jwt.getHeader().getAlgorithm());
        LOGGER.trace(
            "JWK count "
                + filteredByOp.size()
                + " reduced to "
                + filteredByType.size()
                + " after type+length filter ["
                + jwt.getHeader().getAlgorithm()
                + "]."
        );

        for (final JWK jwk : filteredByType) {
            if (jwt.verify(JwtValidateUtil.createJwsVerifier(jwk))) {
                return; // VERIFY SUCCEEDED
            }
        }
        throw new Exception("Verify failed using " + filteredByType.size() + " of " + jwks.size() + " provided JWKs.");
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
            "Unsupported class ["
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

    public static JWSSigner createJwsSigner(final JWK jwk) throws JOSEException {
        if (jwk instanceof RSAKey rsaKey) {
            return new RSASSASigner(rsaKey);
        } else if (jwk instanceof ECKey ecKey) {
            return new ECDSASigner(ecKey);
        } else if (jwk instanceof OctetSequenceKey octetSequenceKey) {
            return new MACSigner(octetSequenceKey);
        }
        throw new JOSEException(
            "Unsupported class ["
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

    public static boolean verifyJWT(final JWSVerifier jwtVerifier, final SignedJWT signedJwt) throws Exception {
        return signedJwt.verify(jwtVerifier);
    }

    public static SignedJWT signJwt(final JWSSigner jwtSigner, final JWSHeader jwsHeader, final JWTClaimsSet jwtClaimsSet)
        throws JOSEException {
        final SignedJWT signedJwt = new SignedJWT(jwsHeader, jwtClaimsSet);
        signedJwt.sign(jwtSigner);
        return signedJwt;
    }
}
