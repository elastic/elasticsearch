/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.Algorithm;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.KeyOperation;
import com.nimbusds.jose.jwk.KeyUse;
import com.nimbusds.jose.jwk.OctetSequenceKey;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;

import java.security.PublicKey;
import java.security.interfaces.RSAPublicKey;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Utilities for JWK Validation.
 */
@SuppressWarnings("checkstyle:MissingJavadocMethod")
public class JwkValidateUtil {
    private static final Logger LOGGER = LogManager.getLogger(JwkValidateUtil.class);

    public static JWKSet loadJwkSetFromString(final String jwkSetConfigKey, final String jwkSetContents) throws SettingsException {
        try {
            return JWKSet.parse(jwkSetContents);
        } catch (Exception e) {
            throw new SettingsException("JWKSet parse failed for setting [" + jwkSetConfigKey + "]", e);
        }
    }

    // Static method for unit testing. No need to construct a complete RealmConfig with all settings.
    public static Tuple<JWKSet, JWKSet> validateAndLoadJwkSetsSettings(
        final String allowedSignatureAlgorithmsConfigKey,
        final List<String> allowedSignatureAlgorithms,
        final String jwkSetConfigKeyHmac,
        final String jwkSetContentsHmac,
        final String jwkSetConfigKeyPkc,
        final String jwkSetContentsPkc
    ) throws SettingsException {
        final List<String> allowedSignatureAlgorithmsHmac = allowedSignatureAlgorithms.stream()
            .filter(JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_HMAC::contains)
            .toList();
        final List<String> allowedSignatureAlgorithmsPkc = allowedSignatureAlgorithms.stream()
            .filter(JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_PKC::contains)
            .toList();

        if ((Strings.hasText(jwkSetContentsHmac) == false) && (Strings.hasText(jwkSetContentsPkc) == false)) {
            // HMAC JWKSet and RSA/EC JWKSet both missing
            throw new SettingsException(
                "Missing setting(s). At least one JWKSet must be set in [" + jwkSetConfigKeyHmac + "] or [" + jwkSetConfigKeyPkc + "]"
            );
        } else if (Strings.hasText(jwkSetContentsHmac) && (allowedSignatureAlgorithmsHmac.isEmpty())) {
            // HMAC JWKSet present without any HMAC signature algorithms
            throw new SettingsException(
                "Settings mismatch. HMAC JWKSet is set in ["
                    + jwkSetConfigKeyHmac
                    + "], but no HMAC signature algorithms are set in ["
                    + allowedSignatureAlgorithmsConfigKey
                    + "]"
            );
        } else if ((allowedSignatureAlgorithmsHmac.isEmpty() == false) && (Strings.hasText(jwkSetContentsHmac) == false)) {
            // HMAC signature algorithms present without a HMAC JWKSet
            throw new SettingsException(
                "Settings mismatch. HMAC signature algorithms are set in ["
                    + allowedSignatureAlgorithmsConfigKey
                    + "], but no HMAC JWKSet is set in ["
                    + jwkSetConfigKeyHmac
                    + "]"
            );
        } else if (Strings.hasText(jwkSetContentsPkc) && (allowedSignatureAlgorithmsPkc.isEmpty())) {
            // RSA/EC JWKSet present without any RSA/EC signature algorithms
            throw new SettingsException(
                "Settings mismatch. RSA/EC JWKSet is set in ["
                    + jwkSetConfigKeyPkc
                    + "], but no RSA/EC signature algorithms are set in ["
                    + allowedSignatureAlgorithmsConfigKey
                    + "]"
            );
        } else if ((allowedSignatureAlgorithmsPkc.isEmpty() == false) && (Strings.hasText(jwkSetContentsPkc) == false)) {
            // RSA/EC signature algorithms present without an RSA/EC JWKSet
            throw new SettingsException(
                "Settings mismatch. RSA/EC signature algorithms are set in ["
                    + allowedSignatureAlgorithmsConfigKey
                    + "], but no RSA/EC JWKSet is set in ["
                    + jwkSetConfigKeyPkc
                    + "]"
            );
        }

        // At this point, one or both JWKSets are configured. Load and validate their JWK contents against each set of signature algorithms.
        JWKSet jwkSetHmac = null;
        if (Strings.hasText(jwkSetContentsHmac)) {
            jwkSetHmac = JwkValidateUtil.loadJwkSetFromString(jwkSetConfigKeyHmac, jwkSetContentsHmac);
            JwkValidateUtil.validateJwkSetContents(jwkSetConfigKeyHmac, jwkSetHmac, allowedSignatureAlgorithmsHmac);
        }
        JWKSet jwkSetPkc = null;
        if (Strings.hasText(jwkSetContentsPkc)) {
            jwkSetPkc = JwkValidateUtil.loadJwkSetFromString(jwkSetConfigKeyPkc, jwkSetContentsPkc);
            JwkValidateUtil.validateJwkSetContents(jwkSetConfigKeyPkc, jwkSetPkc, allowedSignatureAlgorithmsPkc);
        }
        return new Tuple<>(jwkSetHmac, jwkSetPkc);
    }

    public static void validateJwkSetContents(
        final String jwkSetConfigKey,
        final JWKSet jwkSet,
        final List<String> allowedSignatureAlgorithms
    ) throws SettingsException {
        final List<JWK> jwks = jwkSet.getKeys();
        if (jwks.isEmpty()) {
            throw new SettingsException("JWKSet contents are empty in [" + jwkSetConfigKey + "].");
        }
        for (final JWK jwk : jwks) {
            if (jwk instanceof OctetSequenceKey jwkHmac) {
                isOkJwkTypeHmac(jwkSetConfigKey, jwkHmac, allowedSignatureAlgorithms);
            } else if (jwk instanceof RSAKey jwkRsa) {
                isOkJwkTypeRsa(jwkSetConfigKey, jwkRsa, allowedSignatureAlgorithms);
            } else if (jwk instanceof ECKey jwkEc) {
                isOkJwkTypeEc(jwkSetConfigKey, jwkEc, allowedSignatureAlgorithms);
            } else {
                throw new SettingsException(
                    "JWKSet [" + jwkSetConfigKey + " contains invalid JWK of class [" + jwk.getClass().getCanonicalName() + "]."
                );
            }
        }
    }

    public static int computeBitLengthRsa(final PublicKey publicKey) throws Exception {
        if (publicKey instanceof RSAPublicKey rsaPublicKey) {
            final int modulusBigLength = rsaPublicKey.getModulus().bitLength();
            return (modulusBigLength + 8 - 1) / 8 * 8;
        } else if (publicKey == null) {
            throw new Exception("Expected public key class [RSAPublicKey]. Got [" + "null" + "] instead.");
        }
        throw new Exception("Expected public key class [RSAPublicKey]. Got [" + publicKey.getClass().getSimpleName() + "] instead.");
    }

    public static int computeMinBitLengthHmac(final List<String> allowedSignatureAlgorithmsHmac) throws SettingsException {
        int minBitLengthHmacOverall = Integer.MAX_VALUE;
        for (final String signatureAlgorithmHmac : allowedSignatureAlgorithmsHmac) {
            try {
                final int minBitLengthHmac = MACSigner.getMinRequiredSecretLength(JWSAlgorithm.parse(signatureAlgorithmHmac));
                minBitLengthHmacOverall = Math.min(minBitLengthHmacOverall, minBitLengthHmac);
            } catch (JOSEException e) {
                throw new SettingsException("Invalid HMAC signature algorithm [" + signatureAlgorithmHmac + "].", e);
            }
        }
        return minBitLengthHmacOverall;
    }

    @SuppressWarnings({ "unused" })
    public static int computeMinBitLengthRsa(final List<String> allowedSignatureAlgorithmsRsa) {
        return RSAKeyGenerator.MIN_KEY_SIZE_BITS;
    }

    public static Set<Curve> computeAllowedCurvesEc(final List<String> allowedSignatureAlgorithmsHmac) {
        final Set<Curve> allowedCurvesEc = new HashSet<>(); // Computed below
        for (final String signatureAlgorithmEc : allowedSignatureAlgorithmsHmac) {
            allowedCurvesEc.addAll(Curve.forJWSAlgorithm(JWSAlgorithm.parse(signatureAlgorithmEc)));
        }
        return allowedCurvesEc;
    }

    public static void isOkJwkTypeHmac(
        final String configKeyJwkSetHmac,
        final OctetSequenceKey jwkHmac,
        final List<String> allowedSignatureAlgorithms
    ) {
        final List<String> allowedSignatureAlgorithmsHmac = allowedSignatureAlgorithms.stream()
            .filter(JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_HMAC::contains)
            .toList();
        final int minBitLengthHmac = JwkValidateUtil.computeMinBitLengthHmac(allowedSignatureAlgorithmsHmac);
        if (minBitLengthHmac == Integer.MAX_VALUE) {
            throw new SettingsException(
                "JWKSet contains HMAC JWK in ["
                    + configKeyJwkSetHmac
                    + "], but there are no HMAC signature algorithms. Signature algorithms are ["
                    + String.join(",", allowedSignatureAlgorithms)
                    + "]."
            );
        } else if (jwkHmac.size() < minBitLengthHmac) {
            throw new SettingsException(
                "JWKSet contains HMAC JWK in ["
                    + configKeyJwkSetHmac
                    + "], but bit length is smaller than minimum allowed by signature algorithms ["
                    + String.join(",", allowedSignatureAlgorithms)
                    + "]."
            );
        }
    }

    public static void isOkJwkTypeRsa(
        final String configKeyJwkSetHmac,
        final RSAKey jwkRsa,
        final List<String> allowedSignatureAlgorithms
    ) {
        final List<String> allowedSignatureAlgorithmsRsa = allowedSignatureAlgorithms.stream()
            .filter(JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_RSA::contains)
            .toList();
        final int minBitLengthRsa = JwkValidateUtil.computeMinBitLengthRsa(allowedSignatureAlgorithmsRsa);
        if (minBitLengthRsa == Integer.MAX_VALUE) {
            throw new SettingsException(
                "JWKSet contains RSA JWK in ["
                    + configKeyJwkSetHmac
                    + ", but there are no RSA signature algorithms. Signature algorithms are ["
                    + String.join(",", allowedSignatureAlgorithms)
                    + "]."
            );
        }
        final int bitLengthRsa;
        try {
            bitLengthRsa = JwkValidateUtil.computeBitLengthRsa(jwkRsa.toPublicKey());
        } catch (Exception e) {
            throw new SettingsException(
                "JWKSet contains RSA JWK in [" + configKeyJwkSetHmac + "], but computing its bit length failed.",
                e
            );
        }
        if (bitLengthRsa < minBitLengthRsa) {
            throw new SettingsException(
                "JWKSet contains RSA JWK in ["
                    + configKeyJwkSetHmac
                    + "], but its bit length ["
                    + bitLengthRsa
                    + "] is smaller than minimum allowed by signature algorithms ["
                    + String.join(",", allowedSignatureAlgorithmsRsa)
                    + "]."
            );
        }
    }

    public static void isOkJwkTypeEc(final String configKeyJwkSetHmac, final ECKey jwkEc, final List<String> allowedSignatureAlgorithms) {
        final List<String> allowedSignatureAlgorithmsEc = allowedSignatureAlgorithms.stream()
            .filter(JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_EC::contains)
            .toList();
        final Set<Curve> allowedEcCurves = JwkValidateUtil.computeAllowedCurvesEc(allowedSignatureAlgorithmsEc);
        if (allowedEcCurves.isEmpty()) {
            throw new SettingsException(
                "JWKSet contains EC JWK in ["
                    + configKeyJwkSetHmac
                    + "], but there are no EC signature algorithms. Signature algorithms are ["
                    + String.join(",", allowedSignatureAlgorithms)
                    + "]."
            );
        }
        if (allowedEcCurves.contains(jwkEc.getCurve()) == false) {
            throw new SettingsException(
                "JWKSet contains EC JWK in ["
                    + configKeyJwkSetHmac
                    + "], but there are no EC signature algorithms for that curve. EC signature algorithms are ["
                    + String.join(",", allowedSignatureAlgorithmsEc)
                    + "]. Curves are ["
                    + allowedEcCurves
                    + "]."
            );
        }
    }

    // Remove JWKs if they have an optional kid and it does not match the filter
    public static List<JWK> removeJwkKidMismatches(final List<JWK> jwks, final String jwtKid) {
        if (Strings.hasText(jwtKid)) {
            final List<JWK> filtered = jwks.stream().filter(jwk -> isJwkKidOk(jwk, jwtKid)).toList();
            LOGGER.info("JWK count " + jwks.size() + " now " + filtered.size() + " after KID filter [" + jwtKid + "].");
            return filtered;
        }
        return jwks;
    }

    // Remove JWKs if they have an optional KeyUse and it does not match the filter
    public static List<JWK> removeJwksKeyUseMismatches(final List<JWK> jwks, final KeyUse keyUse) {
        final List<JWK> filtered = jwks.stream().filter(jwk -> isJwkKeyUseOk(jwk, keyUse)).toList();
        LOGGER.info("JWK count " + jwks.size() + " now " + filtered.size() + " after KeyUse filter [" + keyUse + "].");
        return filtered;
    }

    // Remove JWKs if they have an optional KeyOperation and it does not match the filter
    public static List<JWK> removeJwksKeyOperationMismatches(final List<JWK> jwks, final KeyOperation keyOperation) {
        final List<JWK> filtered = jwks.stream().filter(jwk -> isJwkKeyOperationOk(jwk, keyOperation)).toList();
        LOGGER.info("JWK count " + jwks.size() + " now " + filtered.size() + " after KeyOperation filter [" + keyOperation + "].");
        return filtered;
    }

    // Remove JWKs if they have an optional Algorithm and it does not match the filter
    public static List<JWK> removeJwksAlgMismatches(final List<JWK> jwks, final Algorithm algorithm) {
        final List<JWK> filtered = jwks.stream().filter(jwk -> isJwkAlgOk(jwk, algorithm)).toList();
        LOGGER.info("JWK count " + jwks.size() + " now " + filtered.size() + " after Algorithm filter [" + algorithm + "].");
        return filtered;
    }

    // Remove JWKs if their key type or length/curve do not match the algorithm filter
    public static List<JWK> removeJwkTypeMismatches(final List<JWK> jwks, final Algorithm algorithm) throws Exception {
        final List<JWK> filtered;
        if (JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_HMAC.contains(algorithm.getName())) {
            filtered = jwks.stream().filter(jwk -> JwkValidateUtil.isOkJwkTypeHmac(jwk, algorithm.getName())).toList();
        } else if (JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_RSA.contains(algorithm.getName())) {
            filtered = jwks.stream().filter(jwk -> JwkValidateUtil.isOkJwkTypeRsa(jwk)).toList();
        } else if (JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_EC.contains(algorithm.getName())) {
            filtered = jwks.stream().filter(jwk -> JwkValidateUtil.isOkJwkTypeEc(jwk, algorithm.getName())).toList();
        } else {
            throw new Exception("Unsupported algorithm [" + algorithm + "].");
        }
        LOGGER.info("JWK count " + jwks.size() + " now " + filtered.size() + " after type+length filter [" + algorithm + "].");
        return filtered;
    }

    private static boolean isJwkKidOk(final JWK jwk, final String kid) {
        return jwk.getKeyID() == null || kid.equals(jwk.getKeyID());
    }

    private static boolean isJwkKeyUseOk(final JWK jwk, final KeyUse keyUse) {
        return jwk.getKeyUse() == null || keyUse.equals(jwk.getKeyUse());
    }

    private static boolean isJwkKeyOperationOk(final JWK jwk, final KeyOperation keyOperation) {
        return (jwk.getKeyOperations() == null) || jwk.getKeyOperations().contains(keyOperation);
    }

    private static boolean isJwkAlgOk(final JWK jwk, final Algorithm algorithm) {
        return jwk.getAlgorithm() == null || algorithm.equals(jwk.getAlgorithm());
    }

    private static boolean isOkJwkTypeHmac(final JWK jwk, final String signatureAlgorithm) {
        if (jwk instanceof OctetSequenceKey jwkHmac) {
            try {
                return jwkHmac.size() >= MACSigner.getMinRequiredSecretLength(JWSAlgorithm.parse(signatureAlgorithm));
            } catch (JOSEException e) {
                LOGGER.trace("Unexpected exception", e);
            }
        }
        return false;
    }

    private static boolean isOkJwkTypeRsa(final JWK jwk) {
        if (jwk instanceof RSAKey jwkRsa) {
            try {
                return JwkValidateUtil.computeBitLengthRsa(jwkRsa.toPublicKey()) >= 2048;
            } catch (Exception e) {
                LOGGER.trace("Unexpected exception", e);
            }
        }
        return false;
    }

    private static boolean isOkJwkTypeEc(final JWK jwk, final String signatureAlgorithm) {
        if (jwk instanceof ECKey jwkEc) {
            try {
                return Curve.forJWSAlgorithm(JWSAlgorithm.parse(signatureAlgorithm)).contains(jwkEc.getCurve());
            } catch (Exception e) {
                LOGGER.trace("Unexpected exception", e);
            }
        }
        return false;
    }
}
