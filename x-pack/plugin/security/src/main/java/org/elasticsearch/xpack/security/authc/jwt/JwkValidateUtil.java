/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.Algorithm;
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
import java.util.Collections;
import java.util.List;

/**
 * Utilities for JWK Validation.
 */
public class JwkValidateUtil {
    private static final Logger LOGGER = LogManager.getLogger(JwkValidateUtil.class);

    // Static method for unit testing. No need to construct a complete RealmConfig with all settings.
    static Tuple<List<String>, List<JWK>> filterJwksAndAlgorithms(final List<JWK> jwks, final List<String> algs) throws SettingsException {
        // Keep algorithms with at least one corresponding JWK+strength. Keep JWKs with at least one corresponding algorithm+strength.
        final List<String> validAlgs = algs.stream().filter(alg -> (jwks.stream().anyMatch(jwk -> isMatch(jwk, alg)))).toList();
        final List<JWK> validJwks = jwks.stream().filter(jwk -> (algs.stream().anyMatch(alg -> isMatch(jwk, alg)))).toList();
        LOGGER.trace("Algorithms [" + algs + "]. Valid [" + validAlgs + "]. JWKs [" + jwks.size() + "]. Valid [" + validJwks.size() + "].");
        return new Tuple<>(validAlgs, validJwks);
    }

    static boolean isMatch(final JWK jwk, final String algorithm) {
        try {
            if ((JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_HMAC.contains(algorithm)) && (jwk instanceof OctetSequenceKey jwkHmac)) {
                return jwkHmac.size() >= MACSigner.getMinRequiredSecretLength(JWSAlgorithm.parse(algorithm));
            } else if ((JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_RSA.contains(algorithm)) && (jwk instanceof RSAKey jwkRsa)) {
                return JwkValidateUtil.computeBitLengthRsa(jwkRsa.toPublicKey()) >= RSAKeyGenerator.MIN_KEY_SIZE_BITS;
            } else if ((JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_EC.contains(algorithm)) && (jwk instanceof ECKey jwkEc)) {
                return Curve.forJWSAlgorithm(JWSAlgorithm.parse(algorithm)).contains(jwkEc.getCurve());
            }
        } catch (Exception e) {
            LOGGER.trace("Unexpected exception", e);
        }
        return false;
    }

    static int computeBitLengthRsa(final PublicKey publicKey) throws Exception {
        if (publicKey instanceof RSAPublicKey rsaPublicKey) {
            final int modulusBigLength = rsaPublicKey.getModulus().bitLength();
            return (modulusBigLength + 8 - 1) / 8 * 8;
        } else if (publicKey == null) {
            throw new Exception("Expected public key class [RSAPublicKey]. Got [" + "null" + "] instead.");
        }
        throw new Exception("Expected public key class [RSAPublicKey]. Got [" + publicKey.getClass().getSimpleName() + "] instead.");
    }

    // Remove JWKs if they have an optional kid and it does not match the filter
    static List<JWK> removeJwkKidMismatches(final List<JWK> jwks, final String kid) {
        return jwks.stream()
            .filter(j -> ((Strings.hasText(kid) == false) || (j.getKeyID() == null) || (kid.equals(j.getKeyID()))))
            .toList();
    }

    // Remove JWKs if they have an optional KeyUse and it does not match the filter
    static List<JWK> removeJwksKeyUseMismatches(final List<JWK> jwks, final KeyUse keyUse) {
        return jwks.stream().filter(j -> ((keyUse == null) || (j.getKeyUse() == null) || (keyUse.equals(j.getKeyUse())))).toList();
    }

    // Remove JWKs if they have an optional KeyOperation and it does not match the filter
    static List<JWK> removeJwksKeyOperationMismatches(final List<JWK> jwks, final KeyOperation ko) {
        return jwks.stream().filter(j -> ((ko == null) || (j.getKeyOperations() == null) || (j.getKeyOperations().contains(ko)))).toList();
    }

    // Remove JWKs if they have an optional Algorithm and it does not match the filter
    static List<JWK> removeJwksAlgMismatches(final List<JWK> jwks, final Algorithm algorithm) {
        return jwks.stream().filter(j -> (j.getAlgorithm() == null) || (algorithm.equals(j.getAlgorithm()))).toList();
    }

    // Remove JWKs if their key type or length/curve do not match the algorithm filter
    static List<JWK> removeJwkTypeOrStrengthMismatches(final List<JWK> jwks, final Algorithm algorithm) {
        return jwks.stream().filter(jwk -> JwkValidateUtil.isMatch(jwk, algorithm.getName())).toList();
    }

    static List<JWK> loadJwksFromJwkSetString(final String jwkSetConfigKey, final String jwkSetContents) throws SettingsException {
        if (Strings.hasText(jwkSetContents)) {
            try {
                return JWKSet.parse(jwkSetContents).getKeys();
            } catch (Exception e) {
                throw new SettingsException("JWKSet parse failed for setting [" + jwkSetConfigKey + "]", e);
            }
        }
        return Collections.emptyList();
    }
}
