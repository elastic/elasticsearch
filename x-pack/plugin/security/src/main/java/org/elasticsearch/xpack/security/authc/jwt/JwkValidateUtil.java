/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

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
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;

import java.nio.charset.StandardCharsets;
import java.security.PublicKey;
import java.security.interfaces.RSAPublicKey;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.core.Strings.format;

/**
 * Utilities for JWK Validation.
 */
public class JwkValidateUtil {
    private static final Logger logger = LogManager.getLogger(JwkValidateUtil.class);

    // Static method for unit testing. No need to construct a complete RealmConfig with all settings.
    static JwkSetLoader.JwksAlgs filterJwksAndAlgorithms(final List<JWK> jwks, final List<String> algs) throws SettingsException {
        try (JwtUtil.TraceBuffer tracer = new JwtUtil.TraceBuffer(logger)) {
            tracer.append("Filtering [{}] JWKs for the following algorithms [{}].", jwks.size(), String.join(",", algs));

            final Predicate<JWK> keyUsePredicate = j -> ((j.getKeyUse() == null) || (KeyUse.SIGNATURE.equals(j.getKeyUse())));
            final List<JWK> jwksSig = jwks.stream().filter(keyUsePredicate).toList();
            tracer.append("[{}] remaining JWKs after KeyUse [SIGNATURE] filter.", jwksSig.size());

            final Predicate<JWK> keyOpPredicate = j -> ((j.getKeyOperations() == null)
                || (j.getKeyOperations().contains(KeyOperation.VERIFY)));
            final List<JWK> jwksVerify = jwksSig.stream().filter(keyOpPredicate).toList();
            tracer.append("[{}] remaining JWKs after KeyOperation [VERIFY] filter.", jwksVerify.size());

            final List<JWK> jwksFiltered = jwksVerify.stream().filter(j -> (algs.stream().anyMatch(a -> isMatch(j, a, tracer)))).toList();
            tracer.append("[{}] remaining JWKs after algorithms name filter.", jwksFiltered.size());

            final List<String> algsFiltered = algs.stream()
                .filter(a -> (jwksFiltered.stream().anyMatch(j -> isMatch(j, a, tracer))))
                .toList();
            tracer.append(
                "[{}] remaining JWKs after configured algorithms [{}] filter.",
                jwksFiltered.size(),
                String.join(",", algsFiltered)
            );

            return new JwkSetLoader.JwksAlgs(jwksFiltered, algsFiltered);
        }
    }

    /**
     * Verify JWK type matches algorithm requirement. EX: HS256 needs OctetSequenceKey, RS256/PS256 needs RSAKey, ES256 needs ECKey.
     * Verify JWK strength matches algorithm requirement. EX: HS384 needs min 384-bit, RSA needs min 2048-bit, ES256 needs secp256r1 curve.
     *
     * @param jwk JWK object of class OctetSequenceKey, RSAKey, or ECKey.
     * @param algorithm Algorithm string of value HS256, HS384, HS512, RS256, RS384, RS512, PS256, PS384, PS512, ES256, ES384, or ES512.
     * @return True if JWT type and strength match both requirements for the signature algorithm.
     */
    static boolean isMatch(final JWK jwk, final String algorithm, JwtUtil.TraceBuffer tracer) {
        try {
            if ((JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_HMAC.contains(algorithm)) && (jwk instanceof OctetSequenceKey jwkHmac)) {
                final int bits = jwkHmac.size();
                final int min = MACSigner.getMinRequiredSecretLength(JWSAlgorithm.parse(algorithm));
                final boolean isMatch = bits >= min;
                if (isMatch == false) {
                    tracer.append("HMAC JWK [" + bits + "] bits too small for algorithm [" + algorithm + "] minimum [" + min + "].");
                }
                return isMatch;
            } else if ((JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_RSA.contains(algorithm)) && (jwk instanceof RSAKey jwkRsa)) {
                final int bits = JwkValidateUtil.computeBitLengthRsa(jwkRsa.toPublicKey());
                final int min = RSAKeyGenerator.MIN_KEY_SIZE_BITS;
                final boolean isMatch = bits >= min;
                if (isMatch == false) {
                    tracer.append("RSA JWK [" + bits + "] bits too small for algorithm [" + algorithm + "] minimum [" + min + "].");
                }
                return isMatch;
            } else if ((JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_EC.contains(algorithm)) && (jwk instanceof ECKey jwkEc)) {
                final Curve curve = jwkEc.getCurve();
                final Set<Curve> allowed = Curve.forJWSAlgorithm(JWSAlgorithm.parse(algorithm));
                final boolean isMatch = allowed.contains(curve);
                if (isMatch == false) {
                    tracer.append("EC JWK [" + curve + "] curve not allowed for algorithm [" + algorithm + "] allowed " + allowed + ".");
                }
                return isMatch;
            }
        } catch (Exception e) {
            Supplier<String> message = () -> format(
                "Unexpected exception while matching JWK with kid [%s] to it's algorithm requirement.",
                jwk.getKeyID()
            );
            if (logger.isTraceEnabled()) {
                logger.trace(message.get(), e);
            } else {
                logger.debug(message.get());
            }
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

    static List<JWK> loadJwksFromJwkSetString(final String jwkSetConfigKey, final CharSequence jwkSetContents) throws SettingsException {
        if (Strings.hasText(jwkSetContents)) {
            try {
                return JWKSet.parse(jwkSetContents.toString()).getKeys();
            } catch (Exception e) {
                throw new SettingsException("JWKSet parse failed for setting [" + jwkSetConfigKey + "]", e);
            }
        }
        return Collections.emptyList();
    }

    static OctetSequenceKey loadHmacJwkFromJwkString(final String jwkSetConfigKey, final CharSequence hmacKeyContents) {
        if (Strings.hasText(hmacKeyContents)) {
            try {
                return buildHmacKeyFromString(hmacKeyContents);
            } catch (Exception e) {
                throw new SettingsException("HMAC Key parse failed for setting [" + jwkSetConfigKey + "]", e);
            }
        }
        return null;
    }

    static OctetSequenceKey buildHmacKeyFromString(CharSequence hmacKeyContents) {
        final String hmacKeyString = hmacKeyContents.toString();
        final byte[] utf8Bytes = hmacKeyString.getBytes(StandardCharsets.UTF_8); // OIDC spec: UTF8 encoding of HMAC keys
        // Note: JWK has no attributes (kid, alg, use, ops)
        return new OctetSequenceKey.Builder(utf8Bytes).build();
    }
}
