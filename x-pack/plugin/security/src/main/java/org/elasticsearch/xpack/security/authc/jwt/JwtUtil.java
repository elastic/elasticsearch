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
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.OctetSequenceKey;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.util.ArrayUtils;
import com.nimbusds.jose.util.JSONObjectUtils;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PublicKey;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.crypto.Cipher;

/**
 * Utilities for JWT JWS create, sign, and verify, as well as generate JWT JWS credential.
 */
public class JwtUtil {

    // Expect all JWSAlgorithm values to work in Family.HMAC_SHA, Family.RSA, and Family.EC (except ES256K)
    // Non-FIPS: Expect ES256K to fail because it is deprecated and disabled by default in Java 11.0.9/11.0.10 and 17
    // FIPS mode: Expect ES256K to succeed because it is still supported by BC-FIPS; not deprecated or disabled yet
    public static final JWSAlgorithm.Family EC_NON_FIPS = new JWSAlgorithm.Family(
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

    public static Tuple<JWSSigner, JWSVerifier> createJwsSignerJwsVerifierForRsa(final RSAKey jwk) throws JOSEException {
        return new Tuple<>(new RSASSASigner(jwk), new RSASSAVerifier(jwk));
    }

    public static Tuple<JWSSigner, JWSVerifier> createJwsSignerJwsVerifierForEc(final ECKey jwk) throws JOSEException {
        return new Tuple<>(new ECDSASigner(jwk), new ECDSAVerifier(jwk));
    }

    public static Tuple<JWSSigner, JWSVerifier> createJwsSignerJwsVerifierForHmac(final OctetSequenceKey jwk) throws JOSEException {
        return new Tuple<>(new MACSigner(jwk), new MACVerifier(jwk));
    }

    public static JWKSet createJwk(final JWK... jwk) {
        return new JWKSet(List.of(jwk));
    }

    public static void saveJwkSet(final Path path, final JWKSet jwkSet, final boolean publicKeysOnly) throws IOException {
        Files.writeString(path, JwtUtil.serializeJwkSet(jwkSet, publicKeysOnly));
    }

    public static String serializeJwkSet(final JWKSet jwkSet, final boolean publicKeysOnly) {
        return JSONObjectUtils.toJSONString(jwkSet.toJSONObject(publicKeysOnly));
    }

    public static JWKSet loadJwkSet(final Path path) throws IOException, ParseException {
        try (InputStream is = Files.newInputStream(path)) {
            return JWKSet.load(is);
        }
    }

    public static JWKSet loadJwkSet(final URL url, int connectTimeoutMillis, int readTimeoutMillis, int sizeLimitBytes) throws IOException,
        ParseException {
        return JWKSet.load(url, connectTimeoutMillis, readTimeoutMillis, sizeLimitBytes);
    }

    public static SecureString getHeaderSchemeParameters(
        final ThreadContext threadContext,
        final String headerName,
        final String schemeValue,
        final boolean ignoreCase
    ) {
        final String headerValue = threadContext.getHeader(headerName);
        if (Strings.hasText(headerValue)) {
            final String schemeValuePlusSpace = schemeValue + " ";
            if (headerValue.regionMatches(ignoreCase, 0, schemeValuePlusSpace, 0, schemeValuePlusSpace.length())) {
                final String trimmedSchemeParameters = headerValue.substring(schemeValuePlusSpace.length()).trim();
                if (Strings.hasText(trimmedSchemeParameters)) {
                    return new SecureString(trimmedSchemeParameters.toCharArray());
                }
            }
        }
        return null;
    }

    public static int computeBitLengthRsa(final PublicKey publicKey) throws Exception {
        final Cipher cipher = Cipher.getInstance("RSA");
        cipher.init(Cipher.ENCRYPT_MODE, publicKey);
        return cipher.getOutputSize(0) * Byte.SIZE;
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

    public static int computeMinBitLengthRsa(final List<String> allowedSignatureAlgorithmsRsa) {
        return allowedSignatureAlgorithmsRsa.isEmpty() ? Integer.MAX_VALUE : 2048;
    }

    public static Set<Curve> computeAllowedCurvesEc(final List<String> allowedSignatureAlgorithmsHmac) {
        final Set<Curve> allowedCurvesEc = new HashSet<>(); // Computed below
        for (final String signatureAlgorithmEc : allowedSignatureAlgorithmsHmac) {
            allowedCurvesEc.addAll(Curve.forJWSAlgorithm(JWSAlgorithm.parse(signatureAlgorithmEc)));
        }
        return allowedCurvesEc;
    }

    // Static method for unit testing. No need to construct a complete RealmConfig with all settings.
    public static void validateClientAuthorizationSettings(
        final String clientAuthorizationTypeConfigKey,
        final String clientAuthorizationType,
        final String clientAuthorizationSharedSecretConfigKey,
        final SecureString clientAuthorizationSharedSecret
    ) throws SettingsException {
        switch (clientAuthorizationType) {
            case JwtRealmSettings.HEADER_CLIENT_AUTHORIZATION_TYPE_SHARED_SECRET:
                // If type is "SharedSecret", the shared secret value must be set
                if (Strings.hasText(clientAuthorizationSharedSecret) == false) {
                    throw new SettingsException(
                        "Missing setting for ["
                            + clientAuthorizationSharedSecretConfigKey
                            + "]. It is required when setting ["
                            + clientAuthorizationTypeConfigKey
                            + "] is ["
                            + JwtRealmSettings.HEADER_CLIENT_AUTHORIZATION_TYPE_SHARED_SECRET
                            + "]"
                    );
                }
                break;
            case JwtRealmSettings.HEADER_CLIENT_AUTHORIZATION_TYPE_NONE:
            default:
                // If type is "None", the shared secret value must not be set
                if (Strings.hasText(clientAuthorizationSharedSecret)) {
                    throw new SettingsException(
                        "Setting ["
                            + clientAuthorizationSharedSecretConfigKey
                            + "] is not supported, because setting ["
                            + clientAuthorizationTypeConfigKey
                            + "] is ["
                            + JwtRealmSettings.HEADER_CLIENT_AUTHORIZATION_TYPE_NONE
                            + "]"
                    );
                }
                break;
        }
    }

    // Static method for unit testing. No need to construct a complete RealmConfig with all settings.
    public static Tuple<JWKSet, JWKSet> validateJwkSets(
        final String allowedSignatureAlgorithmsConfigKey,
        final List<String> allowedSignatureAlgorithms,
        final String jwkSetConfigKeyHmac,
        final String jwkSetContentsHmac,
        final String jwkSetConfigKeyPkc,
        final String jwkSetContentsPkc
    ) throws SettingsException {
        final boolean isConfiguredJwkSetHmac = Strings.hasText(jwkSetContentsHmac);
        final boolean isConfiguredJwkSetPkc = Strings.hasText(jwkSetContentsPkc);
        final List<String> allowedSignatureAlgorithmsHmac = filter(
            allowedSignatureAlgorithms,
            JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_HMAC
        );
        final List<String> allowedSignatureAlgorithmsPkc = filter(
            allowedSignatureAlgorithms,
            JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_PKC
        );

        if ((isConfiguredJwkSetHmac == false) && (isConfiguredJwkSetPkc == false)) {
            // HMAC JWKSet and RSA/EC JWKSet both missing
            throw new SettingsException(
                "Missing setting(s). At least one JWKSet must be set in [" + jwkSetConfigKeyHmac + "] or [" + jwkSetConfigKeyPkc + "]"
            );
        } else {
            if (isConfiguredJwkSetHmac && (allowedSignatureAlgorithmsHmac.isEmpty())) {
                // HMAC JWKSet present without any HMAC signature algorithms
                throw new SettingsException(
                    "Settings mismatch. HMAC JWKSet is set in ["
                        + jwkSetConfigKeyHmac
                        + "], but no HMAC signature algorithms are set in ["
                        + allowedSignatureAlgorithmsConfigKey
                        + "]"
                );
            } else if ((allowedSignatureAlgorithmsHmac.isEmpty() == false) && (isConfiguredJwkSetHmac == false)) {
                // HMAC signature algorithms present without a HMAC JWKSet
                throw new SettingsException(
                    "Settings mismatch. HMAC signature algorithms are set in ["
                        + allowedSignatureAlgorithmsConfigKey
                        + "], but no HMAC JWKSet is set in ["
                        + jwkSetConfigKeyHmac
                        + "]"
                );
            } else if (isConfiguredJwkSetPkc && (allowedSignatureAlgorithmsPkc.isEmpty())) {
                // RSA/EC JWKSet present without any RSA/EC signature algorithms
                throw new SettingsException(
                    "Settings mismatch. RSA/EC JWKSet is set in ["
                        + jwkSetConfigKeyPkc
                        + "], but no RSA/EC signature algorithms are set in ["
                        + allowedSignatureAlgorithmsConfigKey
                        + "]"
                );
            } else if ((allowedSignatureAlgorithmsPkc.isEmpty() == false) && (isConfiguredJwkSetPkc == false)) {
                // RSA/EC signature algorithms present without an RSA/EC JWKSet
                throw new SettingsException(
                    "Settings mismatch. RSA/EC signature algorithms are set in ["
                        + allowedSignatureAlgorithmsConfigKey
                        + "], but no RSA/EC JWKSet is set in ["
                        + jwkSetConfigKeyPkc
                        + "]"
                );
            }
        }

        // At this point, one or both JWKSets are configured. Load and validate their JWK contents against each set of signature algorithms.
        JWKSet jwkSetHmac = null;
        if (isConfiguredJwkSetHmac) {
            jwkSetHmac = JwtUtil.loadJwkSetFromString(jwkSetConfigKeyHmac, jwkSetContentsHmac);
            JwtUtil.validateJwkSetContents(jwkSetConfigKeyHmac, jwkSetHmac, allowedSignatureAlgorithmsHmac);
        }
        JWKSet jwkSetPkc = null;
        if (isConfiguredJwkSetPkc) {
            jwkSetPkc = JwtUtil.loadJwkSetFromString(jwkSetConfigKeyPkc, jwkSetContentsPkc);
            JwtUtil.validateJwkSetContents(jwkSetConfigKeyPkc, jwkSetPkc, allowedSignatureAlgorithmsPkc);
        }
        return new Tuple<>(jwkSetHmac, jwkSetPkc);
    }

    public static byte[] readUrlContents(
        final String jwkSetConfigKeyPkc,
        final URI jwkSetPathPkcUri,
        final CloseableHttpAsyncClient httpClient
    ) throws SettingsException {
        if ((jwkSetPathPkcUri == null) || (Strings.hasText(jwkSetPathPkcUri.toString()) == false)) {
            return null;
        }
        try {
            if (jwkSetPathPkcUri.getScheme().equalsIgnoreCase("https") == false) {
                throw new SettingsException("URI [" + jwkSetPathPkcUri + "] not allowed. Only HTTPS is supported.");
            }
            return JwtRealm.readBytes(httpClient, jwkSetPathPkcUri, Integer.MAX_VALUE);
        } catch (SettingsException e) {
            throw e; // rethrow
        } catch (Exception e) {
            throw new SettingsException(
                "Failed to read contents for setting [" + jwkSetConfigKeyPkc + "] value [" + jwkSetPathPkcUri + "].",
                e
            );
        }
    }

    public static byte[] readFileContents(final String jwkSetConfigKeyPkc, final String jwkSetPathPkc, final Environment environment)
        throws SettingsException {
        if (Strings.hasText(jwkSetPathPkc) == false) {
            return null;
        }
        try {
            final Path path = JwtUtil.resolvePath(environment, jwkSetPathPkc);
            return Files.readAllBytes(path);
        } catch (SettingsException e) {
            throw e; // rethrow
        } catch (Exception e) {
            throw new SettingsException(
                "Failed to read contents for setting [" + jwkSetConfigKeyPkc + "] value [" + jwkSetPathPkc + "].",
                e
            );
        }
    }

    public static JWKSet loadJwkSetFromString(final String jwkSetConfigKey, final String jwkSetContents) throws SettingsException {
        try {
            return JWKSet.parse(jwkSetContents);
        } catch (Exception e) {
            throw new SettingsException("JWKSet parse failed for setting [" + jwkSetConfigKey + "]", e);
        }
    }

    public static JWKSet validateJwkSetContents(
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
                validateJwkHmac(jwkSetConfigKey, jwkHmac, allowedSignatureAlgorithms);
            } else if (jwk instanceof RSAKey jwkRsa) {
                validateJwkRsa(jwkSetConfigKey, jwkRsa, allowedSignatureAlgorithms);
            } else if (jwk instanceof ECKey jwkEc) {
                validateJwkEc(jwkSetConfigKey, jwkEc, allowedSignatureAlgorithms);
            } else {
                throw new SettingsException(
                    "JWKSet [" + jwkSetConfigKey + " contains invalid JWK of class [" + jwk.getClass().getCanonicalName() + "]."
                );
            }
        }
        return jwkSet;
    }

    public static void validateJwkHmac(
        final String configKeyJwkSetHmac,
        final OctetSequenceKey jwkHmac,
        final List<String> allowedSignatureAlgorithms
    ) {
        final List<String> allowedSignatureAlgorithmsHmac = filter(
            allowedSignatureAlgorithms,
            JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_HMAC
        );
        final int minBitLengthHmac = JwtUtil.computeMinBitLengthHmac(allowedSignatureAlgorithmsHmac);
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

    public static void validateJwkRsa(
        final String configKeyJwkSetHmac,
        final RSAKey jwkRsa,
        final List<String> allowedSignatureAlgorithms
    ) {
        final List<String> allowedSignatureAlgorithmsRsa = filter(
            allowedSignatureAlgorithms,
            JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_RSA
        );
        final int minBitLengthRsa = JwtUtil.computeMinBitLengthRsa(allowedSignatureAlgorithmsRsa);
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
            bitLengthRsa = JwtUtil.computeBitLengthRsa(jwkRsa.toPublicKey());
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

    public static void validateJwkEc(final String configKeyJwkSetHmac, final ECKey jwkEc, final List<String> allowedSignatureAlgorithms) {
        final List<String> allowedSignatureAlgorithmsEc = filter(
            allowedSignatureAlgorithms,
            JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_EC
        );
        final Set<Curve> allowedEcCurves = JwtUtil.computeAllowedCurvesEc(allowedSignatureAlgorithmsEc);
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

    public static List<String> filter(final List<String> input, final List<String> filter) {
        return filter.stream().filter(input::contains).collect(Collectors.toList());
    }

    public static URI parseUriNoException(final String uri) {
        try {
            return new URI(uri);
        } catch (Exception e) {
            return null;
        }
    }

    public static Path resolvePath(final Environment environment, final String jwkSetPath) {
        final Path directoryPath = environment.configFile();
        final Path filePath = directoryPath.resolve(jwkSetPath);
        return filePath;
    }

    public static void validateJwtAuthTime(final long allowedClockSkewSeconds, final Date now, final Date auth_time) throws Exception {
        if (allowedClockSkewSeconds < 0L) {
            throw new Exception("Invalid allowedClockSkewSeconds [" + allowedClockSkewSeconds + "] < 0.");
        } else if (auth_time == null) {
            return;
        }
        // skewSec=0 auth_time=3:00:00.000 now=2:59:59.999 --> fail
        // skewSec=0 auth_time=3:00:00.000 now=3:00:00.000 --> pass
        // skewSec=1 auth_time=3:00:00.000 now=2:59:59.999 --> pass (subtract skew from auth_time)
        if ((auth_time.getTime() - (allowedClockSkewSeconds * 1000L)) > now.getTime()) {
            throw new Exception(
                "Invalid auth_time ["
                    + auth_time.getTime()
                    + "ms/"
                    + auth_time
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

    public static void validateJwtIssuedAtTime(final long allowedClockSkewSeconds, final Date now, final Date iat) throws Exception {
        if (allowedClockSkewSeconds < 0L) {
            throw new Exception("Invalid allowedClockSkewSeconds [" + allowedClockSkewSeconds + "] < 0.");
        } else if (iat == null) {
            throw new Exception("Invalid iat [null].");
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

    public static void validateJwtNotBeforeTime(final long allowedClockSkewSeconds, final Date now, final Date nbf) throws Exception {
        if (allowedClockSkewSeconds < 0L) {
            throw new Exception("Invalid allowedClockSkewSeconds [" + allowedClockSkewSeconds + "] < 0.");
        } else if (nbf == null) {
            return;
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

    // Time claims: auth_time <= iat <= nbf <= NOW < exp
    public static void validateJwtExpiredTime(final long allowedClockSkewSeconds, final Date now, final Date exp) throws Exception {
        if (allowedClockSkewSeconds < 0L) {
            throw new Exception("Invalid allowedClockSkewSeconds [" + allowedClockSkewSeconds + "] < 0.");
        } else if (exp == null) {
            throw new Exception("Invalid exp [null].");
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

    public static List<JWSAlgorithm> toJwsAlgorithms(final List<String> signatureAlgorithms) throws JOSEException {
        final List<JWSAlgorithm> list = new ArrayList<>(signatureAlgorithms.size());
        for (final String signatureAlgorithm : signatureAlgorithms) {
            list.add(JWSAlgorithm.parse(signatureAlgorithm));
        }
        return list;
    }

    public static SecureString join(final String join, final SecureString... secureStrings) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < secureStrings.length; i++) {
            if (i != 0) {
                sb.append(join);
            }
            sb.append(secureStrings[i]);
        }
        return new SecureString(sb.toString().toCharArray());
    }
}
