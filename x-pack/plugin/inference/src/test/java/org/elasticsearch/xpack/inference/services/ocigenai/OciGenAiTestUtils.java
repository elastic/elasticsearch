/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.ByteArrayOutputStream;
import java.math.BigInteger;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.interfaces.RSAPrivateCrtKey;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;

/**
 * Shared fixtures for the OCI Generative AI tests: a generated RSA API signing key pair, PEM encoders and settings builders.
 */
public final class OciGenAiTestUtils {

    public static final String TENANCY_ID = "ocid1.tenancy.oc1..aaaaaaaatenancy";
    public static final String USER_ID = "ocid1.user.oc1..aaaaaaaauser";
    public static final String FINGERPRINT = "20:3b:97:13:55:1c:5b:0d:d3:37:d8:50:4e:c5:3a:34";
    public static final String COMPARTMENT_ID = "ocid1.compartment.oc1..aaaaaaaacompartment";
    public static final String ENDPOINT_ID_VALUE = "ocid1.generativeaiendpoint.oc1.us-chicago-1.aaaaaaaaendpoint";
    public static final String REGION_VALUE = "us-chicago-1";
    public static final String AUTH_HEADER_VALUE = "Signature version=\"1\",keyId=\"test\"";

    private static volatile KeyPair keyPair;

    /**
     * A 2048 bit RSA key pair generated once per JVM (key generation is comparatively slow).
     */
    public static KeyPair keyPair() {
        if (keyPair == null) {
            synchronized (OciGenAiTestUtils.class) {
                if (keyPair == null) {
                    keyPair = generateKeyPair();
                }
            }
        }
        return keyPair;
    }

    public static KeyPair generateKeyPair() {
        try {
            var generator = KeyPairGenerator.getInstance("RSA");
            generator.initialize(2048);
            return generator.generateKeyPair();
        } catch (GeneralSecurityException e) {
            throw new AssertionError(e);
        }
    }

    public static String privateKeyPem() {
        return toPkcs8Pem(keyPair().getPrivate());
    }

    public static String toPkcs8Pem(PrivateKey privateKey) {
        return "-----BEGIN PRIVATE KEY-----\n"
            + Base64.getMimeEncoder(64, new byte[] { '\n' }).encodeToString(privateKey.getEncoded())
            + "\n-----END PRIVATE KEY-----\n";
    }

    /**
     * Encodes the key in the legacy PKCS#1 {@code RSAPrivateKey} PEM format (the format produced by {@code openssl genrsa} with
     * OpenSSL 1.x).
     */
    public static String toPkcs1Pem(RSAPrivateCrtKey key) {
        var content = new ByteArrayOutputStream();
        content.writeBytes(derInteger(BigInteger.ZERO));
        content.writeBytes(derInteger(key.getModulus()));
        content.writeBytes(derInteger(key.getPublicExponent()));
        content.writeBytes(derInteger(key.getPrivateExponent()));
        content.writeBytes(derInteger(key.getPrimeP()));
        content.writeBytes(derInteger(key.getPrimeQ()));
        content.writeBytes(derInteger(key.getPrimeExponentP()));
        content.writeBytes(derInteger(key.getPrimeExponentQ()));
        content.writeBytes(derInteger(key.getCrtCoefficient()));
        var der = derElement(0x30, content.toByteArray());
        return "-----BEGIN RSA PRIVATE KEY-----\n"
            + Base64.getMimeEncoder(64, new byte[] { '\n' }).encodeToString(der)
            + "\n-----END RSA PRIVATE KEY-----\n";
    }

    private static byte[] derInteger(BigInteger value) {
        return derElement(0x02, value.toByteArray());
    }

    private static byte[] derElement(int tag, byte[] content) {
        var out = new ByteArrayOutputStream();
        out.write(tag);
        if (content.length < 0x80) {
            out.write(content.length);
        } else {
            var lengthBytes = BigInteger.valueOf(content.length).toByteArray();
            if (lengthBytes[0] == 0) {
                lengthBytes = java.util.Arrays.copyOfRange(lengthBytes, 1, lengthBytes.length);
            }
            out.write(0x80 | lengthBytes.length);
            out.writeBytes(lengthBytes);
        }
        out.writeBytes(content);
        return out.toByteArray();
    }

    public static OciGenAiSecretSettings createSecretSettings() {
        return createSecretSettings(privateKeyPem());
    }

    public static OciGenAiSecretSettings createSecretSettings(String privateKeyPem) {
        return new OciGenAiSecretSettings(
            new SecureString(TENANCY_ID.toCharArray()),
            new SecureString(USER_ID.toCharArray()),
            new SecureString(FINGERPRINT.toCharArray()),
            new SecureString(privateKeyPem.toCharArray())
        );
    }

    public static String keyId() {
        return TENANCY_ID + "/" + USER_ID + "/" + FINGERPRINT;
    }

    public static Map<String, Object> secretSettingsMap() {
        return secretSettingsMap(privateKeyPem());
    }

    public static Map<String, Object> secretSettingsMap(String privateKeyPem) {
        return new HashMap<>(
            Map.of(
                OciGenAiSecretSettings.TENANCY_ID,
                TENANCY_ID,
                OciGenAiSecretSettings.USER_ID,
                USER_ID,
                OciGenAiSecretSettings.FINGERPRINT,
                FINGERPRINT,
                OciGenAiSecretSettings.PRIVATE_KEY,
                privateKeyPem
            )
        );
    }

    public static Map<String, Object> serviceSettingsMap(String modelId) {
        return serviceSettingsMap(REGION_VALUE, COMPARTMENT_ID, modelId, null, null);
    }

    public static Map<String, Object> serviceSettingsMap(
        @Nullable String region,
        String compartmentId,
        String modelId,
        @Nullable String endpointId,
        @Nullable String url
    ) {
        var map = new HashMap<String, Object>();
        if (region != null) {
            map.put(OciGenAiServiceFields.REGION, region);
        }
        map.put(OciGenAiServiceFields.COMPARTMENT_ID, compartmentId);
        map.put(MODEL_ID, modelId);
        if (endpointId != null) {
            map.put(OciGenAiServiceFields.ENDPOINT_ID, endpointId);
        }
        if (url != null) {
            map.put(URL, url);
        }
        return map;
    }

    public static OciGenAiServiceSettings.CommonSettings commonSettings(String modelId) {
        return commonSettings(REGION_VALUE, COMPARTMENT_ID, modelId, null, null, null);
    }

    public static OciGenAiServiceSettings.CommonSettings commonSettings(
        @Nullable String region,
        String compartmentId,
        String modelId,
        @Nullable String endpointId,
        @Nullable URI uri,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        return new OciGenAiServiceSettings.CommonSettings(region, compartmentId, modelId, endpointId, uri, rateLimitSettings);
    }

    /**
     * A request signer replacement that sets a fixed {@code Authorization} header instead of computing a real OCI signature.
     */
    public static BiConsumer<HttpPost, OciGenAiModel> fixedAuthHeader(String value) {
        return (httpPost, model) -> httpPost.setHeader(HttpHeaders.AUTHORIZATION, value);
    }

    public static BiConsumer<HttpPost, OciGenAiModel> fixedAuthHeader() {
        return fixedAuthHeader(AUTH_HEADER_VALUE);
    }

    private OciGenAiTestUtils() {}
}
