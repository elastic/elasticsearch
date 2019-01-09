/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support.jwt;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Base64;

/**
 * Class offering necessary functionality for validating the signatures of JWTs that have been signed with a
 * Hash-based Message Authentication Code using a secret key.
 */
public class HmacSignatureValidator implements JwtSignatureValidator {
    protected final Logger logger = LogManager.getLogger(getClass());
    private Key key;
    private SignatureAlgorithm algorithm;

    public HmacSignatureValidator(SignatureAlgorithm algorithm, Key key) {
        if (key instanceof SecretKey == false) {
            throw new IllegalArgumentException("HMAC signatures can only be verified using a SecretKey but a [" + key.getClass().getName() + "] is provided");
        }
        if (SignatureAlgorithm.getHmacAlgorithms().contains(algorithm) == false) {
            throw new IllegalArgumentException("Unsupported algorithm " + algorithm.name() + " for HMAC signature");
        }
        this.key = key;
        this.algorithm = algorithm;

    }

    /**
     * Validates the signature of a signed JWT by generating the signature using the provided key and verifying that
     * it matches the provided signature.
     *
     * @param data              The JWT payload
     * @param expectedSignature The JWT signature
     * @return True if the newly calculated signature of the header and payload matches the one that was included in the JWT, false
     * otherwise
     */
    @Override
    public void validateSignature(byte[] data, byte[] expectedSignature) {
        if (null == data || data.length == 0) {
            throw new IllegalArgumentException("JWT data must be provided");
        }
        if (null == expectedSignature || expectedSignature.length == 0) {
            throw new IllegalArgumentException("JWT signature must be provided");
        }

        try {
            final SecretKeySpec keySpec = new SecretKeySpec(key.getEncoded(), algorithm.getJcaAlgoName());
            final Mac mac = Mac.getInstance(algorithm.getJcaAlgoName());
            mac.init(keySpec);
            final byte[] calculatedSignature = mac.doFinal(data);
            if (MessageDigest.isEqual(calculatedSignature, expectedSignature) == false) {
                throw new ElasticsearchSecurityException("JWT HMAC Signature could not be validated. Calculated value was [{}] but the " +
                    "expected one was [{}]",
                    Base64.getUrlEncoder().withoutPadding().encodeToString(calculatedSignature),
                    Base64.getUrlEncoder().withoutPadding().encodeToString(expectedSignature));
            }
        } catch (Exception e) {
            throw new ElasticsearchSecurityException("Encountered error attempting to validate the JWT HMAC Signature", e);
        }
    }
}
