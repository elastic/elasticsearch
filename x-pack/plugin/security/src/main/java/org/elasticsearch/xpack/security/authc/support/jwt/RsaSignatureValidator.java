/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support.jwt;

import org.elasticsearch.ElasticsearchSecurityException;

import java.security.Key;
import java.security.PublicKey;
import java.security.Signature;
import java.util.Base64;

/**
 * Class offering necessary functionality for validating the signatures of JWTs that have been signed with
 * RSASSA-PKCS1-v1_5 (PKCS#1) using an RSA Private Key
 */
public class RsaSignatureValidator implements JwtSignatureValidator {
    private SignatureAlgorithm algorithm;
    private Key key;

    public RsaSignatureValidator(SignatureAlgorithm algorithm, Key key) {
        if (key instanceof PublicKey == false) {
            throw new IllegalArgumentException("RSA signatures can only be verified using a PublicKey " +
                "but a [" + key.getClass().getName() + "] is provided");
        }
        if (SignatureAlgorithm.getRsaAlgorithms().contains(algorithm) == false) {
            throw new IllegalArgumentException("Unsupported algorithm " + algorithm.name() + " for RSA signature");
        }
        this.key = key;
        this.algorithm = algorithm;
    }

    /**
     * Validates the signature of a signed JWT using the Public Key that corresponds to the Private Key
     * with which it was signed
     *
     * @param data      The serialized representation of the JWT payload
     * @param signature The serialized representation of the JWT signature
     */
    @Override
    public void validateSignature(byte[] data, byte[] signature) {
        if (null == data || data.length == 0) {
            throw new IllegalArgumentException("JWT data must be provided");
        }
        if (null == signature || signature.length == 0) {
            throw new IllegalArgumentException("JWT signature must be provided");
        }

        try {
            final byte[] signatureBytes = Base64.getUrlDecoder().decode(signature);
            final Signature rsa = Signature.getInstance(algorithm.getJcaAlgoName());
            rsa.initVerify((PublicKey) key);
            rsa.update(data);
            rsa.verify(signatureBytes);
        } catch (Exception e) {
            throw new ElasticsearchSecurityException("Encountered error attempting to validate the JWT RSA Signature", e);
        }

    }
}
