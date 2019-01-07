/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support.jwt;

import org.elasticsearch.ElasticsearchSecurityException;

import java.security.Key;
import java.security.Signature;
import java.security.interfaces.ECPublicKey;
import java.util.Base64;

/**
 * Class offering necessary functionality for validating the signatures of JWTs that have been signed with the
 * Elliptic Curve Digital Signature Algorithm (ECDSA) using a EC Private Key
 */
public class EcSignatureValidator implements JwtSignatureValidator {
    private SignatureAlgorithm algorithm;
    private Key key;

    public EcSignatureValidator(SignatureAlgorithm algorithm, Key key) {
        if (key instanceof ECPublicKey == false) {
            throw new IllegalArgumentException("ECDSA signatures can only be verified using an ECPublicKey " +
                "but a [" + key.getClass().getName() + "] is provided");
        }
        if (SignatureAlgorithm.getEcAlgorithms().contains(algorithm) == false) {
            throw new IllegalArgumentException("Unsupported algorithm " + algorithm.name() + " for ECDSA signature");
        }
        this.key = key;
        this.algorithm = algorithm;
    }

    /**
     * Validates the signature of a signed JWT using the EC Public Key that corresponds to the EC Private Key
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
            final Signature ecdsa = Signature.getInstance(algorithm.getJcaAlgoName());
            ecdsa.initVerify((ECPublicKey) key);
            ecdsa.update(data);
            ecdsa.verify(signatureBytes);
        } catch (Exception e) {
            throw new ElasticsearchSecurityException("Encountered error attempting to validate the JWT ECDSA Signature", e);
        }
    }

}
