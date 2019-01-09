/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support.jwt;

import org.elasticsearch.ElasticsearchSecurityException;

import java.security.Key;
import java.security.Signature;
import java.security.SignatureException;
import java.security.interfaces.ECPublicKey;

import static org.elasticsearch.xpack.security.authc.support.jwt.SignatureAlgorithm.getEcAlgorithms;

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
        if (getEcAlgorithms().contains(algorithm) == false) {
            throw new IllegalArgumentException("Unsupported algorithm " + algorithm.name() + " for ECDSA signature");
        }
        this.key = key;
        this.algorithm = algorithm;
    }

    /**
     * Return the expected ECDSA signature length (number of bytes) as described in
     * <a href="https://tools.ietf.org/html/rfc7518#section-3.4">the specification</a>
     *
     * @param algorithm the {@link SignatureAlgorithm} for which to get the expected signature length
     * @return the ECDSA signature length
     */
    private int getExpectedSignatureLength(SignatureAlgorithm algorithm) {
        switch (algorithm) {
            case ES256:
                return 64;
            case ES384:
                return 96;
            case ES512:
                return 132;
            default:
                throw new IllegalArgumentException("Unsupported algorithm " + algorithm.name() + " for ECDSA signature");
        }
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
            final Signature ecdsa = Signature.getInstance(algorithm.getJcaAlgoName());
            ecdsa.initVerify((ECPublicKey) key);
            ecdsa.update(data);
            ecdsa.verify(convertToDer(signature));
        } catch (Exception e) {
            throw new ElasticsearchSecurityException("Encountered error attempting to validate the JWT ECDSA Signature", e);
        }
    }

    /**
     * Converts the JOSE signature to DER so that it can be verified. See
     * <a href="https://tools.ietf.org/html/rfc7518#section-3.4">the specification</a>
     * Based on https://github.com/jwtk/jjwt/blob/1520ae8a21052b376282f8a38d310a91b15285e5/
     * impl/src/main/java/io/jsonwebtoken/impl/crypto/EllipticCurveProvider.java
     *
     * @param jwsSignature The signature as decoded from the JWT
     * @return the signature, DER encoded so that it can be used in {@link Signature#verify(byte[])}
     * @throws SignatureException if the signature is malformed
     */
    private byte[] convertToDer(byte[] jwsSignature) throws SignatureException {
        if (jwsSignature.length != getExpectedSignatureLength(algorithm)) {
            throw new SignatureException("Invalid ECDSA signature length");
        }
        int rawLen = jwsSignature.length / 2;

        // Remove any extra padding bytes from the end of R if any
        int i = rawLen;
        while ((i > 0) && (jwsSignature[rawLen - i] == 0)) {
            i--;
        }
        int rLength = i;
        if (jwsSignature[rawLen - i] < 0) {
            rLength += 1;
        }

        // Remove any extra padding bytes from the end of S if any
        int k = rawLen;
        while ((k > 0) && (jwsSignature[2 * rawLen - k] == 0)) {
            k--;
        }
        int sLength = k;
        if (jwsSignature[2 * rawLen - k] < 0) {
            sLength += 1;
        }

        int len = 2 + rLength + 2 + sLength;

        if (len > 255) {
            throw new SignatureException("Invalid ECDSA signature format");
        }

        int offset;

        final byte derSignature[];
        // Convert octet
        if (len < 128) {
            derSignature = new byte[2 + 2 + rLength + 2 + sLength];
            offset = 1;
        } else {
            derSignature = new byte[3 + 2 + rLength + 2 + sLength];
            derSignature[1] = (byte) 0x81;
            offset = 2;
        }

        derSignature[0] = 48;
        derSignature[offset++] = (byte) len;
        derSignature[offset++] = 2;
        derSignature[offset++] = (byte) rLength;
        //Copy R taking offset into account
        System.arraycopy(jwsSignature, rawLen - i, derSignature, (offset + rLength) - i, i);

        offset += rLength;

        derSignature[offset++] = 2;
        derSignature[offset++] = (byte) sLength;
        //Copy S taking offset into account
        System.arraycopy(jwsSignature, 2 * rawLen - k, derSignature, (offset + sLength) - k, k);

        return derSignature;
    }
}
