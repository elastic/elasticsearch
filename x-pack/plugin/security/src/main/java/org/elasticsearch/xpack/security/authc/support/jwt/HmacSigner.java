/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support.jwt;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.security.GeneralSecurityException;
import java.security.Key;

/**
 * Class offering necessary functionality for generating an HMAC for the JWT using a secret key.
 */
public class HmacSigner implements JwtSigner {

    private Key key;
    private SignatureAlgorithm algorithm;

    public HmacSigner(SignatureAlgorithm algorithm, Key key) {
        if (key instanceof SecretKey == false) {
            throw new IllegalArgumentException("HMAC signatures can only be created using a SecretKey but a [" + key.getClass().getName() +
                "] is provided");
        }
        if (SignatureAlgorithm.getHmacAlgorithms().contains(algorithm) == false) {
            throw new IllegalArgumentException("Unsupported algorithm " + algorithm.name() + " for HMAC signature");
        }
        this.key = key;
        this.algorithm = algorithm;

    }

    /**
     * Generates the HMAC of the JWT
     *
     * @param data the data to be signed
     * @return the HMAC as a byte array
     * @throws GeneralSecurityException if any error was encountered generating the HMAC
     */
    @Override
    public byte[] sign(byte[] data) throws GeneralSecurityException {
        if (null == data || data.length == 0) {
            throw new IllegalArgumentException("JWT data must be provided");
        }


        final SecretKeySpec keySpec = new SecretKeySpec(key.getEncoded(), algorithm.getJcaAlgoName());
        final Mac mac = Mac.getInstance(algorithm.getJcaAlgoName());
        mac.init(keySpec);
        return mac.doFinal(data);
    }
}
