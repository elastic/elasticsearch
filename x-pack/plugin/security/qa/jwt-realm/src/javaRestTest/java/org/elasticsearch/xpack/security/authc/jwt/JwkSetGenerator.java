/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.KeyOperation;
import com.nimbusds.jose.jwk.KeyUse;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;

import java.util.Set;

public class JwkSetGenerator {

    public static void main(String[] args) throws JOSEException {
        final RSAKey key = getRsaKey("RS256", 2048);
        final JWKSet set = new JWKSet(key);
        System.out.println("===============");
        System.out.println("Private Key Set");
        System.out.println("===============");
        System.out.println();
        System.out.println(set.toString(false));
        System.out.println();
        System.out.println("==============");
        System.out.println("Public Key Set");
        System.out.println("==============");
        System.out.println();
        System.out.println(set.toString(true));
        System.out.println();
    }

    private static RSAKey getRsaKey(String algoId, int bitSize) throws JOSEException {
        final JWSAlgorithm algorithm = JWSAlgorithm.parse(algoId);

        final RSAKeyGenerator generator = new RSAKeyGenerator(bitSize, false);
        generator.keyID("test-rsa-key");
        generator.algorithm(algorithm);
        generator.keyUse(KeyUse.SIGNATURE);
        generator.keyOperations(Set.of(KeyOperation.SIGN, KeyOperation.VERIFY));

        return generator.generate();
    }
}
