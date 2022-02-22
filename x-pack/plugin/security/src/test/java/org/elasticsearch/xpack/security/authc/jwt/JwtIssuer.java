/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;

import org.elasticsearch.xpack.core.security.user.User;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Test class with settings for a JWT issuer to sign JWTs for users.
 * Based on these settings, a test JWT realm can be created.
 */
public class JwtIssuer {
    record AlgJwkPair(String alg, JWK jwk) {}

    final String issuer;
    final List<String> audiences;
    final List<AlgJwkPair> algAndJwksPkc;
    final List<AlgJwkPair> algAndJwksHmac;
    final AlgJwkPair algAndJwkHmacOidc;
    final Map<String, User> users; // and their roles

    JwtIssuer(
        final String issuer,
        final List<String> audiences,
        final List<AlgJwkPair> algAndJwksPkc,
        final List<AlgJwkPair> algAndJwksHmac,
        final AlgJwkPair algAndJwkHmacOidc,
        final Map<String, User> users
    ) {
        this.issuer = issuer;
        this.audiences = audiences;
        this.algAndJwksPkc = algAndJwksPkc;
        this.algAndJwksHmac = algAndJwksHmac;
        this.algAndJwkHmacOidc = algAndJwkHmacOidc;
        this.users = users;
    }

    Set<String> getAllAlgorithms() {
        return getAllAlgJwkPairs().stream().map(p -> p.alg).collect(Collectors.toSet());
    }

    Set<JWK> getAllJwks() {
        return getAllAlgJwkPairs().stream().map(p -> p.jwk).collect(Collectors.toSet());
    }

    Set<AlgJwkPair> getAllAlgJwkPairs() {
        final Set<AlgJwkPair> all = new HashSet<>(this.algAndJwksPkc.size() + this.algAndJwksHmac.size() + 1);
        all.addAll(this.algAndJwksPkc);
        all.addAll(this.algAndJwksHmac);
        if (this.algAndJwkHmacOidc != null) {
            all.add(this.algAndJwkHmacOidc);
        }
        return all;
    }

    JWKSet getJwkSetPkc() {
        return new JWKSet(this.algAndJwksPkc.stream().map(p -> p.jwk).toList());
    }

    JWKSet getJwkSetHmac() {
        return new JWKSet(this.algAndJwksHmac.stream().map(p -> p.jwk).toList());
    }
}
