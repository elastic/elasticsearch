/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.OctetSequenceKey;

import org.elasticsearch.xpack.core.security.user.User;

import java.util.List;
import java.util.Map;

public class JwtIssuer {
    final String issuer;
    final List<String> audiences;
    final Map<String, List<JWK>> signatureAlgorithmAndJwks;
    final Map<String, User> users; // and their roles
    final JWKSet jwkSetAll;
    final JWKSet jwkSetHmac; // contains list of secret keys
    final JWKSet jwkSetPkc; // contains list of private/public key pairs (i.e. publicKeysOnly=false)
    JwtRealm realm; // Not final due to order: 1) Create issuer, 2) Use issuer to create realm, 3) Add realm to issuer

    JwtIssuer(
        final String issuer,
        final List<String> audiences,
        final Map<String, List<JWK>> signatureAlgorithmAndJwks,
        final Map<String, User> users
    ) {
        this.issuer = issuer;
        this.audiences = audiences;
        this.signatureAlgorithmAndJwks = signatureAlgorithmAndJwks;
        this.users = users;
        this.jwkSetAll = new JWKSet(this.signatureAlgorithmAndJwks.values().stream().flatMap(List::stream).toList());
        this.jwkSetHmac = new JWKSet(this.jwkSetAll.getKeys().stream().filter(e -> (e instanceof OctetSequenceKey)).toList());
        this.jwkSetPkc = new JWKSet(this.jwkSetAll.getKeys().stream().filter(e -> (e instanceof OctetSequenceKey) == false).toList());
    }
}
