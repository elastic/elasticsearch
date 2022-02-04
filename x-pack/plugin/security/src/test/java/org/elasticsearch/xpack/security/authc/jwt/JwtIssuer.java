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

@SuppressWarnings({ "checkstyle:MissingJavadocType", "checkstyle:MissingJavadocMethod" })
public class JwtIssuer {
    private final String issuer;
    private final List<String> audiences;
    private final Map<String, List<JWK>> signatureAlgorithmAndJwks;
    private final Map<String, User> users; // and their roles
    private final JWKSet jwksetAll;
    private final JWKSet jwksetHmac; // contains list of secret keys
    private final JWKSet jwksetPkc; // contains list of private/public key pairs (i.e. publicKeysOnly=false)

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
        this.jwksetAll = new JWKSet(this.signatureAlgorithmAndJwks.values().stream().flatMap(List::stream).toList());
        this.jwksetHmac = new JWKSet(this.jwksetAll.getKeys().stream().filter(e -> (e instanceof OctetSequenceKey)).toList());
        this.jwksetPkc = new JWKSet(this.jwksetAll.getKeys().stream().filter(e -> (e instanceof OctetSequenceKey) == false).toList());
    }

    public String getIssuer() {
        return this.issuer;
    }

    public List<String> getAudiences() {
        return this.audiences;
    }

    public Map<String, List<JWK>> getSignatureAlgorithmAndJwks() {
        return this.signatureAlgorithmAndJwks;
    }

    public Map<String, User> getUsers() {
        return this.users;
    }

    public JWKSet getJwkSetAll() {
        return this.jwksetAll;
    }

    public JWKSet getJwkSetHmac() {
        return this.jwksetHmac;
    }

    public JWKSet getJwkSetPkc() {
        return this.jwksetPkc;
    }
}
