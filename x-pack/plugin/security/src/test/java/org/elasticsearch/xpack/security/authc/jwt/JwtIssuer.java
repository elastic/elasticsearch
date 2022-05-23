/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Test class with settings for a JWT issuer to sign JWTs for users.
 * Based on these settings, a test JWT realm can be created.
 */
public class JwtIssuer implements Closeable {
    private static final Logger LOGGER = LogManager.getLogger(JwtIssuer.class);

    record AlgJwkPair(String alg, JWK jwk) {}

    // input parameters
    final String issuer;
    final List<String> audiences;
    final List<AlgJwkPair> algAndJwksPkc;
    final List<AlgJwkPair> algAndJwksHmac;
    final AlgJwkPair algAndJwkHmacOidc;
    final Map<String, User> users; // and their roles

    // Computed values
    final List<AlgJwkPair> algAndJwksAll;
    final Set<String> algorithmsAll;
    final String encodedJwkSetPkcPrivate;
    final String encodedJwkSetPkcPublic;
    final String encodedJwkSetHmac;
    final String encodedKeyHmacOidc;
    final JwtIssuerHttpsServer httpsServer;

    JwtIssuer(
        final String issuer,
        final List<String> audiences,
        final List<AlgJwkPair> algAndJwksPkc,
        final List<AlgJwkPair> algAndJwksHmac,
        final AlgJwkPair algAndJwkHmacOidc,
        final Map<String, User> users,
        final boolean createHttpsServer
    ) throws Exception {
        this.issuer = issuer;
        this.audiences = audiences;
        this.algAndJwksPkc = algAndJwksPkc;
        this.algAndJwksHmac = algAndJwksHmac;
        this.algAndJwkHmacOidc = algAndJwkHmacOidc;
        this.users = users;

        this.algAndJwksAll = new ArrayList<>(this.algAndJwksPkc.size() + this.algAndJwksHmac.size() + 1);
        this.algAndJwksAll.addAll(this.algAndJwksPkc);
        this.algAndJwksAll.addAll(this.algAndJwksHmac);
        if (this.algAndJwkHmacOidc != null) {
            this.algAndJwksAll.add(this.algAndJwkHmacOidc);
        }

        this.algorithmsAll = this.algAndJwksAll.stream().map(p -> p.alg).collect(Collectors.toSet());

        final JWKSet jwkSetPkc = new JWKSet(this.algAndJwksPkc.stream().map(p -> p.jwk).toList());
        final JWKSet jwkSetHmac = new JWKSet(this.algAndJwksHmac.stream().map(p -> p.jwk).toList());

        this.encodedJwkSetPkcPrivate = jwkSetPkc.getKeys().isEmpty() ? null : JwtUtil.serializeJwkSet(jwkSetPkc, false);
        this.encodedJwkSetPkcPublic = jwkSetPkc.getKeys().isEmpty() ? null : JwtUtil.serializeJwkSet(jwkSetPkc, true);
        this.encodedJwkSetHmac = jwkSetHmac.getKeys().isEmpty() ? null : JwtUtil.serializeJwkSet(jwkSetHmac, false);
        this.encodedKeyHmacOidc = (algAndJwkHmacOidc == null) ? null : JwtUtil.serializeJwkHmacOidc(this.algAndJwkHmacOidc.jwk);

        if ((Strings.hasText(this.encodedJwkSetPkcPublic) == false) || (createHttpsServer == false)) {
            this.httpsServer = null; // no PKC JWKSet, or skip HTTPS server because caller will use local file instead
        } else {
            final byte[] encodedJwkSetPkcPublicBytes = this.encodedJwkSetPkcPublic.getBytes(StandardCharsets.UTF_8);
            this.httpsServer = new JwtIssuerHttpsServer(encodedJwkSetPkcPublicBytes);
        }
    }

    @Override
    public void close() {
        if (this.httpsServer != null) {
            try {
                this.httpsServer.close();
            } catch (IOException e) {
                LOGGER.warn("Exception closing HTTPS server for issuer [" + issuer + "]", e);
            }
        }
    }
}
