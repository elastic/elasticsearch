/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLengthBetween;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;

/**
 * Test class with settings for a JWT issuer to sign JWTs for users.
 * Based on these settings, a test JWT realm can be created.
 */
public class JwtIssuer implements Closeable {
    private static final Logger LOGGER = LogManager.getLogger(JwtIssuer.class);

    record AlgJwkPair(String alg, JWK jwk) {}

    // input parameters
    final String issuerClaimValue; // claim name is hard-coded to `iss` for OIDC ID Token compatibility
    final List<String> audiencesClaimValue; // claim name is hard-coded to `aud` for OIDC ID Token compatibility
    final String principalClaimName;
    final Map<String, User> principals; // principals with roles, for sending encoded JWTs into JWT realms for authc/authz verification
    final JwtIssuerHttpsServer httpsServer;

    List<String> algorithmsAll;

    // Computed values
    List<AlgJwkPair> algAndJwksPkc;
    List<AlgJwkPair> algAndJwksHmac;
    AlgJwkPair algAndJwkHmacOidc;
    List<AlgJwkPair> algAndJwksAll;
    String encodedJwkSetPkcPublicPrivate;
    String encodedJwkSetPkcPublic;
    String encodedJwkSetHmac;
    String encodedKeyHmacOidc;

    JwtIssuer(
        final String issuerClaimValue,
        final List<String> audiencesClaimValue,
        final Map<String, User> principals,
        final boolean createHttpsServer
    ) throws Exception {
        this.issuerClaimValue = issuerClaimValue;
        this.audiencesClaimValue = audiencesClaimValue;
        this.principals = principals;
        this.httpsServer = createHttpsServer ? new JwtIssuerHttpsServer(null) : null;
        this.principalClaimName = randomFrom("sub", "oid", "client_id", "appid", "azp", "email", randomAlphaOfLengthBetween(12, 18));
    }

    // The flag areHmacJwksOidcSafe indicates if all provided HMAC JWKs are UTF8, for HMAC OIDC JWK encoding compatibility.
    void setJwks(final List<AlgJwkPair> algAndJwks, final boolean areHmacJwksOidcSafe) throws JOSEException {
        algorithmsAll = algAndJwks.stream().map(e -> e.alg).toList();
        LOGGER.info("Setting JWKs: algorithms=[{}], areHmacJwksOidcSafe=[{}]", String.join(",", algorithmsAll), areHmacJwksOidcSafe);
        algAndJwksAll = algAndJwks;
        algAndJwksPkc = algAndJwksAll.stream().filter(e -> JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_PKC.contains(e.alg)).toList();
        algAndJwksHmac = algAndJwksAll.stream().filter(e -> JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_HMAC.contains(e.alg)).toList();
        if ((algAndJwksHmac.size() == 1) && (areHmacJwksOidcSafe) && (randomBoolean())) {
            algAndJwkHmacOidc = algAndJwksHmac.get(0);
            algAndJwksHmac = Collections.emptyList();
        } else {
            algAndJwkHmacOidc = null;
        }

        // Encode PKC JWKSet (key material bytes are wrapped in Base64URL, and then wraps in JSON)
        final JWKSet jwkSetPkc = new JWKSet(algAndJwksPkc.stream().map(p -> p.jwk).toList());
        encodedJwkSetPkcPublicPrivate = JwtUtil.serializeJwkSet(jwkSetPkc, false);
        encodedJwkSetPkcPublic = JwtUtil.serializeJwkSet(jwkSetPkc, true);

        // Encode HMAC JWKSet (key material bytes are wrapped in Base64URL, and then wraps in JSON)
        final JWKSet jwkSetHmac = new JWKSet(algAndJwksHmac.stream().map(p -> p.jwk).toList());
        encodedJwkSetHmac = JwtUtil.serializeJwkSet(jwkSetHmac, false);

        // Encode HMAC OIDC JWK (key material bytes are decoded from UTF8 to UNICODE String)
        encodedKeyHmacOidc = (algAndJwkHmacOidc == null) ? null : JwtUtil.serializeJwkHmacOidc(algAndJwkHmacOidc.jwk);

        if (httpsServer != null) {
            httpsServer.updateJwkSetPkcContents(encodedJwkSetPkcPublic.getBytes(StandardCharsets.UTF_8));
        }
    }

    @Override
    public void close() {
        if (httpsServer != null) {
            try {
                httpsServer.close();
            } catch (IOException e) {
                LOGGER.warn("Exception closing HTTPS server for issuer [" + issuerClaimValue + "]", e);
            }
        }
    }
}
