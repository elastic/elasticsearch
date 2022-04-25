/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;

import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * An {@link AuthenticationToken} to hold JWT authentication related content.
 */
public class JwtAuthenticationToken implements AuthenticationToken {
    // If the `sub` claim is absent, order and use all of these other claims in its place, except these claims
    private static final List<String> CLAIMS_TO_FILTER = List.of("iss", "aud", "exp", "iat", "nbf", "auth_time", "nonce", "jti");

    // Stored members
    protected SecureString endUserSignedJwt; // required
    protected SecureString clientAuthenticationSharedSecret; // optional, nullable
    protected String principal; // "iss/aud/sub"

    /**
     * Store a mandatory JWT and optional Shared Secret. Parse the JWT, and extract the header, claims set, and signature.
     * Throws IllegalArgumentException if bearerString is missing, or if JWT parsing fails.
     * @param endUserSignedJwt Base64Url-encoded JWT for End-user authentication. Required by all JWT realms.
     * @param clientAuthenticationSharedSecret URL-safe Shared Secret for Client authentication. Required by some JWT realms.
     */
    public JwtAuthenticationToken(final SecureString endUserSignedJwt, @Nullable final SecureString clientAuthenticationSharedSecret) {
        if (endUserSignedJwt.isEmpty()) {
            throw new IllegalArgumentException("JWT bearer token must be non-empty");
        } else if ((clientAuthenticationSharedSecret != null) && (clientAuthenticationSharedSecret.isEmpty())) {
            throw new IllegalArgumentException("Client shared secret must be non-empty");
        }
        this.endUserSignedJwt = endUserSignedJwt; // required
        this.clientAuthenticationSharedSecret = clientAuthenticationSharedSecret; // optional, nullable

        JWTClaimsSet jwtClaimsSet;
        try {
            jwtClaimsSet = SignedJWT.parse(this.endUserSignedJwt.toString()).getJWTClaimsSet();
        } catch (ParseException e) {
            throw new IllegalArgumentException("Failed to parse JWT bearer token", e);
        }
        final String issuer = jwtClaimsSet.getIssuer();
        final List<String> audiences = jwtClaimsSet.getAudience();

        if (Strings.hasText(issuer) == false) {
            throw new IllegalArgumentException("Issuer claim 'iss' is missing.");
        } else if ((audiences == null) || (audiences.isEmpty())) {
            throw new IllegalArgumentException("Audiences claim 'aud' is missing.");
        }
        final String orderedAudiences = String.join(",", new TreeSet<>(jwtClaimsSet.getAudience()));
        // OPTIONAL (REQUIRED for ID Tokens, but JWT Realm can override)
        final String computedSubject;
        if (Strings.hasText(jwtClaimsSet.getSubject())) {
            computedSubject = jwtClaimsSet.getSubject(); // principal = "iss/aud/sub"
        } else {
            final Map<String, Object> remainingClaims = jwtClaimsSet.getClaims()
                .entrySet()
                .stream()
                .filter(e -> CLAIMS_TO_FILTER.contains(e.getKey()) == false)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            if (remainingClaims.isEmpty()) {
                throw new IllegalArgumentException("No claims left after filtering [" + String.join(",", CLAIMS_TO_FILTER) + "].");
            }
            computedSubject = new TreeMap<>(jwtClaimsSet.getClaims()).toString(); // principal = "iss/aud/orderedClaimsSubset"
        }
        this.principal = jwtClaimsSet.getIssuer() + "/" + orderedAudiences + "/" + computedSubject;
    }

    @Override
    public String principal() {
        return this.principal;
    }

    @Override
    public SecureString credentials() {
        return null;
    }

    public SecureString getEndUserSignedJwt() {
        return this.endUserSignedJwt;
    }

    public SecureString getClientAuthenticationSharedSecret() {
        return this.clientAuthenticationSharedSecret;
    }

    @Override
    public void clearCredentials() {
        this.endUserSignedJwt.close();
        this.endUserSignedJwt = null;
        if (this.clientAuthenticationSharedSecret != null) {
            this.clientAuthenticationSharedSecret.close();
            this.clientAuthenticationSharedSecret = null;
        }
        this.principal = null;
    }

    @Override
    public String toString() {
        return JwtAuthenticationToken.class.getSimpleName() + "=" + this.principal;
    }
}
