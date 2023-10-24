/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Realm;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Objects;

/**
 * An {@link AuthenticationToken} to hold JWT authentication related content.
 */
public class JwtAuthenticationToken implements AuthenticationToken {
    private static final Logger LOGGER = LogManager.getLogger(JwtAuthenticationToken.class);

    private final String principal;
    private SignedJWT signedJWT;
    private final byte[] userCredentialsHash;
    @Nullable
    private final SecureString clientAuthenticationSharedSecret;

    public static JwtAuthenticationToken parseJWTAuthenticationToken(
        Iterable<Realm> allRealms,
        @Nullable CharSequence jwt,
        @Nullable SecureString clientAuthentication
    ) {
        if (jwt == null || jwt.isEmpty()) {
            return null;
        }
        final SignedJWT signedJWT;
        try {
            signedJWT = SignedJWT.parse(jwt.toString());
            // trigger JWT claims parsing
            signedJWT.getJWTClaimsSet();
        } catch (ParseException e) {
            LOGGER.debug("Failed to parse JWT bearer token", e);
            return null;
        }
        return new JwtAuthenticationToken(
            buildJwtTokenPrincipal(allRealms, signedJWT),
            signedJWT,
            JwtUtil.sha256(jwt),
            clientAuthentication
        );
    }

    /**
     * Store a mandatory JWT and optional Shared Secret.
     * @param principal The token's principal, useful as a realm order cache key
     * @param signedJWT The JWT parsed from the end-user credentials
     * @param userCredentialsHash The hash of the end-user credentials is used to compute the key for user cache at the realm level.
     *                            See also {@link JwtRealm#authenticate}.
     * @param clientAuthenticationSharedSecret URL-safe Shared Secret for Client authentication. Required by some JWT realms.
     */
    private JwtAuthenticationToken(
        String principal,
        SignedJWT signedJWT,
        byte[] userCredentialsHash,
        @Nullable final SecureString clientAuthenticationSharedSecret
    ) {
        this.principal = Objects.requireNonNull(principal);
        this.signedJWT = Objects.requireNonNull(signedJWT);
        this.userCredentialsHash = Objects.requireNonNull(userCredentialsHash);

        if ((clientAuthenticationSharedSecret != null) && (clientAuthenticationSharedSecret.isEmpty())) {
            throw new IllegalArgumentException("Client shared secret must be non-empty");
        }
        this.clientAuthenticationSharedSecret = clientAuthenticationSharedSecret;
    }

    @Override
    public String principal() {
        return principal;
    }

    @Override
    public SecureString credentials() {
        return null;
    }

    public SignedJWT getSignedJWT() {
        return signedJWT;
    }

    public JWTClaimsSet getJWTClaimsSet() {
        try {
            return signedJWT.getJWTClaimsSet();
        } catch (ParseException e) {
            assert false : "The JWT claims set should have already been successfully parsed before building the JWT authentication token";
            throw new IllegalArgumentException(e);
        }
    }

    public byte[] getUserCredentialsHash() {
        return userCredentialsHash;
    }

    public SecureString getClientAuthenticationSharedSecret() {
        return clientAuthenticationSharedSecret;
    }

    @Override
    public void clearCredentials() {
        signedJWT = null;
        Arrays.fill(userCredentialsHash, (byte) 0);
        if (clientAuthenticationSharedSecret != null) {
            clientAuthenticationSharedSecret.close();
        }
    }

    @Override
    public String toString() {
        return JwtAuthenticationToken.class.getSimpleName() + "=" + this.principal;
    }

    private static String buildJwtTokenPrincipal(Iterable<Realm> realms, SignedJWT signedJWT) {
        final JWTClaimsSet jwtClaimsSet;
        try {
            jwtClaimsSet = signedJWT.getJWTClaimsSet();
        } catch (ParseException e) {
            assert false : "claims should've been correctly parsed before";
            throw new IllegalStateException("Failed to parse JWT claims set", e);
        }
        final String issuer = jwtClaimsSet.getIssuer();
        if (Strings.hasText(issuer) == false) {
            return "<unrecognized-jwt>";
        }
        for (var realm : realms) {
            if (realm instanceof final JwtRealm jwtRealm) {
                // Try all known extraction functions to build the token principal
                final String tokenPrincipalSuffix = jwtRealm.buildTokenPrincipal(jwtClaimsSet);
                if (tokenPrincipalSuffix != null) {
                    return issuer + "/" + tokenPrincipalSuffix;
                }
            }
        }
        return "<unrecognized-jwt> by " + issuer;
    }
}
