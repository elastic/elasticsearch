/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.util.Base64URL;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;

import java.text.ParseException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An {@link AuthenticationToken} to hold JWT authentication related content.
 */
public class JwtAuthenticationToken implements AuthenticationToken {
    // Stored members
    protected final SecureString endUserSecret; // required
    protected final SecureString clientAuthorizationSharedSecret; // optional, nullable

    // Parsed members
    protected final AtomicReference<SignedJWT> signedJwt = new AtomicReference<>(null);
    protected final AtomicReference<JWSHeader> jwsHeader = new AtomicReference<>(null);
    protected final AtomicReference<JWTClaimsSet> jwtClaimsSet = new AtomicReference<>(null);
    protected final AtomicReference<Base64URL> jwtSignature = new AtomicReference<>(null);

    /**
     * Store a mandatory JWT and optional Shared Secret. Parse the JWT, and extract the header, claims set, and signature.
     * Throws IllegalArgumentException if bearerString is missing, or if JWT parsing fails.
     * @param endUserSecret Base64Url-encoded JWT for End-user authorization. Required by all JWT realms.
     * @param clientAuthorizationSharedSecret Base64Url-encoded Shared Secret for Client authorization. Required by some JWT realms.
     */
    public JwtAuthenticationToken(final SecureString endUserSecret, @Nullable final SecureString clientAuthorizationSharedSecret) {
        if (endUserSecret == null) {
            throw new IllegalArgumentException("JWT bearer token must be non-null");
        } else if (endUserSecret.isEmpty()) {
            throw new IllegalArgumentException("JWT bearer token must be non-empty");
        } else if ((clientAuthorizationSharedSecret != null) && (clientAuthorizationSharedSecret.isEmpty())) {
            throw new IllegalArgumentException("Client shared secret must be non-empty");
        }
        this.endUserSecret = endUserSecret; // required
        this.clientAuthorizationSharedSecret = clientAuthorizationSharedSecret; // optional, nullable
        // Parse JWT
        try {
            final SignedJWT parsed = SignedJWT.parse(this.endUserSecret.toString());
            this.signedJwt.set(parsed);
            this.jwsHeader.set(parsed.getHeader());
            this.jwtClaimsSet.set(parsed.getJWTClaimsSet());
            this.jwtSignature.set(parsed.getSignature());
        } catch (ParseException e) {
            this.signedJwt.set(null);
            this.jwsHeader.set(null);
            this.jwtClaimsSet.set(null);
            this.jwtSignature.set(null);
            throw new IllegalArgumentException("Failed to parse JWT bearer token", e);
        }
    }

    @Override
    public String principal() {
        return this.endUserSecret.toString();
    }

    @Override
    public SecureString credentials() {
        return this.endUserSecret;
    }

    public SecureString getSerializedJwt() {
        return this.endUserSecret;
    }

    public SecureString getClientAuthorizationSharedSecret() {
        return this.clientAuthorizationSharedSecret;
    }

    public SignedJWT getSignedJwt() {
        return this.signedJwt.get();
    }

    public JWSHeader getJwsHeader() {
        return this.jwsHeader.get();
    }

    public JWTClaimsSet getJwtClaimsSet() {
        return this.jwtClaimsSet.get();
    }

    public Base64URL getSignature() {
        return this.jwtSignature.get();
    }

    public void clearCredentials() {
        this.endUserSecret.close();
        if (this.clientAuthorizationSharedSecret != null) {
            this.clientAuthorizationSharedSecret.close();
        }
        this.signedJwt.set(null);
        this.jwsHeader.set(null);
        this.jwtClaimsSet.set(null);
        this.jwtSignature.set(null);
    }
}
