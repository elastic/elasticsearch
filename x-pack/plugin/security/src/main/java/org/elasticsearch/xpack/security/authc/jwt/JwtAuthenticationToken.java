/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jwt.SignedJWT;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.security.authc.BearerToken;

import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link AuthenticationToken} to hold JWT authentication related content.
 */
public class JwtAuthenticationToken extends BearerToken {
    protected static final String PRINCIPAL = "_jwt";

    protected final AtomicReference<SignedJWT> signedJWT;
    protected final SecureString clientAuthorizationSharedSecret;

    /**
     * @param endUserAuthorizationToken End-user authorization token (i.e. Base64Url-encoded JWT)
     * @param clientAuthorizationSharedSecret Client authorization token (e.g. null, Base64Url-encoded shared secret)
     */
    public JwtAuthenticationToken(
        final SecureString endUserAuthorizationToken,
        final SignedJWT signedJWT,
        @Nullable final SecureString clientAuthorizationSharedSecret
    ) {
        super(endUserAuthorizationToken); // SecureString JWT bearerString
        this.signedJWT = new AtomicReference(signedJWT); // parsed JWT object
        this.clientAuthorizationSharedSecret = clientAuthorizationSharedSecret; // optional, different realms may or may not require it
    }

    // Different realms can choose different claim for principal, so return generic response "_jwt"
    @Override
    public String principal() {
        return JwtAuthenticationToken.PRINCIPAL;
    }

    @Override
    public SecureString credentials() {
        return null;
    }

    public SecureString getJwt() {
        return super.bearerString;
    }

    public SignedJWT getSignedJwt() {
        return this.signedJWT.get();
    }

    public SecureString getClientAuthorizationSharedSecret() {
        return this.clientAuthorizationSharedSecret;
    }

    @Override
    public void clearCredentials() {
        super.clearCredentials(); // Assumption super.bearerString.close()
        this.signedJWT.set(null);
        if (this.clientAuthorizationSharedSecret != null) {
            this.clientAuthorizationSharedSecret.close();
        }
    }
}
