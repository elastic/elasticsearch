/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jwt.SignedJWT;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.security.authc.BearerToken;

import java.text.ParseException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link AuthenticationToken} to hold JWT authentication related content.
 */
public class JwtAuthenticationToken extends BearerToken {
    private static final Logger LOGGER = LogManager.getLogger(JwtAuthenticationToken.class);

    protected static final String PRINCIPAL = "_jwt";

    protected final AtomicReference<SignedJWT> signedJWT;
    protected final SecureString clientAuthorizationSharedSecret;

    /**
     * @param endUserAuthorizationToken End-user authorization token (i.e. Base64Url-encoded JWT)
     * @param clientAuthorizationSharedSecret Client authorization token (e.g. Base64Url-encoded shared secret, null)
     */
    public JwtAuthenticationToken(
        final SecureString endUserAuthorizationToken,
        @Nullable final SecureString clientAuthorizationSharedSecret
    ) throws ParseException {
        super(endUserAuthorizationToken); // super.bearerString
        this.signedJWT = new AtomicReference<>(SignedJWT.parse(endUserAuthorizationToken.toString()));
        this.clientAuthorizationSharedSecret = clientAuthorizationSharedSecret; // optional
    }

    // Return generic response "_jwt" because different JWT realms can pick different principal claims
    @Override
    public String principal() {
        return JwtAuthenticationToken.PRINCIPAL;
    }

    @Override
    public SecureString credentials() {
        return null;
    }

    public SecureString getSerializedJwt() {
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
        super.clearCredentials(); // super.bearerString.close()
        this.signedJWT.set(null);
        if (this.clientAuthorizationSharedSecret != null) {
            this.clientAuthorizationSharedSecret.close();
        }
    }
}
