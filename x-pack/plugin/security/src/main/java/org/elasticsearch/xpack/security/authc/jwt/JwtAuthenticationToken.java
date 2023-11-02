/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Objects;
import java.util.TreeSet;

/**
 * An {@link AuthenticationToken} to hold JWT authentication related content.
 */
public class JwtAuthenticationToken implements AuthenticationToken {
    private SignedJWT signedJWT;
    private final String principal;
    private final byte[] userCredentialsHash;
    @Nullable
    private final SecureString clientAuthenticationSharedSecret;

    /**
     * Store a mandatory JWT and optional Shared Secret.
     * @param signedJWT The JWT parsed from the end-user credentials
     * @param userCredentialsHash The hash of the end-user credentials is used to compute the key for user cache at the realm level.
     *                            See also {@link JwtRealm#authenticate}.
     * @param clientAuthenticationSharedSecret URL-safe Shared Secret for Client authentication. Required by some JWT realms.
     */
    public JwtAuthenticationToken(
        SignedJWT signedJWT,
        byte[] userCredentialsHash,
        @Nullable final SecureString clientAuthenticationSharedSecret
    ) {
        this.signedJWT = Objects.requireNonNull(signedJWT);
        this.principal = buildTokenPrincipal();
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
            throw new IllegalStateException(e);
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

    private String buildTokenPrincipal() {
        JWTClaimsSet jwtClaimsSet = getJWTClaimsSet();
        StringBuilder principalBuilder = new StringBuilder();
        for (String claimName : new TreeSet<>(jwtClaimsSet.getClaims().keySet())) {
            // only use String or String[] claim values to assemble the principal
            try {
                String claimValue = jwtClaimsSet.getStringClaim(claimName);
                if (principalBuilder.isEmpty() == false) {
                    principalBuilder.append(' ');
                }
                principalBuilder.append('\'').append(claimName).append(':').append(claimValue).append('\'');
            } catch (ParseException e1) {
                try {
                    String[] claimValues = jwtClaimsSet.getStringArrayClaim(claimName);
                    if (claimValues != null && claimValues.length > 0) {
                        if (principalBuilder.isEmpty() == false) {
                            principalBuilder.append(' ');
                        }
                        principalBuilder.append('\'').append(claimName).append(':');
                        for (int i = 0; i < claimValues.length; i++) {
                            if (i > 0) {
                                principalBuilder.append(',');
                            }
                            principalBuilder.append(claimValues[i]);
                        }
                        principalBuilder.append('\'');
                    }
                } catch (ParseException e2) {
                    // ignored claim type
                }
            }
        }
        if (principalBuilder.isEmpty()) {
            return "<malformed JWT token>";
        }
        return principalBuilder.toString();
    }
}
