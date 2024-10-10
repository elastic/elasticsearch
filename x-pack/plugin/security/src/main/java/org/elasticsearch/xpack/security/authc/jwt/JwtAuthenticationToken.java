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
import java.util.List;
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

    public static JwtAuthenticationToken tryParseJwt(SecureString userCredentials, @Nullable SecureString clientCredentials) {
        SignedJWT signedJWT = JwtUtil.parseSignedJWT(userCredentials);
        if (signedJWT == null) {
            return null;
        }
        return new JwtAuthenticationToken(signedJWT, JwtUtil.sha256(userCredentials), clientCredentials);
    }

    /**
     * Store a mandatory JWT and optional Shared Secret.
     * @param signedJWT The JWT parsed from the end-user credentials
     * @param userCredentialsHash The hash of the end-user credentials is used to compute the key for user cache at the realm level.
     *                            See also {@code JwtRealm#authenticate}.
     * @param clientAuthenticationSharedSecret URL-safe Shared Secret for Client authentication. Required by some JWT realms.
     */
    @SuppressWarnings("this-escape")
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
        claimsLoop: for (String claimName : new TreeSet<>(jwtClaimsSet.getClaims().keySet())) {
            Object claimValue = jwtClaimsSet.getClaim(claimName);
            if (claimValue == null) {
                continue;
            }
            // only use String or String[] claim values to assemble the principal
            if (claimValue instanceof String) {
                if (principalBuilder.isEmpty() == false) {
                    principalBuilder.append(' ');
                }
                principalBuilder.append('\'').append(claimName).append(':').append((String) claimValue).append('\'');
            } else if (claimValue instanceof List<?>) {
                List<?> claimValuesList = (List<?>) claimValue;
                if (claimValuesList.isEmpty()) {
                    continue;
                }
                for (Object claimValueElem : claimValuesList) {
                    if (claimValueElem instanceof String == false) {
                        continue claimsLoop;
                    }
                }
                if (principalBuilder.isEmpty() == false) {
                    principalBuilder.append(' ');
                }
                principalBuilder.append('\'').append(claimName).append(':');
                for (int i = 0; i < claimValuesList.size(); i++) {
                    if (i > 0) {
                        principalBuilder.append(',');
                    }
                    principalBuilder.append((String) claimValuesList.get(i));
                }
                principalBuilder.append('\'');
            }
        }
        if (principalBuilder.isEmpty()) {
            return "<unrecognized JWT token>";
        }
        return principalBuilder.toString();
    }
}
