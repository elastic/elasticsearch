/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;

import java.text.ParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * An {@link AuthenticationToken} to hold JWT authentication related content.
 */
public class JwtAuthenticationToken implements AuthenticationToken {
    private static final List<String> CLAIMS_TO_REMOVE = List.of("iss", "aud", "exp", "iat", "nbf", "auth_time", "nonce", "jti");

    // Stored members
    protected final SecureString endUserSignedJwt; // required
    protected final SecureString clientAuthorizationSharedSecret; // optional, nullable

    // Parsed members
    protected SignedJWT signedJwt;
    protected JWSHeader jwsHeader;
    protected JWTClaimsSet jwtClaimsSet;
    protected byte[] jwtSignature;
    protected String issuerClaim;
    protected List<String> audiencesClaim;
    protected String subjectClaim;
    protected String principal;

    /**
     * Store a mandatory JWT and optional Shared Secret. Parse the JWT, and extract the header, claims set, and signature.
     * Throws IllegalArgumentException if bearerString is missing, or if JWT parsing fails.
     * @param endUserSignedJwt Base64Url-encoded JWT for End-user authorization. Required by all JWT realms.
     * @param clientAuthorizationSharedSecret URL-safe Shared Secret for Client authorization. Required by some JWT realms.
     */
    public JwtAuthenticationToken(final SecureString endUserSignedJwt, @Nullable final SecureString clientAuthorizationSharedSecret) {
        if (endUserSignedJwt == null) {
            throw new IllegalArgumentException("JWT bearer token must be non-null");
        } else if (endUserSignedJwt.isEmpty()) {
            throw new IllegalArgumentException("JWT bearer token must be non-empty");
        } else if ((clientAuthorizationSharedSecret != null) && (clientAuthorizationSharedSecret.isEmpty())) {
            throw new IllegalArgumentException("Client shared secret must be non-empty");
        }
        this.endUserSignedJwt = endUserSignedJwt; // required
        this.clientAuthorizationSharedSecret = clientAuthorizationSharedSecret; // optional, nullable
        try {
            final SignedJWT parsed = SignedJWT.parse(this.endUserSignedJwt.toString());
            this.signedJwt = parsed;
            this.jwsHeader = parsed.getHeader();
            this.jwtClaimsSet = parsed.getJWTClaimsSet();
            this.jwtSignature = parsed.getSignature().decode();
        } catch (ParseException e) {
            throw new IllegalArgumentException("Failed to parse JWT bearer token", e);
        }
        final JWTClaimsSet jwtClaimsSet = this.jwtClaimsSet;
        this.issuerClaim = jwtClaimsSet.getIssuer();
        this.audiencesClaim = jwtClaimsSet.getAudience();
        this.subjectClaim = jwtClaimsSet.getSubject();

        if (Strings.hasText(this.issuerClaim) == false) {
            throw new IllegalArgumentException("Issuer claim is missing.");
        } else if ((this.audiencesClaim == null) || (this.audiencesClaim.isEmpty())) {
            throw new IllegalArgumentException("Audiences claim is missing.");
        }
        final String orderedAudiences = String.join(",", new TreeSet<>(this.audiencesClaim));
        final String computedSubject;
        if (Strings.hasText(this.subjectClaim)) {
            computedSubject = this.subjectClaim; // principal = "iss/aud/sub"
        } else {
            final Map<String, Object> orderedClaimsSubset = new TreeMap<>(jwtClaimsSet.getClaims());
            for (final String claimToRemove : CLAIMS_TO_REMOVE) {
                orderedClaimsSubset.remove(claimToRemove);
            }
            if (orderedClaimsSubset.isEmpty()) {
                throw new IllegalArgumentException(
                    "Claim [sub] is absent, and no other claims found besides [" + String.join(",", CLAIMS_TO_REMOVE) + "]."
                );
            }
            computedSubject = orderedClaimsSubset.toString(); // principal = "iss/aud/orderedClaimsSubset"
        }
        this.principal = this.issuerClaim + "/" + orderedAudiences + "/" + computedSubject;
    }

    @Override
    public String principal() {
        return this.principal;
    }

    @Override
    public SecureString credentials() {
        return this.endUserSignedJwt;
    }

    public SecureString getEndUserSignedJwt() {
        return this.endUserSignedJwt;
    }

    public SecureString getClientAuthorizationSharedSecret() {
        return this.clientAuthorizationSharedSecret;
    }

    public SignedJWT getSignedJwt() {
        return this.signedJwt;
    }

    public JWSHeader getJwsHeader() {
        return this.jwsHeader;
    }

    public JWTClaimsSet getJwtClaimsSet() {
        return this.jwtClaimsSet;
    }

    public byte[] getSignatureBytes() {
        return this.jwtSignature;
    }

    public String getIssuerClaim() {
        return this.issuerClaim;
    }

    public List<String> getAudiencesClaim() {
        return this.audiencesClaim;
    }

    public String getSubjectClaim() {
        return this.subjectClaim;
    }

    @Override
    public void clearCredentials() {
        this.endUserSignedJwt.close();
        if (this.clientAuthorizationSharedSecret != null) {
            this.clientAuthorizationSharedSecret.close();
        }
        this.signedJwt = null;
        this.jwsHeader = null;
        this.jwtClaimsSet = null;
        Arrays.fill(this.jwtSignature, (byte) 0);
        this.jwtSignature = null;
        this.issuerClaim = null;
        this.audiencesClaim = null;
        this.subjectClaim = null;
        this.principal = null;
    }
}
