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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import java.util.concurrent.atomic.AtomicReference;

/**
 * An {@link AuthenticationToken} to hold JWT authentication related content.
 */
public class JwtAuthenticationToken implements AuthenticationToken {
    private static final Logger LOGGER = LogManager.getLogger(JwtAuthenticationToken.class);

    private static final List<String> CLAIMS_TO_REMOVE = List.of("iss", "aud", "exp", "iat", "nbf", "auth_time", "nonce", "jti");

    // Stored members
    protected final SecureString endUserSecret; // required
    protected final SecureString clientAuthorizationSharedSecret; // optional, nullable

    // Parsed members
    protected final AtomicReference<SignedJWT> signedJwt = new AtomicReference<>(null);
    protected final AtomicReference<JWSHeader> jwsHeader = new AtomicReference<>(null);
    protected final AtomicReference<JWTClaimsSet> jwtClaimsSet = new AtomicReference<>(null);
    protected final AtomicReference<byte[]> jwtSignature = new AtomicReference<>(null);
    protected final String issuerClaim;
    protected final List<String> audiencesClaim;
    protected final String subjectClaim;
    protected final String principal;
    protected final SecureString credentials;

    /**
     * Store a mandatory JWT and optional Shared Secret. Parse the JWT, and extract the header, claims set, and signature.
     * Throws IllegalArgumentException if bearerString is missing, or if JWT parsing fails.
     * @param endUserSecret Base64Url-encoded JWT for End-user authorization. Required by all JWT realms.
     * @param clientAuthorizationSharedSecret URL-safe Shared Secret for Client authorization. Required by some JWT realms.
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
            final JWSHeader jwsHeader = parsed.getHeader();
            final JWTClaimsSet jwtClaimsSet = parsed.getJWTClaimsSet();
            final Base64URL base64Url = parsed.getSignature();
            final byte[] signatureBytes = base64Url.decode();
            this.signedJwt.set(parsed);
            this.jwsHeader.set(jwsHeader);
            this.jwtClaimsSet.set(jwtClaimsSet);
            this.jwtSignature.set(signatureBytes);
        } catch (ParseException e) {
            this.signedJwt.set(null);
            this.jwsHeader.set(null);
            this.jwtClaimsSet.set(null);
            this.jwtSignature.set(null);
            throw new IllegalArgumentException("Failed to parse JWT bearer token", e);
        }
        final JWTClaimsSet jwtClaimsSet = this.jwtClaimsSet.get();
        this.issuerClaim = jwtClaimsSet.getIssuer();
        this.audiencesClaim = jwtClaimsSet.getAudience();
        this.subjectClaim = jwtClaimsSet.getSubject();

        if (Strings.hasText(this.issuerClaim) == false) {
            throw new IllegalArgumentException("Issuer claim is missing.");
        } else if ((this.audiencesClaim == null) || (this.audiencesClaim.isEmpty())) {
            throw new IllegalArgumentException("Audiences claim is missing.");
        }
        final String orderedAudiencesString = String.join(",", new TreeSet<>(this.audiencesClaim));
        if (Strings.hasText(this.subjectClaim)) {
            // Subject is present, so set principal to "issuer/audiences/subject"
            this.principal = this.issuerClaim + "/" + orderedAudiencesString + "/" + this.subjectClaim;
        } else {
            // Subject is absent, so set principal to "issuer/audiences/orderedClaims" (Note: remove unnecessary claims from orderedClaims)
            final Map<String, Object> orderedClaimSet = new TreeMap<>(jwtClaimsSet.getClaims()); // ordered
            for (final String claimToRemove : CLAIMS_TO_REMOVE) {
                orderedClaimSet.remove(claimToRemove);
            }
            if (orderedClaimSet.isEmpty()) {
                throw new IllegalArgumentException(
                    "Subject claim is missing, and no other claims found besides [" + String.join(",", CLAIMS_TO_REMOVE) + "]."
                );
            }
            this.principal = this.issuerClaim + "/" + orderedAudiencesString + "/" + orderedClaimSet;
        }
        this.credentials = new SecureString(this.principal.toCharArray());
    }

    @Override
    public String principal() {
        return this.principal;
    }

    @Override
    public SecureString credentials() {
        return this.credentials;
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

    public byte[] getSignatureBytes() {
        return this.jwtSignature.get();
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

    public void clearCredentials() {
        this.endUserSecret.close();
        if (this.clientAuthorizationSharedSecret != null) {
            this.clientAuthorizationSharedSecret.close();
        }
        this.signedJwt.set(null);
        this.jwsHeader.set(null);
        this.jwtClaimsSet.set(null);
        Arrays.fill(this.jwtSignature.getAndSet(null), (byte) 0);
    }
}
