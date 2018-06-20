/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.kerberos;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;

import java.util.Arrays;
import java.util.Base64;

/**
 * This class represents an AuthenticationToken for Kerberos authentication
 * using SPNEGO. The token stores base 64 decoded token bytes, extracted from
 * the Authorization header with auth scheme 'Negotiate'.
 * <p>
 * Example Authorization header "Authorization: Negotiate
 * YIIChgYGKwYBBQUCoII..."
 * <p>
 * If there is any error handling during extraction of 'Negotiate' header then
 * it throws {@link ElasticsearchSecurityException} with
 * {@link RestStatus#UNAUTHORIZED} and header 'WWW-Authenticate: Negotiate'
 */
public final class KerberosAuthenticationToken implements AuthenticationToken {

    public static final String WWW_AUTHENTICATE = "WWW-Authenticate";
    public static final String AUTH_HEADER = "Authorization";
    public static final String NEGOTIATE_SCHEME_NAME = "Negotiate";
    public static final String NEGOTIATE_AUTH_HEADER_PREFIX = NEGOTIATE_SCHEME_NAME + " ";

    // authorization scheme check is case-insensitive
    private static final boolean IGNORE_CASE_AUTH_HEADER_MATCH = true;

    private final byte[] base64DecodedToken;

    public KerberosAuthenticationToken(final byte[] base64DecodedToken) {
        this.base64DecodedToken = base64DecodedToken;
    }

    /**
     * Extract token from authorization header and if it is valid
     * {@value #NEGOTIATE_AUTH_HEADER_PREFIX} then returns
     * {@link KerberosAuthenticationToken}
     *
     * @param authorizationHeader Authorization header from request
     * @return returns {@code null} if {@link #AUTH_HEADER} is empty or does not
     *         start with {@value #NEGOTIATE_AUTH_HEADER_PREFIX} else returns valid
     *         {@link KerberosAuthenticationToken}
     * @throws ElasticsearchSecurityException when negotiate header is invalid.
     */
    public static KerberosAuthenticationToken extractToken(final String authorizationHeader) {
        if (Strings.isNullOrEmpty(authorizationHeader)) {
            return null;
        }
        if (authorizationHeader.regionMatches(IGNORE_CASE_AUTH_HEADER_MATCH, 0, NEGOTIATE_AUTH_HEADER_PREFIX, 0,
                NEGOTIATE_AUTH_HEADER_PREFIX.length()) == false) {
            return null;
        }

        final String base64EncodedToken = authorizationHeader.substring(NEGOTIATE_AUTH_HEADER_PREFIX.length()).trim();
        if (Strings.isEmpty(base64EncodedToken)) {
            throw unauthorized("invalid negotiate authentication header value, expected base64 encoded token but value is empty", null);
        }

        byte[] decodedKerberosTicket = null;
        try {
            decodedKerberosTicket = Base64.getDecoder().decode(base64EncodedToken);
        } catch (IllegalArgumentException iae) {
            throw unauthorized("invalid negotiate authentication header value, could not decode base64 token {}", iae, base64EncodedToken);
        }

        return new KerberosAuthenticationToken(decodedKerberosTicket);
    }

    @Override
    public String principal() {
        return "<Unauthenticated Principal>";
    }

    @Override
    public Object credentials() {
        return base64DecodedToken;
    }

    @Override
    public void clearCredentials() {
        Arrays.fill(base64DecodedToken, (byte) 0);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(base64DecodedToken);
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other)
            return true;
        if (other == null)
            return false;
        if (getClass() != other.getClass())
            return false;
        final KerberosAuthenticationToken otherKerbToken = (KerberosAuthenticationToken) other;
        return Arrays.equals(otherKerbToken.base64DecodedToken, this.base64DecodedToken);
    }

    private static ElasticsearchSecurityException unauthorized(final String message, final Throwable cause, final Object... args) {
        ElasticsearchSecurityException ese = new ElasticsearchSecurityException(message, RestStatus.UNAUTHORIZED, cause, args);
        ese.addHeader(WWW_AUTHENTICATE, NEGOTIATE_SCHEME_NAME);
        return ese;
    }
}
