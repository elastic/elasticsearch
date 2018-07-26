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

    private final byte[] decodedToken;

    public KerberosAuthenticationToken(final byte[] decodedToken) {
        this.decodedToken = decodedToken;
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
        return "<Kerberos Token>";
    }

    @Override
    public Object credentials() {
        return decodedToken;
    }

    @Override
    public void clearCredentials() {
        Arrays.fill(decodedToken, (byte) 0);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(decodedToken);
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
        return Arrays.equals(otherKerbToken.decodedToken, this.decodedToken);
    }

    /**
     * Creates {@link ElasticsearchSecurityException} with
     * {@link RestStatus#UNAUTHORIZED} and cause. This also populates
     * 'WWW-Authenticate' header with value as 'Negotiate' scheme.
     *
     * @param message the detail message
     * @param cause nested exception
     * @param args the arguments for the message
     * @return instance of {@link ElasticsearchSecurityException}
     */
    static ElasticsearchSecurityException unauthorized(final String message, final Throwable cause, final Object... args) {
        ElasticsearchSecurityException ese = new ElasticsearchSecurityException(message, RestStatus.UNAUTHORIZED, cause, args);
        ese.addHeader(WWW_AUTHENTICATE, NEGOTIATE_SCHEME_NAME);
        return ese;
    }

    /**
     * Sets 'WWW-Authenticate' header if outToken is not null on passed instance of
     * {@link ElasticsearchSecurityException} and returns the instance. <br>
     * If outToken is provided and is not {@code null} or empty, then that is
     * appended to 'Negotiate ' and is used as header value for header
     * 'WWW-Authenticate' sent to the peer in the form 'Negotiate oYH1MIHyoAMK...'.
     * This is required by client for GSS negotiation to continue further.
     *
     * @param ese instance of {@link ElasticsearchSecurityException} with status
     *            {@link RestStatus#UNAUTHORIZED}
     * @param outToken if non {@code null} and not empty then this will be the value
     *            sent to the peer.
     * @return instance of {@link ElasticsearchSecurityException} with
     *         'WWW-Authenticate' header populated.
     */
    static ElasticsearchSecurityException unauthorizedWithOutputToken(final ElasticsearchSecurityException ese, final String outToken) {
        assert ese.status() == RestStatus.UNAUTHORIZED;
        if (Strings.hasText(outToken)) {
            ese.addHeader(WWW_AUTHENTICATE, NEGOTIATE_AUTH_HEADER_PREFIX + outToken);
        }
        return ese;
    }
}
