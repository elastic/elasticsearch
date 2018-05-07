/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.kerberos;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Locale;
import java.util.Objects;

/**
 * Holds on to base 64 encoded ticket, also helps extracting token from
 * {@link ThreadContext}
 */
public final class KerberosAuthenticationToken implements AuthenticationToken {

    public static final String AUTH_HEADER = "Authorization";
    public static final String NEGOTIATE_AUTH_HEADER = "Negotiate ";
    public static final String UNAUTHENTICATED_PRINCIPAL_NAME = "<Unauthenticated Principal Name>";

    private final String principalName;
    private String base64EncodedTicket;

    public KerberosAuthenticationToken(final String principalName, final String base64EncodedToken) {
        this.principalName = principalName;
        this.base64EncodedTicket = base64EncodedToken;
    }

    /**
     * Extract token from header and if valid {@link #NEGOTIATE_AUTH_HEADER} then
     * returns {@link KerberosAuthenticationToken}
     *
     * @param context
     *            {@link ThreadContext}
     * @return returns {@code null} if {@link #AUTH_HEADER} is empty or not an
     *         {@link #NEGOTIATE_AUTH_HEADER} else returns valid
     *         {@link KerberosAuthenticationToken}
     */
    public static KerberosAuthenticationToken extractToken(final ThreadContext context) {
        final String authHeader = context.getHeader(AUTH_HEADER);
        if (Strings.isNullOrEmpty(authHeader)) {
            return null;
        }

        if (authHeader.startsWith(NEGOTIATE_AUTH_HEADER) == Boolean.FALSE) {
            return null;
        }

        final String base64EncodedToken = authHeader.substring(NEGOTIATE_AUTH_HEADER.length()).trim();
        if (Strings.isEmpty(base64EncodedToken)) {
            throw new IllegalArgumentException(
                    "invalid negotiate authentication header value, expected base64 encoded token but value is empty");
        }
        final byte[] base64Token = base64EncodedToken.getBytes(StandardCharsets.UTF_8);
        final byte[] decodedKerberosTicket = Base64.getDecoder().decode(base64Token);

        if (decodedKerberosTicket == null || decodedKerberosTicket.length == 0) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "invalid negotiate authentication header value, could not decode base64 token %s", base64Token));
        }
        return new KerberosAuthenticationToken(UNAUTHENTICATED_PRINCIPAL_NAME, base64EncodedToken);
    }

    @Override
    public String principal() {
        return principalName;
    }

    @Override
    public Object credentials() {
        return base64EncodedTicket;
    }

    @Override
    public void clearCredentials() {
        this.base64EncodedTicket = null;
    }

    @Override
    public int hashCode() {
        return Objects.hash(principalName, base64EncodedTicket);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;
        if (other == null)
            return false;
        if (getClass() != other.getClass())
            return false;
        final KerberosAuthenticationToken otherKerbToken = (KerberosAuthenticationToken) other;
        return Objects.equals(otherKerbToken.principal(), principal()) && Objects.equals(otherKerbToken.credentials(), credentials());
    }

}
