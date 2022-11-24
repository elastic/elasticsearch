/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.nio.CharBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.security.authc.ApiKeyService.API_KEY_SCHEME;

public record RcsClusterCredentials(String scheme, SecureString value) {

    public static final String RCS_CLUSTER_CREDENTIAL_HEADER_KEY = "_remote_access_cluster_credential";

    public static final List<String> RCS_SUPPORTED_AUTHENTICATION_SCHEMES = Collections.singletonList(API_KEY_SCHEME);
    private static final List<CharSequence> SUPPORTED_PREFIXES_CHAR_SEQUENCES = RCS_SUPPORTED_AUTHENTICATION_SCHEMES.stream()
        .map(p -> CharBuffer.wrap(p).subSequence(0, p.length()))
        .collect(Collectors.toList());

    public RcsClusterCredentials {
        if (Strings.isEmpty(scheme)) {
            throw new IllegalArgumentException("Empty scheme not supported");
        } else if (Strings.isEmpty(value)) {
            throw new IllegalArgumentException("Empty value not supported");
        } else if (RCS_SUPPORTED_AUTHENTICATION_SCHEMES.contains(scheme) == false) {
            throw new IllegalArgumentException(
                "Unsupported scheme [" + scheme + "], supported schemes are " + RCS_SUPPORTED_AUTHENTICATION_SCHEMES
            );
        }
    }

    public static RcsClusterCredentials readFromContextHeader(final ThreadContext ctx) {
        return decode(ctx.getHeader(RCS_CLUSTER_CREDENTIAL_HEADER_KEY));
    }

    public static void writeToContextHeader(final ThreadContext threadContext, final RcsClusterCredentials schemeAndCredentials) {
        try (SecureString encoded = encode(schemeAndCredentials)) {
            threadContext.putHeader(RCS_CLUSTER_CREDENTIAL_HEADER_KEY, encoded.toString());
        }
    }

    public static void writeToContextHeader(final ThreadContext threadContext, final SecureString schemeAndCredentials) {
        final RcsClusterCredentials schemeAndCredentialsIfValid = decode(schemeAndCredentials);
        if (schemeAndCredentialsIfValid != null) {
            threadContext.putHeader(RCS_CLUSTER_CREDENTIAL_HEADER_KEY, schemeAndCredentials.toString());
        }
    }

    // encoded = scheme + " " + value
    public static SecureString encode(final RcsClusterCredentials schemeAndCredentials) {
        if (schemeAndCredentials != null) {
            final String scheme = schemeAndCredentials.scheme();
            final SecureString value = schemeAndCredentials.value();
            final char[] schemeAndCredentialsChars = new char[scheme.length() + 1 + value.length()];
            int i = 0;
            for (int s = 0; s < scheme.length(); s++) {
                schemeAndCredentialsChars[i++] = scheme.charAt(s);
            }
            schemeAndCredentialsChars[i++] = ' ';
            for (int c = 0; c < value.length(); c++) {
                schemeAndCredentialsChars[i++] = value.charAt(c);
            }
            return new SecureString(schemeAndCredentialsChars);
        }
        return null;
    }

    public static RcsClusterCredentials decode(final CharSequence schemeAndValue) {
        if (Strings.isEmpty(schemeAndValue)) {
            throw new IllegalArgumentException("Empty value not supported");
        }
        for (final CharSequence supportedPrefixChars : SUPPORTED_PREFIXES_CHAR_SEQUENCES) {
            if (startsWith(schemeAndValue, supportedPrefixChars)) {
                // found a value scheme
                if (schemeAndValue.length() == supportedPrefixChars.length()) {
                    // Example: "ApiKey" lacks a space and value after the prefix, so reject it
                    throw new IllegalArgumentException("Missing space and value after scheme [" + supportedPrefixChars + "]");
                } else if (schemeAndValue.charAt(supportedPrefixChars.length()) != ' ') {
                    // Example: "ApiKey-value" does not have the expected space between scheme and value
                    throw new IllegalArgumentException("Missing space after scheme [" + supportedPrefixChars + "]");
                } else if (schemeAndValue.length() == supportedPrefixChars.length() + 1) {
                    // Example: "ApiKey " lacks a value after the prefix, so reject it
                    throw new IllegalArgumentException("Missing value after scheme [" + supportedPrefixChars + "] and space");
                }
                final char[] valueChars = new char[schemeAndValue.length() - supportedPrefixChars.length() - 1];
                int i = 0;
                for (int c = supportedPrefixChars.length() + 1; c < schemeAndValue.length(); c++) {
                    valueChars[i++] = schemeAndValue.charAt(c);
                }
                return new RcsClusterCredentials(supportedPrefixChars.toString(), new SecureString(valueChars));
            }
        }
        throw new IllegalArgumentException("No supported scheme found, supported schemes are " + RCS_SUPPORTED_AUTHENTICATION_SCHEMES);
    }

    public static boolean startsWith(final CharSequence value, final CharSequence prefix) {
        if ((value == null) || (prefix == null)) {
            return false;
        }
        final int valueLength = value.length();
        final int prefixLength = prefix.length();
        if (valueLength < prefixLength) {
            return false;
        }
        for (int i = 0; i < prefixLength; i++) {
            final char valueChar = value.charAt(i);
            final char prefixChar = prefix.charAt(i);
            if (valueChar != prefixChar) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof RcsClusterCredentials that) {
            return Objects.equals(this.scheme, that.scheme) && Objects.equals(this.value, that.value);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return this.scheme.hashCode() + this.value.hashCode();
    }

    @Override
    public String toString() {
        return this.scheme + " REDACTED";
    }
}
