/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;

import java.nio.CharBuffer;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.security.authc.ApiKeyService.API_KEY_SCHEME;

// TODO plural
public record RcsClusterCredential(String scheme, SecureString value) {

    private static final Logger LOGGER = LogManager.getLogger(SecurityServerTransportInterceptor.class);

    public static final Set<String> SUPPORTED_PREFIXES = Set.of(API_KEY_SCHEME);
    private static final Set<CharSequence> SUPPORTED_PREFIXES_CHAR_SEQUENCES = SUPPORTED_PREFIXES.stream()
        .map(p -> CharBuffer.wrap(p).subSequence(0, p.length()))
        .collect(Collectors.toSet());

    public RcsClusterCredential {
        if (Strings.isEmpty(scheme)) {
            throw new RuntimeException("Missing scheme");
        } else if (SUPPORTED_PREFIXES.contains(scheme) == false) {
            throw new RuntimeException(String.format("Unsupported scheme [%s], supported schemes are %s", scheme, SUPPORTED_PREFIXES));
        } else if (Strings.isEmpty(value)) {
            throw new RuntimeException("Missing value");
        }
    }

    public static RcsClusterCredential readFromContextHeader(final ThreadContext ctx) {
        return decode(ctx.getHeader(AuthenticationField.RCS_CLUSTER_CREDENTIAL_HEADER_KEY));
    }

    public static void writeToContextHeader(final ThreadContext threadContext, final RcsClusterCredential schemeAndCredentials) {
        try (SecureString encoded = encode(schemeAndCredentials)) {
            threadContext.putHeader(AuthenticationField.RCS_CLUSTER_CREDENTIAL_HEADER_KEY, encoded.toString());
        }
    }

    public static void writeToContextHeader(final ThreadContext threadContext, final SecureString schemeAndCredentials) {
        RcsClusterCredential schemeAndCredentialsIfValid = decode(schemeAndCredentials);
        if (schemeAndCredentialsIfValid != null) {
            threadContext.putHeader(AuthenticationField.RCS_CLUSTER_CREDENTIAL_HEADER_KEY, schemeAndCredentials.toString());
        }
    }

    // encoded = scheme + " " + value
    public static SecureString encode(final RcsClusterCredential schemeAndCredentials) {
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

    public static RcsClusterCredential decode(final CharSequence schemeAndValue) {
        if (Strings.isEmpty(schemeAndValue)) {
            LOGGER.warn("No scheme and value found");
            return null;
        }
        for (final CharSequence supportedPrefixChars : SUPPORTED_PREFIXES_CHAR_SEQUENCES) {
            if (startsWith(schemeAndValue, supportedPrefixChars)) {
                // found a value scheme
                if (schemeAndValue.length() <= supportedPrefixChars.length() + 1) {
                    // Example: "ApiKey" and "ApiKey " lack a value after the prefix, so reject them
                    LOGGER.warn("Empty value not allowed after scheme [{}]", supportedPrefixChars);
                    return null;
                } else if (schemeAndValue.charAt(schemeAndValue.length()) != ' ') {
                    // Example: "ApiKey-value" does not have the expected space between scheme and value
                    LOGGER.warn("Missing space after scheme [{}]", supportedPrefixChars);
                    return null;
                }
                final char[] valueChars = new char[schemeAndValue.length() - supportedPrefixChars.length() - 1];
                int i = 0;
                for (int c = supportedPrefixChars.length() + 1; c < schemeAndValue.length(); c++) {
                    valueChars[i++] = schemeAndValue.charAt(c);
                }
                return new RcsClusterCredential(supportedPrefixChars.toString(), new SecureString(valueChars));
            }
        }
        LOGGER.warn("No supported scheme found, supports schemes are {}", SUPPORTED_PREFIXES);
        return null;
    }

    public static boolean startsWith(final CharSequence value, final CharSequence prefix) {
        if ((value == null) || (prefix == null) || (value.length() < prefix.length())) {
            return false;
        }
        for (int i = 0; i < value.length(); i++) {
            if (value.charAt(i) != prefix.charAt(i)) {
                return false;
            }
        }
        return true;
    }
}
