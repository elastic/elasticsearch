/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.ssl;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Represents the verification mode to be used for SSL connections.
 */
public enum SslVerificationMode {
    /**
     * Verify neither the hostname, nor the provided certificate.
     */
    NONE {
        @Override
        public boolean isHostnameVerificationEnabled() {
            return false;
        }

        @Override
        public boolean isCertificateVerificationEnabled() {
            return false;
        }
    },
    /**
     * Verify the provided certificate against the trust chain, but do not verify the hostname.
     */
    CERTIFICATE {
        @Override
        public boolean isHostnameVerificationEnabled() {
            return false;
        }

        @Override
        public boolean isCertificateVerificationEnabled() {
            return true;
        }
    },
    /**
     * Verify the provided certificate against the trust chain, and also verify that the hostname to which this client is connected
     * matches one of the Subject-Alternative-Names in the certificate.
     */
    FULL {
        @Override
        public boolean isHostnameVerificationEnabled() {
            return true;
        }

        @Override
        public boolean isCertificateVerificationEnabled() {
            return true;
        }
    };

    /**
     * @return true if hostname verification is enabled
     */
    public abstract boolean isHostnameVerificationEnabled();

    /**
     * @return true if certificate verification is enabled
     */
    public abstract boolean isCertificateVerificationEnabled();

    private static final Map<String, SslVerificationMode> LOOKUP = Collections.unmodifiableMap(buildLookup());

    private static Map<String, SslVerificationMode> buildLookup() {
        Map<String, SslVerificationMode> map = new LinkedHashMap<>(3);
        map.put("none", NONE);
        map.put("certificate", CERTIFICATE);
        map.put("full", FULL);
        return map;
    }

    public static SslVerificationMode parse(String value) {
        final SslVerificationMode mode = LOOKUP.get(value.toLowerCase(Locale.ROOT));
        if (mode == null) {
            final String allowedValues = LOOKUP.keySet().stream().collect(Collectors.joining(","));
            throw new SslConfigException(
                "could not resolve ssl client verification mode, unknown value ["
                    + value
                    + "], recognised values are ["
                    + allowedValues
                    + "]"
            );
        }
        return mode;
    }
}
