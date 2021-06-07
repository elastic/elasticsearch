/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.ssl;

import javax.net.ssl.SSLParameters;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The client authentication mode that is used for SSL servers.
 */
public enum SslClientAuthenticationMode {

    /**
     * Never request a client certificate.
     */
    NONE() {
        public boolean enabled() {
            return false;
        }

        public void configure(SSLParameters sslParameters) {
            // nothing to do here
            assert sslParameters.getWantClientAuth() == false;
            assert sslParameters.getNeedClientAuth() == false;
        }
    },
    /**
     * Request a client certificate, but do not enforce that one is provided.
     */
    OPTIONAL() {
        public boolean enabled() {
            return true;
        }

        public void configure(SSLParameters sslParameters) {
            sslParameters.setWantClientAuth(true);
        }
    },
    /**
     * Request and require a client certificate.
     */
    REQUIRED() {
        public boolean enabled() {
            return true;
        }

        public void configure(SSLParameters sslParameters) {
            sslParameters.setNeedClientAuth(true);
        }
    };

    /**
     * @return true if client authentication is enabled
     */
    public abstract boolean enabled();

    /**
     * Configure client authentication of the provided {@link SSLParameters}
     */
    public abstract void configure(SSLParameters sslParameters);

    private static final Map<String, SslClientAuthenticationMode> LOOKUP = Collections.unmodifiableMap(buildLookup());

    static Map<String, SslClientAuthenticationMode> buildLookup() {
        final Map<String, SslClientAuthenticationMode> map = new LinkedHashMap<>(3);
        map.put("none", NONE);
        map.put("optional", OPTIONAL);
        map.put("required", REQUIRED);
        return map;
    }

    public static SslClientAuthenticationMode parse(String value) {
        final SslClientAuthenticationMode mode = LOOKUP.get(value.toLowerCase(Locale.ROOT));
        if (mode == null) {
            final String allowedValues = LOOKUP.keySet().stream().collect(Collectors.joining(","));
            throw new SslConfigException("could not resolve ssl client authentication, unknown value ["
                + value + "], recognised values are [" + allowedValues + "]");
        }
        return mode;
    }
}
