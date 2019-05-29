/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
            assert !sslParameters.getWantClientAuth();
            assert !sslParameters.getNeedClientAuth();
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
