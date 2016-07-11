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

package org.elasticsearch.http.netty.cors;

import org.jboss.netty.handler.codec.http.DefaultHttpHeaders;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

/**
 * Configuration for Cross-Origin Resource Sharing (CORS).
 *
 * This class was lifted from the Netty project:
 *  https://github.com/netty/netty
 */
public final class CorsConfig {

    private final Optional<Set<String>> origins;
    private final Optional<Pattern> pattern;
    private final boolean anyOrigin;
    private final boolean enabled;
    private final boolean allowCredentials;
    private final long maxAge;
    private final Set<HttpMethod> allowedRequestMethods;
    private final Set<String> allowedRequestHeaders;
    private final boolean allowNullOrigin;
    private final Map<CharSequence, Callable<?>> preflightHeaders;
    private final boolean shortCircuit;

    CorsConfig(final CorsConfigBuilder builder) {
        origins = builder.origins.map(s -> new LinkedHashSet<>(s));
        pattern = builder.pattern;
        anyOrigin = builder.anyOrigin;
        enabled = builder.enabled;
        allowCredentials = builder.allowCredentials;
        maxAge = builder.maxAge;
        allowedRequestMethods = builder.requestMethods;
        allowedRequestHeaders = builder.requestHeaders;
        allowNullOrigin = builder.allowNullOrigin;
        preflightHeaders = builder.preflightHeaders;
        shortCircuit = builder.shortCircuit;
    }

    /**
     * Determines if support for CORS is enabled.
     *
     * @return {@code true} if support for CORS is enabled, false otherwise.
     */
    public boolean isCorsSupportEnabled() {
        return enabled;
    }

    /**
     * Determines whether a wildcard origin, '*', is supported.
     *
     * @return {@code boolean} true if any origin is allowed.
     */
    public boolean isAnyOriginSupported() {
        return anyOrigin;
    }

    /**
     * Returns the set of allowed origins.
     *
     * @return {@code Set} the allowed origins.
     */
    public Optional<Set<String>> origins() {
        return origins;
    }

    /**
     * Returns whether the input origin is allowed by this configuration.
     *
     * @return {@code true} if the origin is allowed, otherwise {@code false}
     */
    public boolean isOriginAllowed(final String origin) {
        if (origins.isPresent()) {
            return origins.get().contains(origin);
        } else if (pattern.isPresent()) {
            return pattern.get().matcher(origin).matches();
        }
        return false;
    }

    /**
     * Web browsers may set the 'Origin' request header to 'null' if a resource is loaded
     * from the local file system.
     *
     * If isNullOriginAllowed is true then the server will response with the wildcard for the
     * the CORS response header 'Access-Control-Allow-Origin'.
     *
     * @return {@code true} if a 'null' origin should be supported.
     */
    public boolean isNullOriginAllowed() {
        return allowNullOrigin;
    }

    /**
     * Determines if cookies are supported for CORS requests.
     *
     * By default cookies are not included in CORS requests but if isCredentialsAllowed returns
     * true cookies will be added to CORS requests. Setting this value to true will set the
     * CORS 'Access-Control-Allow-Credentials' response header to true.
     *
     * Please note that cookie support needs to be enabled on the client side as well.
     * The client needs to opt-in to send cookies by calling:
     * <pre>
     * xhr.withCredentials = true;
     * </pre>
     * The default value for 'withCredentials' is false in which case no cookies are sent.
     * Setting this to true will included cookies in cross origin requests.
     *
     * @return {@code true} if cookies are supported.
     */
    public boolean isCredentialsAllowed() {
        return allowCredentials;
    }

    /**
     * Gets the maxAge setting.
     *
     * When making a preflight request the client has to perform two request with can be inefficient.
     * This setting will set the CORS 'Access-Control-Max-Age' response header and enables the
     * caching of the preflight response for the specified time. During this time no preflight
     * request will be made.
     *
     * @return {@code long} the time in seconds that a preflight request may be cached.
     */
    public long maxAge() {
        return maxAge;
    }

    /**
     * Returns the allowed set of Request Methods. The Http methods that should be returned in the
     * CORS 'Access-Control-Request-Method' response header.
     *
     * @return {@code Set} of {@link HttpMethod}s that represent the allowed Request Methods.
     */
    public Set<HttpMethod> allowedRequestMethods() {
        return Collections.unmodifiableSet(allowedRequestMethods);
    }

    /**
     * Returns the allowed set of Request Headers.
     *
     * The header names returned from this method will be used to set the CORS
     * 'Access-Control-Allow-Headers' response header.
     *
     * @return {@code Set<String>} of strings that represent the allowed Request Headers.
     */
    public Set<String> allowedRequestHeaders() {
        return Collections.unmodifiableSet(allowedRequestHeaders);
    }

    /**
     * Returns HTTP response headers that should be added to a CORS preflight response.
     *
     * @return {@link HttpHeaders} the HTTP response headers to be added.
     */
    public HttpHeaders preflightResponseHeaders() {
        if (preflightHeaders.isEmpty()) {
            return HttpHeaders.EMPTY_HEADERS;
        }
        final HttpHeaders preflightHeaders = new DefaultHttpHeaders();
        for (Map.Entry<CharSequence, Callable<?>> entry : this.preflightHeaders.entrySet()) {
            final Object value = getValue(entry.getValue());
            if (value instanceof Iterable) {
                preflightHeaders.add(entry.getKey().toString(), (Iterable<?>) value);
            } else {
                preflightHeaders.add(entry.getKey().toString(), value);
            }
        }
        return preflightHeaders;
    }

    /**
     * Determines whether a CORS request should be rejected if it's invalid before being
     * further processing.
     *
     * CORS headers are set after a request is processed. This may not always be desired
     * and this setting will check that the Origin is valid and if it is not valid no
     * further processing will take place, and a error will be returned to the calling client.
     *
     * @return {@code true} if a CORS request should short-circuit upon receiving an invalid Origin header.
     */
    public boolean isShortCircuit() {
        return shortCircuit;
    }

    private static <T> T getValue(final Callable<T> callable) {
        try {
            return callable.call();
        } catch (final Exception e) {
            throw new IllegalStateException("Could not generate value for callable [" + callable + ']', e);
        }
    }

    @Override
    public String toString() {
        return  "CorsConfig[enabled=" + enabled +
                    ", origins=" + origins +
                    ", anyOrigin=" + anyOrigin +
                    ", isCredentialsAllowed=" + allowCredentials +
                    ", maxAge=" + maxAge +
                    ", allowedRequestMethods=" + allowedRequestMethods +
                    ", allowedRequestHeaders=" + allowedRequestHeaders +
                    ", preflightHeaders=" + preflightHeaders + ']';
    }
}
