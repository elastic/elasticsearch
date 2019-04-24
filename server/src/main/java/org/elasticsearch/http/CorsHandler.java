/*
 * Copyright 2013 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
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

package org.elasticsearch.http;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

public class CorsHandler {

    public static final String ANY_ORIGIN = "*";
    private static final String ORIGIN = "origin";
    private static final String HOST = "host";
    private static final String ACCESS_CONTROL_REQUEST_METHOD = "access-control-request-method";
    private static final String ACCESS_CONTROL_ALLOW_ORIGIN = "access-control-allow-origin";
    private static final String ACCESS_CONTROL_ALLOW_CREDENTIALS = "access-control-allow-credentials";
    private static final Pattern SCHEME_PATTERN = Pattern.compile("^https?://");
    private final Config config;

    public CorsHandler(final Config config) {
        if (config == null) {
            throw new NullPointerException();
        }
        this.config = config;
    }

    public HttpResponse handleRequest(HttpRequest request) {
        if (config.isCorsSupportEnabled()) {
            if (isPreflightRequest(request)) {
                return createPreflightResponse(request);
            }

            // If there is no origin, this is not a CORS request.
            // TODO: Only getting first
            final String origin = request.getHeaders().get(ORIGIN).get(0);
            if (Strings.isNullOrEmpty(origin) == false && validOrigin(request, origin) == false) {
                return request.createResponse(RestStatus.FORBIDDEN, BytesArray.EMPTY);
            }
        }

        return null;
    }

    public void setCorsResponseHeaders(HttpRequest request, HttpResponse resp) {
        if (config.isCorsSupportEnabled() == false) {
            return;
        }
        String originHeader = request.getHeaders().get(ORIGIN).get(0);
        // If there is no origin, this is not a CORS request.
        if (Strings.isNullOrEmpty(originHeader) == false) {
            final String originHeaderVal;
            if (config.isAnyOriginSupported()) {
                originHeaderVal = ANY_ORIGIN;
            } else if (config.isOriginAllowed(originHeader) || isSameOrigin(originHeader, request.getHeaders().get(HOST).get(0))) {
                originHeaderVal = originHeader;
            } else {
                originHeaderVal = null;
            }
            if (originHeaderVal != null) {
                resp.addHeader(ACCESS_CONTROL_ALLOW_ORIGIN, originHeaderVal);
            }
        }
        if (config.isCredentialsAllowed()) {
            resp.addHeader(ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");
        }
    }

    private boolean validOrigin(HttpRequest request, String origin) {
        if (config.isAnyOriginSupported()) {
            return true;
        }

        if (Strings.isNullOrEmpty(origin)) {
            // Not a CORS request so we cannot validate it. It may be a non CORS request.
            return true;
        }

        // if the origin is the same as the host of the request, then allow
        // TODO: I think we want to remove this.
        if (isSameOrigin(origin, request.getHeaders().get(HOST).get(0))) {
            return true;
        }

        return config.isOriginAllowed(origin);
    }

    private HttpResponse createPreflightResponse(HttpRequest request) {
        return null;
    }

    private static boolean isPreflightRequest(final HttpRequest request) {
        Map<String, List<String>> headers = request.getHeaders();
        return request.method().equals(RestRequest.Method.OPTIONS) &&
            headers.containsKey(ORIGIN) &&
            headers.containsKey(ACCESS_CONTROL_REQUEST_METHOD);
    }

    private static boolean isSameOrigin(final String origin, final String host) {
        if (Strings.isNullOrEmpty(host)) {
            return false;
        } else {
            // strip protocol from origin
            final String originDomain = SCHEME_PATTERN.matcher(origin).replaceFirst("");
            return host.equals(originDomain);
        }
    }

    public static class Config {

        private final boolean enabled;
        private final Optional<Set<String>> origins;
        private final Optional<Pattern> pattern;
        private final boolean anyOrigin;
        private final boolean credentialsAllowed;
        private final Set<RestRequest.Method> allowedRequestMethods;
        private final Set<String> allowedRequestHeaders;

        public Config(Builder builder) {
            this.enabled = builder.enabled;
            origins = builder.origins.map(HashSet::new);
            pattern = builder.pattern;
            anyOrigin = builder.anyOrigin;
            this.credentialsAllowed = builder.allowCredentials;
            // TODO: Broken Currently
            this.allowedRequestMethods = Collections.unmodifiableSet(builder.requestMethods);
            this.allowedRequestHeaders = Collections.unmodifiableSet(builder.requestHeaders);
        }

        public static Config disabled() {
            Builder builder = new Builder();
            builder.enabled = false;
            return new Config(builder);
        }

        public boolean isCorsSupportEnabled() {
            return enabled;
        }

        public boolean isAnyOriginSupported() {
            return anyOrigin;
        }

        public boolean isOriginAllowed(String origin) {
            if (origins.isPresent()) {
                return origins.get().contains(origin);
            } else if (pattern.isPresent()) {
                return pattern.get().matcher(origin).matches();
            }
            return false;
        }

        public boolean isCredentialsAllowed() {
            return credentialsAllowed;
        }

        public static class Builder {

            private boolean enabled = true;
            private Optional<Set<String>> origins;
            private Optional<Pattern> pattern;
            private final boolean anyOrigin;
            private boolean allowCredentials = false;
            long maxAge;
            private final Set<RestRequest.Method> requestMethods = new HashSet<>();
            private final Set<String> requestHeaders = new HashSet<>();

            private Builder() {
                anyOrigin = true;
                origins = Optional.empty();
                pattern = Optional.empty();
            }

            private Builder(final String... origins) {
                this.origins = Optional.of(new LinkedHashSet<>(Arrays.asList(origins)));
                pattern = Optional.empty();
                anyOrigin = false;
            }

            private Builder(final Pattern pattern) {
                this.pattern = Optional.of(pattern);
                origins = Optional.empty();
                anyOrigin = false;
            }

            public static Builder forOrigins(final String... origins) {
                return new Builder(origins);
            }

            public static Builder forAnyOrigin() {
                return new Builder();
            }

            public static Builder forPattern(Pattern pattern) {
                return new Builder(pattern);
            }

            public Builder allowCredentials() {
                this.allowCredentials = true;
                return this;
            }


            public Builder allowedRequestMethods(RestRequest.Method[] methods) {
                requestMethods.addAll(Arrays.asList(methods));
                return this;
            }

            public Builder maxAge(int maxAge) {
                this.maxAge = maxAge;
                return this;
            }

            public Builder allowedRequestHeaders(String[] headers) {
                requestHeaders.addAll(Arrays.asList(headers));
                return this;
            }

            public Config build() {
                return new Config(this);
            }
        }
    }
}
