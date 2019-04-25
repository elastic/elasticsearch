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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.RestUtils;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_CREDENTIALS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_HEADERS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_METHODS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_ORIGIN;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ENABLED;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_MAX_AGE;

public class CorsHandler {

    static final String ANY_ORIGIN = "*";
    private static final String DATE = "date";
    static final String ORIGIN = "origin";
    static final String VARY = "vary";
    private static final String ACCESS_CONTROL_REQUEST_METHOD = "access-control-request-method";
    private static final String ACCESS_CONTROL_ALLOW_METHODS = "access-control-allow-methods";
    private static final String ACCESS_CONTROL_ALLOW_HEADERS = "access-control-allow-headers";
    private static final String ACCESS_CONTROL_MAX_AGE = "access-control-max-age";
    static final String ACCESS_CONTROL_ALLOW_ORIGIN = "access-control-allow-origin";
    static final String ACCESS_CONTROL_ALLOW_CREDENTIALS = "access-control-allow-credentials";
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss O", Locale.ENGLISH);
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
            if (Strings.isNullOrEmpty(origin) == false && validOrigin(origin) == false) {
                return request.createResponse(RestStatus.FORBIDDEN, BytesArray.EMPTY);
            }
        }

        return null;
    }

    public void setCorsResponseHeaders(HttpRequest request, HttpResponse resp) {
        if (config.isCorsSupportEnabled()) {
            String originHeader = request.getHeaders().get(ORIGIN).get(0);
            setOrigin(resp, originHeader);
            if (config.isCredentialsAllowed()) {
                resp.addHeader(ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");
            }
        }
    }

    private boolean validOrigin(String origin) {
        if (config.isAnyOriginSupported()) {
            return true;
        }

        if (Strings.isNullOrEmpty(origin)) {
            // Not a CORS request so we cannot validate it. It may be a non CORS request.
            return true;
        }
        return config.isOriginAllowed(origin);
    }

    private HttpResponse createPreflightResponse(HttpRequest request) {
        final String origin = request.getHeaders().get(ORIGIN).get(0);
        if (Strings.isNullOrEmpty(origin) == false) {
            HttpResponse response = request.createResponse(RestStatus.OK, BytesArray.EMPTY);
            setOrigin(response, origin);
            setAllowMethods(response);
            setAllowHeaders(response);
            setAllowCredentials(response);
            setMaxAge(response);
            response.addHeader(DefaultRestChannel.CONTENT_LENGTH, "0");
            dateTimeFormatter.format(Instant.now());
            response.addHeader(DATE, dateTimeFormatter.format(ZonedDateTime.now(ZoneOffset.UTC)));
            return response;
        } else {
            return request.createResponse(RestStatus.FORBIDDEN, BytesArray.EMPTY);
        }
    }

    private void setOrigin(final HttpResponse response, final String origin) {
        // If there is no origin, this is not a CORS request.
        if (Strings.isNullOrEmpty(origin) == false) {
            if (config.isAnyOriginSupported()) {
                if (config.isCredentialsAllowed()) {
                    response.addHeader(ACCESS_CONTROL_ALLOW_ORIGIN, origin);
                    response.addHeader(VARY, ORIGIN);
                } else {
                    response.addHeader(ACCESS_CONTROL_ALLOW_ORIGIN, ANY_ORIGIN);
                }
            }
            if (config.isOriginAllowed(origin)) {
                response.addHeader(ACCESS_CONTROL_ALLOW_ORIGIN, origin);
                response.addHeader(VARY, ORIGIN);
            }
        }
    }

    private void setAllowMethods(final HttpResponse response) {
        for (RestRequest.Method method : config.allowedRequestMethods) {
            response.addHeader(ACCESS_CONTROL_ALLOW_METHODS, method.name().trim());
        }
    }

    private void setAllowHeaders(final HttpResponse response) {
        for (String header : config.allowedRequestHeaders) {
            response.addHeader(ACCESS_CONTROL_ALLOW_HEADERS, header);
        }
    }

    private void setAllowCredentials(final HttpResponse response) {
        if (config.isCredentialsAllowed()) {
            response.addHeader(ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");
        }
    }

    private void setMaxAge(final HttpResponse response) {
        response.addHeader(ACCESS_CONTROL_MAX_AGE, Long.toString(config.maxAge));
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
        private final long maxAge;

        public Config(Builder builder) {
            this.enabled = builder.enabled;
            origins = builder.origins.map(HashSet::new);
            pattern = builder.pattern;
            anyOrigin = builder.anyOrigin;
            this.credentialsAllowed = builder.allowCredentials;
            // TODO: We do not short circuit on these
            this.allowedRequestMethods = Collections.unmodifiableSet(builder.requestMethods);
            this.allowedRequestHeaders = Collections.unmodifiableSet(builder.requestHeaders);
            this.maxAge = builder.maxAge;
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

        private static class Builder {

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

    public static CorsHandler disabled() {
        Config.Builder builder = new Config.Builder();
        builder.enabled = false;
        return new CorsHandler(new Config(builder));
    }

    public static CorsHandler fromSettings(Settings settings) {
        if (SETTING_CORS_ENABLED.get(settings) == false) {
            return new CorsHandler(CorsHandler.Config.disabled());
        }
        String origin = SETTING_CORS_ALLOW_ORIGIN.get(settings);
        final CorsHandler.Config.Builder builder;
        if (Strings.isNullOrEmpty(origin)) {
            builder = CorsHandler.Config.Builder.forOrigins();
        } else if (origin.equals(CorsHandler.ANY_ORIGIN)) {
            builder = CorsHandler.Config.Builder.forAnyOrigin();
        } else {
            try {
                Pattern p = RestUtils.checkCorsSettingForRegex(origin);
                if (p == null) {
                    builder = CorsHandler.Config.Builder.forOrigins(RestUtils.corsSettingAsArray(origin));
                } else {
                    builder = CorsHandler.Config.Builder.forPattern(p);
                }
            } catch (PatternSyntaxException e) {
                throw new SettingsException("Bad regex in [" + SETTING_CORS_ALLOW_ORIGIN.getKey() + "]: [" + origin + "]", e);
            }
        }
        if (SETTING_CORS_ALLOW_CREDENTIALS.get(settings)) {
            builder.allowCredentials();
        }
        String[] strMethods = Strings.tokenizeToStringArray(SETTING_CORS_ALLOW_METHODS.get(settings), ",");
        RestRequest.Method[] methods = Arrays.stream(strMethods)
            .map(RestRequest.Method::valueOf)
            .toArray(RestRequest.Method[]::new);
        Config config = builder.allowedRequestMethods(methods)
            .maxAge(SETTING_CORS_MAX_AGE.get(settings))
            .allowedRequestHeaders(Strings.tokenizeToStringArray(SETTING_CORS_ALLOW_HEADERS.get(settings), ","))
            .build();
        return new CorsHandler(config);
    }
}
