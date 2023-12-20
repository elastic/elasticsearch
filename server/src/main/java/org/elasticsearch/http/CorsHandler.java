/*
 * @notice
 *
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
 *
 * =============================================================================
 * Modifications copyright Elasticsearch B.V.
 *
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.http;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.RestUtils;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_CREDENTIALS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_HEADERS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_METHODS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_ORIGIN;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ENABLED;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_EXPOSE_HEADERS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_MAX_AGE;

/**
 * This file is forked from the https://netty.io project. In particular it combines the following three
 * files: io.netty.handler.codec.http.cors.CorsHandler, io.netty.handler.codec.http.cors.CorsConfig, and
 * io.netty.handler.codec.http.cors.CorsConfigBuilder.
 *
 * It modifies the original netty code to operate on Elasticsearch http request/response abstractions.
 * Additionally, it removes CORS features that are not used by Elasticsearch.
 */
public class CorsHandler {

    public static final String ANY_ORIGIN = "*";
    public static final String ORIGIN = "origin";
    public static final String DATE = "date";
    public static final String VARY = "vary";
    public static final String HOST = "host";
    public static final String ACCESS_CONTROL_REQUEST_METHOD = "access-control-request-method";
    public static final String ACCESS_CONTROL_ALLOW_HEADERS = "access-control-allow-headers";
    public static final String ACCESS_CONTROL_ALLOW_CREDENTIALS = "access-control-allow-credentials";
    public static final String ACCESS_CONTROL_ALLOW_METHODS = "access-control-allow-methods";
    public static final String ACCESS_CONTROL_ALLOW_ORIGIN = "access-control-allow-origin";
    public static final String ACCESS_CONTROL_MAX_AGE = "access-control-max-age";
    public static final String ACCESS_CONTROL_EXPOSE_HEADERS = "access-control-expose-headers";

    private static final Pattern SCHEME_PATTERN = Pattern.compile("^https?://");
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss O", Locale.ENGLISH);
    private final Config config;

    public CorsHandler(Config config) {
        this.config = config;
    }

    public HttpResponse handleInbound(HttpRequest request) {
        if (config.isCorsSupportEnabled()) {
            if (isPreflightRequest(request)) {
                return handlePreflight(request);
            }

            if (validateOrigin(request) == false) {
                return forbidden(request);
            }
        }
        return null;
    }

    public void setCorsResponseHeaders(final HttpRequest httpRequest, final HttpResponse httpResponse) {
        if (config.isCorsSupportEnabled() == false) {
            return;
        }
        if (setOrigin(httpRequest, httpResponse)) {
            setAllowCredentials(httpResponse);
            setExposeHeaders(httpResponse);
        }
    }

    private HttpResponse handlePreflight(final HttpRequest request) {
        final HttpResponse response = request.createResponse(RestStatus.OK, BytesArray.EMPTY);
        if (setOrigin(request, response)) {
            setAllowMethods(response);
            setAllowHeaders(response);
            setAllowCredentials(response);
            setMaxAge(response);
            setPreflightHeaders(response);
            return response;
        } else {
            return forbidden(request);
        }
    }

    private static HttpResponse forbidden(final HttpRequest request) {
        HttpResponse response = request.createResponse(RestStatus.FORBIDDEN, BytesArray.EMPTY);
        response.addHeader("content-length", "0");
        return response;
    }

    private static boolean isSameOrigin(final String origin, final String host) {
        if (Strings.isNullOrEmpty(host) == false) {
            // strip protocol from origin
            final String originDomain = SCHEME_PATTERN.matcher(origin).replaceFirst("");
            if (host.equals(originDomain)) {
                return true;
            }
        }
        return false;
    }

    private static void setPreflightHeaders(final HttpResponse response) {
        response.addHeader(CorsHandler.DATE, dateTimeFormatter.format(ZonedDateTime.now(ZoneOffset.UTC)));
        response.addHeader("content-length", "0");
    }

    private boolean setOrigin(final HttpRequest request, final HttpResponse response) {
        String origin = getOrigin(request);
        if (Strings.isNullOrEmpty(origin) == false) {
            if (config.isAnyOriginSupported()) {
                if (config.isCredentialsAllowed()) {
                    setAllowOrigin(response, origin);
                    setVaryHeader(response);
                } else {
                    setAllowOrigin(response, ANY_ORIGIN);
                }
                return true;
            } else if (config.isOriginAllowed(origin) || isSameOrigin(origin, getHost(request))) {
                setAllowOrigin(response, origin);
                setVaryHeader(response);
                return true;
            }
        }
        return false;
    }

    private boolean validateOrigin(final HttpRequest request) {
        if (config.isAnyOriginSupported()) {
            return true;
        }

        final String origin = getOrigin(request);
        if (Strings.isNullOrEmpty(origin)) {
            // Not a CORS request so we cannot validate it. It may be a non CORS request.
            return true;
        }

        // if the origin is the same as the host of the request, then allow
        if (isSameOrigin(origin, getHost(request))) {
            return true;
        }

        return config.isOriginAllowed(origin);
    }

    private static String getOrigin(HttpRequest request) {
        List<String> headers = request.getHeaders().get(ORIGIN);
        if (headers == null || headers.isEmpty()) {
            return null;
        } else {
            return headers.get(0);
        }
    }

    private static String getHost(HttpRequest request) {
        List<String> headers = request.getHeaders().get(HOST);
        if (headers == null || headers.isEmpty()) {
            return null;
        } else {
            return headers.get(0);
        }
    }

    private static boolean isPreflightRequest(final HttpRequest request) {
        final Map<String, List<String>> headers = request.getHeaders();
        return request.method().equals(RestRequest.Method.OPTIONS)
            && headers.containsKey(ORIGIN)
            && headers.containsKey(ACCESS_CONTROL_REQUEST_METHOD);
    }

    private static void setVaryHeader(final HttpResponse response) {
        response.addHeader(VARY, ORIGIN);
    }

    private static void setAllowOrigin(final HttpResponse response, final String origin) {
        response.addHeader(ACCESS_CONTROL_ALLOW_ORIGIN, origin);
    }

    private void setAllowMethods(final HttpResponse response) {
        for (RestRequest.Method method : config.allowedRequestMethods()) {
            response.addHeader(ACCESS_CONTROL_ALLOW_METHODS, method.name().trim());
        }
    }

    private void setAllowHeaders(final HttpResponse response) {
        for (String header : config.allowedRequestHeaders) {
            response.addHeader(ACCESS_CONTROL_ALLOW_HEADERS, header);
        }
    }

    private void setExposeHeaders(final HttpResponse response) {
        for (String header : config.accessControlExposeHeaders) {
            response.addHeader(ACCESS_CONTROL_EXPOSE_HEADERS, header);
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

    public static class Config {

        private final boolean enabled;
        private final Optional<Set<String>> origins;
        private final Optional<Pattern> pattern;
        private final boolean anyOrigin;
        private final boolean credentialsAllowed;
        private final Set<RestRequest.Method> allowedRequestMethods;
        private final Set<String> allowedRequestHeaders;
        private final Set<String> accessControlExposeHeaders;
        private final long maxAge;

        public Config(Builder builder) {
            this.enabled = builder.enabled;
            origins = builder.origins.map(HashSet::new);
            pattern = builder.pattern;
            anyOrigin = builder.anyOrigin;
            this.credentialsAllowed = builder.allowCredentials;
            this.allowedRequestMethods = Collections.unmodifiableSet(builder.requestMethods);
            this.allowedRequestHeaders = Collections.unmodifiableSet(builder.requestHeaders);
            this.accessControlExposeHeaders = Collections.unmodifiableSet(builder.accessControlExposeHeaders);
            this.maxAge = builder.maxAge;
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

        public Set<RestRequest.Method> allowedRequestMethods() {
            return allowedRequestMethods;
        }

        public Set<String> allowedRequestHeaders() {
            return allowedRequestHeaders;
        }

        public long maxAge() {
            return maxAge;
        }

        public Optional<Set<String>> origins() {
            return origins;
        }

        @Override
        public String toString() {
            return "Config{"
                + "enabled="
                + enabled
                + ", origins="
                + origins
                + ", pattern="
                + pattern
                + ", anyOrigin="
                + anyOrigin
                + ", credentialsAllowed="
                + credentialsAllowed
                + ", allowedRequestMethods="
                + allowedRequestMethods
                + ", allowedRequestHeaders="
                + allowedRequestHeaders
                + ", accessControlExposeHeaders="
                + accessControlExposeHeaders
                + ", maxAge="
                + maxAge
                + '}';
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
            private final Set<String> accessControlExposeHeaders = new HashSet<>();

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

            static Builder forOrigins(final String... origins) {
                return new Builder(origins);
            }

            static Builder forAnyOrigin() {
                return new Builder();
            }

            static Builder forPattern(Pattern pattern) {
                return new Builder(pattern);
            }

            Builder allowCredentials() {
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

            public Builder accessControlExposeHeaders(String[] headers) {
                accessControlExposeHeaders.addAll(Arrays.asList(headers));
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

    public static Config buildConfig(Settings settings) {
        if (SETTING_CORS_ENABLED.get(settings) == false) {
            Config.Builder builder = new Config.Builder();
            builder.enabled = false;
            return new Config(builder);
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
            .map(s -> s.toUpperCase(Locale.ENGLISH))
            .map(RestRequest.Method::valueOf)
            .toArray(RestRequest.Method[]::new);
        Config config = builder.allowedRequestMethods(methods)
            .maxAge(SETTING_CORS_MAX_AGE.get(settings))
            .allowedRequestHeaders(Strings.tokenizeToStringArray(SETTING_CORS_ALLOW_HEADERS.get(settings), ","))
            .accessControlExposeHeaders(Strings.tokenizeToStringArray(SETTING_CORS_EXPOSE_HEADERS.get(settings), ","))
            .build();
        return config;
    }

    public static CorsHandler fromSettings(Settings settings) {
        return new CorsHandler(buildConfig(settings));
    }
}
