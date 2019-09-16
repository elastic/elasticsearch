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

package org.elasticsearch.http;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_CREDENTIALS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_HEADERS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_METHODS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_ORIGIN;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ENABLED;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_MAX_AGE;

/**
 * This file is forked from the https://netty.io project. In particular it combines the following three
 * files: io.netty.handler.codec.http.cors.CorsHandler, io.netty.handler.codec.http.cors.CorsConfig, and
 * io.netty.handler.codec.http.cors.CorsConfigBuilder.
 *
 * It modifies the original netty code to operation on Elasticsearch http request/response abstractions.
 * Additionally, it removes CORS features that are not used by Elasticsearch.
 */
public class CorsHandler {

    public static final String ANY_ORIGIN = "*";
    public static final String ORIGIN = "origin";
    public static final String DATE = "date";
    public static final String VARY = "vary";
    public static final String ACCESS_CONTROL_REQUEST_METHOD = "access-control-request-method";
    public static final String ACCESS_CONTROL_ALLOW_ORIGIN = "access-control-allow-origin";

    private CorsHandler() {
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
            this.allowedRequestMethods = Collections.unmodifiableSet(builder.requestMethods);
            this.allowedRequestHeaders = Collections.unmodifiableSet(builder.requestHeaders);
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
            return "Config{" +
                "enabled=" + enabled +
                ", origins=" + origins +
                ", pattern=" + pattern +
                ", anyOrigin=" + anyOrigin +
                ", credentialsAllowed=" + credentialsAllowed +
                ", allowedRequestMethods=" + allowedRequestMethods +
                ", allowedRequestHeaders=" + allowedRequestHeaders +
                ", maxAge=" + maxAge +
                '}';
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

            public Config build() {
                return new Config(this);
            }
        }
    }

    public static Config disabled() {
        Config.Builder builder = new Config.Builder();
        builder.enabled = false;
        return new Config(builder);
    }

    public static Config fromSettings(Settings settings) {
        if (SETTING_CORS_ENABLED.get(settings) == false) {
            return disabled();
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
            .build();
        return config;
    }
}
