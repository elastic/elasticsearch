/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.http;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

/**
 * Configuration for HTTP/HTTPS storage access.
 * Provides settings for timeouts, redirects, and custom headers.
 */
public final class HttpConfiguration {
    private final Duration connectTimeout;
    private final Duration requestTimeout;
    private final boolean followRedirects;
    private final Map<String, String> customHeaders;
    private final int maxRetries;

    /**
     * Creates a new HttpConfiguration with default settings.
     */
    public static HttpConfiguration defaults() {
        return new Builder().build();
    }

    /**
     * Creates a new builder for HttpConfiguration.
     */
    public static Builder builder() {
        return new Builder();
    }

    private HttpConfiguration(Builder builder) {
        if (builder.connectTimeout == null) {
            throw new IllegalArgumentException("connectTimeout cannot be null");
        }
        if (builder.requestTimeout == null) {
            throw new IllegalArgumentException("requestTimeout cannot be null");
        }
        if (builder.customHeaders == null) {
            throw new IllegalArgumentException("customHeaders cannot be null");
        }
        this.connectTimeout = builder.connectTimeout;
        this.requestTimeout = builder.requestTimeout;
        this.followRedirects = builder.followRedirects;
        this.customHeaders = Map.copyOf(builder.customHeaders);
        this.maxRetries = builder.maxRetries;
    }

    public Duration connectTimeout() {
        return connectTimeout;
    }

    public Duration requestTimeout() {
        return requestTimeout;
    }

    public boolean followRedirects() {
        return followRedirects;
    }

    public Map<String, String> customHeaders() {
        return customHeaders;
    }

    public int maxRetries() {
        return maxRetries;
    }

    public static final class Builder {
        private Duration connectTimeout = Duration.ofSeconds(30);
        private Duration requestTimeout = Duration.ofMinutes(5);
        private boolean followRedirects = true;
        private Map<String, String> customHeaders = Map.of();
        private int maxRetries = 3;

        private Builder() {}

        public Builder connectTimeout(Duration connectTimeout) {
            if (connectTimeout == null) {
                throw new IllegalArgumentException("connectTimeout cannot be null");
            }
            this.connectTimeout = connectTimeout;
            return this;
        }

        public Builder requestTimeout(Duration requestTimeout) {
            if (requestTimeout == null) {
                throw new IllegalArgumentException("requestTimeout cannot be null");
            }
            this.requestTimeout = requestTimeout;
            return this;
        }

        public Builder followRedirects(boolean followRedirects) {
            this.followRedirects = followRedirects;
            return this;
        }

        public Builder customHeaders(Map<String, String> customHeaders) {
            if (customHeaders == null) {
                throw new IllegalArgumentException("customHeaders cannot be null");
            }
            this.customHeaders = customHeaders;
            return this;
        }

        public Builder maxRetries(int maxRetries) {
            if (maxRetries < 0) {
                throw new IllegalArgumentException("maxRetries must be non-negative");
            }
            this.maxRetries = maxRetries;
            return this;
        }

        public HttpConfiguration build() {
            return new HttpConfiguration(this);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HttpConfiguration that = (HttpConfiguration) o;
        return followRedirects == that.followRedirects
            && maxRetries == that.maxRetries
            && Objects.equals(connectTimeout, that.connectTimeout)
            && Objects.equals(requestTimeout, that.requestTimeout)
            && Objects.equals(customHeaders, that.customHeaders);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectTimeout, requestTimeout, followRedirects, customHeaders, maxRetries);
    }

    @Override
    public String toString() {
        return "HttpConfiguration{"
            + "connectTimeout="
            + connectTimeout
            + ", requestTimeout="
            + requestTimeout
            + ", followRedirects="
            + followRedirects
            + ", customHeaders="
            + customHeaders
            + ", maxRetries="
            + maxRetries
            + '}';
    }
}
