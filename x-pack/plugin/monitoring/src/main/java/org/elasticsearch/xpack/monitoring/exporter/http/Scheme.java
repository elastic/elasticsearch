/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.elasticsearch.client.RestClient;

import java.util.Locale;

/**
 * {@code Scheme} provides the list of supported {@code URI} schemes (aka protocols) for working with Elasticsearch via the
 * {@link RestClient}.
 *
 * @see HttpHostBuilder
 */
public enum Scheme {

    /**
     * HTTP is the default {@linkplain Scheme scheme} used by Elasticsearch.
     */
    HTTP("http"),
    /**
     * HTTPS is the secure form of {@linkplain #HTTP http}, which requires that Elasticsearch be using X-Pack Security with TLS/SSL or
     * a similar securing mechanism.
     */
    HTTPS("https");

    private final String scheme;

    Scheme(final String scheme) {
        this.scheme = scheme;
    }

    @Override
    public String toString() {
        return scheme;
    }

    /**
     * Determine the {@link Scheme} from the {@code scheme}.
     * <pre><code>
     * Scheme http = Scheme.fromString("http");
     * Scheme https = Scheme.fromString("https");
     * Scheme httpsCaps = Scheme.fromString("HTTPS"); // same as https
     * </code></pre>
     *
     * @param scheme The scheme to check.
     * @return Never {@code null}.
     * @throws NullPointerException if {@code scheme} is {@code null}.
     * @throws IllegalArgumentException if the {@code scheme} is not supported.
     */
    public static Scheme fromString(final String scheme) {
        switch (scheme.toLowerCase(Locale.ROOT)) {
            case "http":
                return HTTP;
            case "https":
                return HTTPS;
        }

        throw new IllegalArgumentException("unsupported scheme: [" + scheme + "]");
    }

}
