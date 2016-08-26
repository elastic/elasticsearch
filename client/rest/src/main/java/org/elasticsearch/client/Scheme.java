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
package org.elasticsearch.client;

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
