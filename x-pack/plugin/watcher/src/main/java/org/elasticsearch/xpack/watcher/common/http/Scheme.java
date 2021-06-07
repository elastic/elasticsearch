/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.common.http;

import java.util.Locale;
import java.util.Objects;

public enum Scheme {

    HTTP("http", 80),
    HTTPS("https", 443);

    private final String scheme;
    private final int defaultPort;

    Scheme(String scheme, int defaultPort) {
        this.scheme = scheme;
        this.defaultPort = defaultPort;
    }

    public String scheme() {
        return scheme;
    }

    public int defaultPort() {
        return defaultPort;
    }

    public static Scheme parse(String value) {
        Objects.requireNonNull(value, "Scheme should not be Null");
        value = value.toLowerCase(Locale.ROOT);
        switch (value) {
            case "http":
                return HTTP;
            case "https":
                return HTTPS;
            default:
                throw new IllegalArgumentException("unsupported http scheme [" + value + "]");
        }
    }

    public String value() {
        return name().toLowerCase(Locale.ROOT);
    }
}
