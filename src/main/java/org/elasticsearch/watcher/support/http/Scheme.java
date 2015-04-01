/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.http;

import org.elasticsearch.ElasticsearchIllegalArgumentException;

import java.util.Locale;

public enum Scheme {

    HTTP("http"),
    HTTPS("https");

    private final String scheme;

    Scheme(String scheme) {
        this.scheme = scheme;
    }

    public String scheme() {
        return scheme;
    }

    public static Scheme parse(String value) {
        value = value.toLowerCase(Locale.ROOT);
        switch (value) {
            case "http":
                return HTTP;
            case "https":
                return HTTPS;
            default:
                throw new ElasticsearchIllegalArgumentException("unsupported http scheme [" + value + "]");
        }
    }
}
