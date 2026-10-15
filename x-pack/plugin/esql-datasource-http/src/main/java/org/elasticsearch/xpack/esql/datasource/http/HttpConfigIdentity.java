/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasource.http;

import org.elasticsearch.xpack.esql.datasources.spi.StorageIdentity;

import java.util.TreeMap;

/**
 * Custom headers are the only per-data-source credential HTTP sends, so they scope the footer cache.
 * Held as a SHA-256 digest: headers typically carry {@code Authorization} values, and records print
 * their fields in {@code toString}.
 */
record HttpConfigIdentity(String customHeadersDigest) implements StorageIdentity {

    static HttpConfigIdentity of(HttpConfiguration config) {
        // NUL cannot appear in an HTTP header name or value, so it separates entries unambiguously.
        StringBuilder canonical = new StringBuilder();
        new TreeMap<>(config.customHeaders()).forEach((name, value) -> canonical.append(name).append('\0').append(value).append('\0'));
        return new HttpConfigIdentity(StorageIdentity.digestSecret(canonical.toString()));
    }
}
