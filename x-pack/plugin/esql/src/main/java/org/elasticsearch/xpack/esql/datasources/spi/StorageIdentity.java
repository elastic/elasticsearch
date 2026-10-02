/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.common.hash.MessageDigests;

import java.nio.charset.StandardCharsets;

/**
 * Identifies the storage configuration (endpoint, credential identity) a {@link StorageObject} was
 * obtained from. Used as a component of {@code FooterByteCache.Key}: two objects with the same
 * {@code StorageIdentity}, path, and length may share a cache entry; objects with different
 * identities must not.
 * <p>
 * Implementations must provide correct {@link Object#equals} and {@link Object#hashCode} —
 * records satisfy this automatically and are the recommended implementation vehicle.
 * </p>
 * <p>
 * There is deliberately no shared "global" identity: a provider with no per-data-source configuration
 * declares its own <em>private</em> singleton, so different provider types can never share cache entries
 * even when they produce the same path and length.
 * </p>
 * <p>
 * Secret settings must be part of the identity, but only through {@link #digestSecret}: cache keys
 * outlive the data source, and records print their fields in {@code toString}.
 * </p>
 */
public interface StorageIdentity {

    /**
     * Returns a new identity equal only to itself, for objects that must never share a cache entry:
     * in-memory slices and single-use streams, or providers built without a configuration.
     */
    static StorageIdentity unique() {
        return new StorageIdentity() {};
    }

    /**
     * Returns the hex SHA-256 digest of {@code secret}, or {@code null} when it is {@code null}, so that
     * identities stay equal exactly when their secrets are equal without holding the plaintext.
     */
    static String digestSecret(String secret) {
        return secret == null ? null : MessageDigests.toHexString(MessageDigests.sha256().digest(secret.getBytes(StandardCharsets.UTF_8)));
    }
}
