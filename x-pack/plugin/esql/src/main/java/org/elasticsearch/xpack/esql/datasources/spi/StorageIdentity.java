/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.spi;

/**
 * Identifies the storage configuration (endpoint, credential identity) a {@link StorageObject} was
 * obtained from. Used as a component of {@code FooterByteCache.Key}: two objects with the same
 * {@code StorageIdentity}, path, and length may share a cache entry; objects with different
 * identities must not.
 * <p>
 * Implementations must provide correct {@link Object#equals} and {@link Object#hashCode} —
 * records satisfy this automatically and are the recommended implementation vehicle.
 * </p>
 */
public interface StorageIdentity {

    /**
     * Shared identity for providers with a single global configuration (local files, GCS with one
     * service account, etc.). Providers that can be instantiated with different credential
     * configurations in the same JVM must return a distinct implementation.
     */
    StorageIdentity GLOBAL = new Global();

    /** Default {@link StorageIdentity} for single-configuration providers. */
    record Global() implements StorageIdentity {}
}
