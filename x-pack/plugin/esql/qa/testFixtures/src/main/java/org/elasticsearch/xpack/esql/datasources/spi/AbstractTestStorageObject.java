/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

/**
 * Convenience base for test-only {@link StorageObject} implementations that have a single,
 * global storage configuration (in-memory, local file, etc.) and do not exercise
 * credential-scoped footer-cache isolation.
 * <p>
 * This class is intentionally in test-fixture scope only; it must not be used in
 * production code. Production leaf implementations must implement
 * {@link StorageObject#storageIdentity()} directly and return the appropriate
 * per-credential identity.
 */
public abstract class AbstractTestStorageObject implements StorageObject {

    private record NoopIdentity() implements StorageIdentity {}

    /** Shared identity for test storage objects with no credential isolation. */
    public static final StorageIdentity NOOP = new NoopIdentity();

    @Override
    public StorageIdentity storageIdentity() {
        return NOOP;
    }
}
