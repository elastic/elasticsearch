/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.cloud;

/**
 * Strategy for obtaining a {@link CloudCredential} on demand.
 * <p>
 * A {@code CloudCredentialSource} is constructed via the {@code sourceOf(...)} factory methods on
 * {@code CloudCredentialManager} and consumed by {@code CloudCredentialManager#wrapClient(...)} so
 * cross-project actions can fetch a fresh in-memory credential immediately before each
 * {@code Client#execute(...)}.
 * <p>
 * Resolution may be a no-op (when the source already holds an in-memory credential) or it may
 * decode / decrypt a persisted form. The encryption follow-up adds the decrypt path; today there
 * is only a trivial pass-through implementation. {@link #resolve()} is synchronous; implementations
 * throw an unchecked exception if resolution fails.
 *
 * @see CloudCredential
 * @see PersistedCloudCredential
 */
@FunctionalInterface
public interface CloudCredentialSource {

    /**
     * Returns the in-memory credential ready to inject into a thread context.
     *
     * @throws RuntimeException if resolution fails (e.g. decryption failure once the encryption
     *                          follow-up lands)
     */
    CloudCredential resolve();
}
