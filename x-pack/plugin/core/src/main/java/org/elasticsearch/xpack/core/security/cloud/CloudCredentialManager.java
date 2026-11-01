/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.cloud;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;

/**
 * Cloud credential management for runtime handling (read of the active {@link ThreadContext}),
 * persistence (decoding a {@link PersistedCloudCredential} envelope into a portable
 * {@link CloudCredential}), and wrapping {@link Client}s to authenticate with a given credential.
 */
public interface CloudCredentialManager {

    /**
     * Checks if there is a cloud credential in the thread context.
     */
    boolean hasCloudManagedCredential(ThreadContext threadContext);

    /**
     * Extracts the caller's cloud credential from {@code threadContext}, or returns {@code null}
     * when there is none.
     */
    @Nullable
    CloudCredential extractCloudManagedCredential(ThreadContext threadContext);

    /**
     * Decodes a persisted credential into runtime {@link CloudCredential} form. Fails if the
     * envelope cannot be decoded. The caller owns the returned credential and must close it.
     */
    CloudCredential toCloudCredential(PersistedCloudCredential persisted);

    /**
     * Returns a cloud-credentials-aware {@link Client} that authenticates every action with the
     * given credential. Returns {@code delegate} when {@code credential} is null. The credential
     * must remain open for the lifetime of the returned client.
     */
    Client wrapClient(Client delegate, @Nullable CloudCredential credential);

    /**
     * Returns a cloud-credentials-aware {@link Client} that authenticates every action with the
     * given persisted credential. Returns {@code delegate} when {@code persisted} is null. The
     * credential must remain open for the lifetime of the returned client.
     */
    Client wrapClient(Client delegate, @Nullable PersistedCloudCredential persisted);

    /**
     * No-op default used when no real implementation is loaded.
     */
    class Noop implements CloudCredentialManager {

        @Override
        public boolean hasCloudManagedCredential(ThreadContext threadContext) {
            return false;
        }

        @Override
        public CloudCredential extractCloudManagedCredential(ThreadContext threadContext) {
            return null;
        }

        @Override
        public CloudCredential toCloudCredential(PersistedCloudCredential persisted) {
            throw new UnsupportedOperationException("cloud-managed credential decoding is not available");
        }

        @Override
        public Client wrapClient(Client delegate, @Nullable CloudCredential credential) {
            return delegate;
        }

        @Override
        public Client wrapClient(Client delegate, @Nullable PersistedCloudCredential persisted) {
            return delegate;
        }
    }
}
