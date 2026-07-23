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

import java.util.Objects;

/**
 * Cloud credential management for runtime handling (read and write of the active {@link ThreadContext})
 * and persistence (decoding a {@link PersistedCloudCredential} envelope into a usable
 * {@link CloudCredential}).
 */
public interface CloudCredentialManager {

    /**
     * Checks if there is a cloud credential in the thread context.
     */
    boolean hasCloudManagedCredential(ThreadContext threadContext);

    /**
     * Extracts the caller's cloud credential from {@code threadContext}.
     * <p>
     * <b>Precondition:</b> {@link #hasCloudManagedCredential(ThreadContext)} must return {@code true};
     * otherwise the call fails with an unchecked exception.
     *
     * @return the caller's cloud credential
     */
    CloudCredential extractCloudManagedCredential(ThreadContext threadContext);

    /**
     * Injects a {@link CloudCredential} into {@code threadContext} so downstream actions can
     * authenticate with it.
     */
    void injectCloudManagedCredential(ThreadContext threadContext, CloudCredential credential);

    /**
     * Returns a resolver backed by an in-memory credential.
     */
    default CloudCredentialResolver resolverOf(CloudCredential credential) {
        Objects.requireNonNull(credential, "credential must not be null");
        return () -> credential;
    }

    /**
     * Returns a resolver backed by a persisted credential.
     */
    CloudCredentialResolver resolverOf(PersistedCloudCredential persisted);

    /**
     * Returns a cloud-credentials-aware {@link Client}. On every {@code execute(...)} the
     * implementation obtains a {@link CloudCredential} from {@code resolver}, injects it into
     * the active {@link ThreadContext} via {@link #injectCloudManagedCredential}, and dispatches
     * to {@code delegate} under that isolated context.
     * <p>
     * The credential or persisted envelope captured by {@code resolver} must remain open for the
     * lifetime of the returned client.
     */
    Client wrapClient(Client delegate, CloudCredentialResolver resolver);

    /**
     * Wraps client and injects the cloud credential via {@link #resolverOf(CloudCredential)}. Returns {@code delegate} when
     * {@code credential} is null.
     */
    default Client wrapClient(Client delegate, @Nullable CloudCredential credential) {
        return credential == null ? delegate : wrapClient(delegate, resolverOf(credential));
    }

    /**
     * Wraps client and injects the cloud credential via {@link #resolverOf(PersistedCloudCredential)}.
     * Returns {@code delegate} when {@code persisted} is null.
     */
    default Client wrapClient(Client delegate, @Nullable PersistedCloudCredential persisted) {
        return persisted == null ? delegate : wrapClient(delegate, resolverOf(persisted));
    }

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
            throw new UnsupportedOperationException("cloud-managed credential extraction is not available");
        }

        @Override
        public void injectCloudManagedCredential(ThreadContext threadContext, CloudCredential credential) {
            throw new UnsupportedOperationException("cloud-managed credential injection is not available");
        }

        @Override
        public CloudCredentialResolver resolverOf(PersistedCloudCredential persisted) {
            throw new UnsupportedOperationException("cloud-managed credential decoding is not available");
        }

        @Override
        public Client wrapClient(Client delegate, CloudCredentialResolver resolver) {
            return delegate;
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
