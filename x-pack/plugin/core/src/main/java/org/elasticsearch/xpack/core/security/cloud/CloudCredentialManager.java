/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.cloud;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.FilterClient;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;

import java.util.Objects;

/**
 * Cloud credential management that deals with handling credentials at runtime (write and read from thread context)
 * and persistence of the cloud credentials (read from persisted form).
 */
public interface CloudCredentialManager {

    /**
     * Checks if there is a cloud credential in the thread context.
     */
    boolean hasCloudManagedCredential(ThreadContext threadContext);

    /**
     * Extracts the caller's UIAM cloud credential from {@code threadContext}.
     * <p>
     * <b>Precondition:</b> {@link #hasCloudManagedCredential(ThreadContext)} must return {@code true}
     * for {@code threadContext}. Calling this method when no cloud credential is present in the active
     * identity throws {@link IllegalStateException}.
     *
     * @return the caller's cloud credential
     */
    CloudCredential extractCloudManagedCredential(ThreadContext threadContext);

    /**
     * Injects a previously-extracted {@link CloudCredential} into {@code threadContext} so cross-project
     * actions executed in that context.
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
     * Returns a cloud credentials-aware {@link Client}. On every {@code execute(...)}, calls {@code resolver.resolve()}
     * to obtain a {@link CloudCredential}, injects it via {@link #injectCloudManagedCredential}, then
     * delegates to {@code delegate}.
     */
    default Client wrapClient(Client delegate, @Nullable CloudCredentialResolver resolver) {
        if (resolver == null) {
            return delegate;
        }
        final CloudCredentialManager self = this;
        return new FilterClient(delegate) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                final CloudCredential credential;
                try {
                    credential = resolver.resolve();
                } catch (Exception e) {
                    listener.onFailure(e);
                    return;
                }
                if (credential == null) {
                    super.doExecute(action, request, listener);
                    return;
                }
                try {
                    self.injectCloudManagedCredential(threadPool().getThreadContext(), credential);
                } catch (Exception e) {
                    listener.onFailure(e);
                    return;
                }
                super.doExecute(action, request, listener);
            }
        };
    }

    /** Convenience: wraps via {@link #resolverOf(CloudCredential)}. Returns {@code delegate} when {@code credential} is null. */
    default Client wrapClient(Client delegate, @Nullable CloudCredential credential) {
        return wrapClient(delegate, credential == null ? null : resolverOf(credential));
    }

    /** Convenience: wraps via {@link #resolverOf(PersistedCloudCredential)}. Returns {@code delegate} when {@code persisted} is null. */
    default Client wrapClient(Client delegate, @Nullable PersistedCloudCredential persisted) {
        return wrapClient(delegate, persisted == null ? null : resolverOf(persisted));
    }

    /**
     * No-op default used when serverless security is not loaded.
     */
    class Default implements CloudCredentialManager {

        @Override
        public boolean hasCloudManagedCredential(ThreadContext threadContext) {
            return false;
        }

        @Override
        public CloudCredential extractCloudManagedCredential(ThreadContext threadContext) {
            throw new IllegalStateException("no cloud-managed credential extraction is supported by this implementation");
        }

        @Override
        public void injectCloudManagedCredential(ThreadContext threadContext, CloudCredential credential) {
            throw new UnsupportedOperationException("cloud-managed credential injection is not available");
        }

        @Override
        public CloudCredentialResolver resolverOf(PersistedCloudCredential persisted) {
            throw new UnsupportedOperationException("cloud-managed credential decoding is not available");
        }
    }
}
