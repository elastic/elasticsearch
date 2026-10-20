/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.ResolveIdentityRequest;

import org.elasticsearch.logging.Logger;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Wraps an {@link AwsCredentialsProvider} and logs failures from {@link #resolveCredentials()} or
 * the asynchronous identity-resolution methods. Used inside {@link AwsCredentialsProvider} chains
 * so that earlier providers fail visibly before the chain falls through to the next link.
 *
 * <p>Ported from {@code repository-s3}'s {@code ErrorLoggingCredentialsProvider}.
 */
final class ErrorLoggingCredentialsProvider implements AwsCredentialsProvider {

    private final AwsCredentialsProvider delegate;
    private final Logger logger;

    ErrorLoggingCredentialsProvider(AwsCredentialsProvider delegate, Logger logger) {
        this.delegate = Objects.requireNonNull(delegate);
        this.logger = Objects.requireNonNull(logger);
    }

    @Override
    public AwsCredentials resolveCredentials() {
        try {
            return delegate.resolveCredentials();
        } catch (Exception e) {
            logger.error(() -> "Unable to load credentials from " + delegate, e);
            throw e;
        }
    }

    @Override
    public Class<AwsCredentialsIdentity> identityType() {
        return delegate.identityType();
    }

    private <T> T resultHandler(T result, Throwable exception) {
        if (exception != null) {
            logger.error(() -> "Unable to resolve identity from " + delegate, exception);
            if (exception instanceof Error error) {
                throw error;
            } else if (exception instanceof RuntimeException runtimeException) {
                throw runtimeException;
            } else {
                throw new RuntimeException(exception);
            }
        }
        return result;
    }

    @Override
    public CompletableFuture<AwsCredentialsIdentity> resolveIdentity(ResolveIdentityRequest request) {
        return delegate.resolveIdentity(request).handle(this::resultHandler);
    }

    @Override
    public CompletableFuture<? extends AwsCredentialsIdentity> resolveIdentity(Consumer<ResolveIdentityRequest.Builder> consumer) {
        return delegate.resolveIdentity(consumer).handle(this::resultHandler);
    }

    @Override
    public CompletableFuture<? extends AwsCredentialsIdentity> resolveIdentity() {
        return delegate.resolveIdentity().handle(this::resultHandler);
    }

    @Override
    public String toString() {
        return "ErrorLogging[" + delegate + "]";
    }
}
