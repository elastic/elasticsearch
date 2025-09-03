/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import com.google.api.gax.retrying.ResultRetryAlgorithm;
import com.google.api.gax.retrying.TimedAttemptSettings;
import com.google.cloud.storage.StorageRetryStrategy;

import java.util.concurrent.CancellationException;

public class ShouldRetryDecorator<T> implements StorageRetryStrategy {

    private final ResultRetryAlgorithm<T> idempotentRetryAlgorithm;
    private final ResultRetryAlgorithm<T> nonIdempotentRetryAlgorithm;

    /**
     * Decorate the should-retry logic for the specified {@link StorageRetryStrategy}
     *
     * @param delegate The underling storage retry strategy
     * @param shouldRetryDecorator The decorated behaviour of {@link ResultRetryAlgorithm#shouldRetry(Throwable, Object)}
     * @return A decorated {@link StorageRetryStrategy}
     */
    public static StorageRetryStrategy decorate(StorageRetryStrategy delegate, Decorator<?> shouldRetryDecorator) {
        return new ShouldRetryDecorator<>(delegate, shouldRetryDecorator);
    }

    /**
     * The logic to use for {@link ResultRetryAlgorithm#shouldRetry(Throwable, Object)}
     */
    public interface Decorator<R> {
        boolean shouldRetry(Throwable prevThrowable, R prevResponse, ResultRetryAlgorithm<R> delegate);
    }

    /**
     * @param delegate The delegate {@link StorageRetryStrategy}
     * @param shouldRetryDecorator The function to call for shouldRetry for idempotent and non-idempotent requests
     */
    @SuppressWarnings("unchecked")
    private ShouldRetryDecorator(StorageRetryStrategy delegate, Decorator<T> shouldRetryDecorator) {
        this.idempotentRetryAlgorithm = new DelegatingResultRetryAlgorithm<>(
            (ResultRetryAlgorithm<T>) delegate.getIdempotentHandler(),
            shouldRetryDecorator
        );
        this.nonIdempotentRetryAlgorithm = new DelegatingResultRetryAlgorithm<>(
            (ResultRetryAlgorithm<T>) delegate.getNonidempotentHandler(),
            shouldRetryDecorator
        );
    }

    @Override
    public ResultRetryAlgorithm<?> getIdempotentHandler() {
        return idempotentRetryAlgorithm;
    }

    @Override
    public ResultRetryAlgorithm<?> getNonidempotentHandler() {
        return nonIdempotentRetryAlgorithm;
    }

    private record DelegatingResultRetryAlgorithm<R>(ResultRetryAlgorithm<R> delegate, Decorator<R> shouldRetryDecorator)
        implements
            ResultRetryAlgorithm<R> {

        @Override
        public TimedAttemptSettings createNextAttempt(Throwable prevThrowable, R prevResponse, TimedAttemptSettings prevSettings) {
            return delegate.createNextAttempt(prevThrowable, prevResponse, prevSettings);
        }

        @Override
        public boolean shouldRetry(Throwable prevThrowable, R prevResponse) throws CancellationException {
            return shouldRetryDecorator.shouldRetry(prevThrowable, prevResponse, delegate);
        }
    }
}
