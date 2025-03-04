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
import java.util.function.Function;

public class DelegatingStorageRetryStrategy<T> implements StorageRetryStrategy {

    private final ResultRetryAlgorithm<T> idempotentRetryAlgorithm;
    private final ResultRetryAlgorithm<T> nonIdempotentRetryAlgorithm;

    /**
     * @param delegate The delegate {@link StorageRetryStrategy}
     * @param decorator The function applied to both idempotent and non-idempotent {@link ResultRetryAlgorithm}s
     */
    @SuppressWarnings("unchecked")
    public DelegatingStorageRetryStrategy(
        StorageRetryStrategy delegate,
        Function<ResultRetryAlgorithm<T>, ResultRetryAlgorithm<T>> decorator
    ) {
        this.idempotentRetryAlgorithm = decorator.apply((ResultRetryAlgorithm<T>) delegate.getIdempotentHandler());
        this.nonIdempotentRetryAlgorithm = decorator.apply((ResultRetryAlgorithm<T>) delegate.getNonidempotentHandler());
    }

    @Override
    public ResultRetryAlgorithm<?> getIdempotentHandler() {
        return idempotentRetryAlgorithm;
    }

    @Override
    public ResultRetryAlgorithm<?> getNonidempotentHandler() {
        return nonIdempotentRetryAlgorithm;
    }

    public static class DelegatingResultRetryAlgorithm<R> implements ResultRetryAlgorithm<R> {

        protected final ResultRetryAlgorithm<R> delegate;

        public DelegatingResultRetryAlgorithm(ResultRetryAlgorithm<R> delegate) {
            this.delegate = delegate;
        }

        @Override
        public TimedAttemptSettings createNextAttempt(Throwable prevThrowable, R prevResponse, TimedAttemptSettings prevSettings) {
            return delegate.createNextAttempt(prevThrowable, prevResponse, prevSettings);
        }

        @Override
        public boolean shouldRetry(Throwable prevThrowable, R prevResponse) throws CancellationException {
            return delegate.shouldRetry(prevThrowable, prevResponse);
        }
    }
}
