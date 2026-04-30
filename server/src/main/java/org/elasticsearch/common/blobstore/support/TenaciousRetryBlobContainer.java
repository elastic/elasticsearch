/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.blobstore.support;

import joptsimple.internal.Strings;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.core.CheckedSupplier;
import org.elasticsearch.repositories.RepositoriesMetrics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.core.Strings.format;

/**
 * Cloud storage services use an eventual consistency model for identity, access control, and metadata.
 * As a result, they may occasionally return transient 403 Forbidden errors even when permissions are correctly configured.
 * This class wraps the blobContainer to provide a dedicated retry mechanism initially for handling these 403 errors.
 *
 */
public abstract class TenaciousRetryBlobContainer extends FilterBlobContainer {

    private static final Logger logger = LogManager.getLogger(TenaciousRetryBlobContainer.class);
    private final RepositoriesMetrics repositoriesMetrics;
    private static final int INITIAL_ATTEMPT = 1;
    public static final int MAX_SUPPRESSED_EXCEPTIONS = 10;
    protected final BlobContainer delegate;
    protected final String blobPath;

    public enum RetryMethod {
        LIST_BLOBS("listBlobs"),
        LIST_BLOBS_BY_PREFIX("listBlobsByPrefix"),
        CHILDREN("children");

        private final String name;

        RetryMethod(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    public TenaciousRetryBlobContainer(BlobContainer delegate, RepositoriesMetrics repositoriesMetrics) {
        super(delegate);
        this.repositoriesMetrics = repositoriesMetrics;
        this.delegate = delegate;
        this.blobPath = delegate.path() == null ? Strings.EMPTY : delegate.path().buildAsString();
    }

    protected abstract boolean isExceptionRetryable(Exception e);

    protected abstract Map<String, Object> getMetricsAttributes(RetryMethod method);

    @Override
    public Map<String, BlobMetadata> listBlobs(OperationPurpose purpose) throws IOException {
        if (shouldRetry(purpose)) {
            return execute(() -> super.listBlobs(purpose), RetryMethod.LIST_BLOBS);
        }

        return super.listBlobs(purpose);
    }

    @Override
    public Map<String, BlobMetadata> listBlobsByPrefix(OperationPurpose purpose, String blobNamePrefix) throws IOException {
        if (shouldRetry(purpose)) {
            return execute(() -> super.listBlobsByPrefix(purpose, blobNamePrefix), RetryMethod.LIST_BLOBS_BY_PREFIX);
        }

        return super.listBlobsByPrefix(purpose, blobNamePrefix);
    }

    @Override
    public Map<String, BlobContainer> children(OperationPurpose purpose) throws IOException {
        if (shouldRetry(purpose)) {
            return execute(() -> super.children(purpose), RetryMethod.CHILDREN);
        }

        return super.children(purpose);
    }

    // Visible for testing
    protected <T, E extends Exception> T execute(CheckedSupplier<T, E> operation, RetryMethod method) throws E {
        final List<Exception> failures = new ArrayList<>(MAX_SUPPRESSED_EXCEPTIONS);

        int attempts = INITIAL_ATTEMPT;

        while (true) {
            try {
                T t = operation.get();
                maybeLogSuccessfulRetry(method, attempts);
                return t;
            } catch (Exception e) {
                if (isExceptionRetryable(e)) {
                    attempts++;
                    if (failures.size() < MAX_SUPPRESSED_EXCEPTIONS) {
                        failures.add(e);
                    }
                    logRetryAttempt(method);
                    try {
                        Thread.sleep(getRetryDelayInMillis(attempts));
                    } catch (InterruptedException exception) {
                        Thread.currentThread().interrupt();
                    }
                } else {
                    for (Exception failure : failures) {
                        e.addSuppressed(failure);
                    }
                    logRetryFailure(e, method, attempts);
                    throw e;
                }
            }
        }
    }

    private boolean shouldRetry(OperationPurpose purpose) {
        return purpose == OperationPurpose.INDICES;
    }

    private void maybeLogSuccessfulRetry(RetryMethod method, int attempts) {
        if (attempts > INITIAL_ATTEMPT) {
            repositoriesMetrics.allocationTransientErrorRetrySuccessCounter().incrementBy(1, getMetricsAttributes(method));
            logger.info("""
                Blobstore [{}] operation [{}] succeeded after [{}] attempts.
                """, delegate.path().buildAsString(), method.name(), attempts);
        }
    }

    private void logRetryAttempt(RetryMethod method) {
        repositoriesMetrics.allocationTransientErrorRetryCounter().incrementBy(1, getMetricsAttributes(method));
    }

    private void logRetryFailure(Exception ex, RetryMethod method, int attempts) {
        logger.log(
            Integer.bitCount(attempts) == 1 ? Level.INFO : Level.DEBUG,
            () -> format(
                "Blobstore [%s] operation [%s] failed after [%d] attempts.",
                delegate.path().buildAsString(),
                method.name(),
                attempts
            ),
            ex
        );
        repositoriesMetrics.allocationTransientErrorRetryFailureCounter().incrementBy(1, getMetricsAttributes(method));
    }

    protected long getRetryDelayInMillis(int attempt) {
        // Initial delay is 10 ms and cap max delay at 10 * 1024 millis, i.e. it retries every ~10 seconds at a minimum
        return 10L << (Math.min(attempt - 1, 10));
    }
}
