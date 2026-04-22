/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.blobstore.support;

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
    private static final String REPOSITORY_TYPE = "cloud_provider";
    public static final int MAX_SUPPRESSED_EXCEPTIONS = 10;
    private final List<Exception> failures;

    public TenaciousRetryBlobContainer(BlobContainer delegate, RepositoriesMetrics repositoriesMetrics) {
        super(delegate);
        this.repositoriesMetrics = repositoriesMetrics;
        this.failures = new ArrayList<>(MAX_SUPPRESSED_EXCEPTIONS);
    }

    protected abstract boolean isExceptionRetryable(Exception e);

    protected abstract String getRepositoryType();

    @Override
    public Map<String, BlobMetadata> listBlobs(OperationPurpose purpose) throws IOException {
        if (shouldRetry(purpose)) {
            return execute(() -> super.listBlobs(purpose));
        }

        return super.listBlobs(purpose);
    }

    @Override
    public Map<String, BlobMetadata> listBlobsByPrefix(OperationPurpose purpose, String blobNamePrefix) throws IOException {
        if (shouldRetry(purpose)) {
            return execute(() -> super.listBlobsByPrefix(purpose, blobNamePrefix));
        }

        return super.listBlobsByPrefix(purpose, blobNamePrefix);
    }

    @Override
    public Map<String, BlobContainer> children(OperationPurpose purpose) throws IOException {
        if (shouldRetry(purpose)) {
            return execute(() -> super.children(purpose));
        }

        return super.children(purpose);
    }

    private <T, E extends Exception> T execute(CheckedSupplier<T, E> operation) throws E {
        int attempts = INITIAL_ATTEMPT;

        while (true) {
            try {
                T t = operation.get();
                maybeLogSuccessfulRetry(attempts);
                return t;
            } catch (Exception e) {
                if (isExceptionRetryable(e)) {
                    attempts++;
                    if (failures.size() < MAX_SUPPRESSED_EXCEPTIONS) {
                        failures.add(e);
                    }
                    logRetryAttempt();
                    try {
                        Thread.sleep(getRetryDelayInMillis(attempts));
                    } catch (InterruptedException exception) {
                        logger.info("");
                        Thread.currentThread().interrupt();
                    }
                } else {
                    for (Exception failure : failures) {
                        e.addSuppressed(failure);
                    }
                    logRetryFailure(e);
                    throw e;
                }
            }
        }
    }

    private boolean shouldRetry(OperationPurpose purpose) {
        return purpose == OperationPurpose.INDICES;
    }

    private void maybeLogSuccessfulRetry(int attempts) {
        if (attempts > INITIAL_ATTEMPT) {
            repositoriesMetrics.allocationTransientErrorRetrySuccessCounter().incrementBy(1, Map.of(REPOSITORY_TYPE, getRepositoryType()));
        }
    }

    private void logRetryAttempt() {
        repositoriesMetrics.allocationTransientErrorRetryCounter().incrementBy(1, Map.of(REPOSITORY_TYPE, getRepositoryType()));
    }

    private void logRetryFailure(Exception ex) {
        logger.warn("Retries failed for blob store", ex);
        repositoriesMetrics.allocationTransientErrorRetryFailureCounter().incrementBy(1, Map.of(REPOSITORY_TYPE, getRepositoryType()));
    }

    protected long getRetryDelayInMillis(int attempt) {
        // Initial delay is 10 ms and cap max delay at 10 * 1024 millis, i.e. it retries every ~10 seconds at a minimum
        return 10L << (Math.min(attempt - 1, 10));
    }
}
