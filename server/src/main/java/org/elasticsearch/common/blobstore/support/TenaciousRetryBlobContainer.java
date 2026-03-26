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
import org.elasticsearch.common.BackoffPolicy;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.core.CheckedSupplier;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.repositories.RepositoriesMetrics;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Cloud storage services use an eventual consistency model for identity, access control, and metadata.
 * As a result, they may occasionally return transient 403 Forbidden errors even when permissions are correctly configured.
 * This class wraps the blobContainer to provide a dedicated retry mechanism initially for handling these 403 errors.
 *
 */
public abstract class TenaciousRetryBlobContainer extends FilterBlobContainer {

    private static final Logger logger = LogManager.getLogger(TenaciousRetryBlobContainer.class);
    private final int maxRetries;
    private final BackoffPolicy backoffPolicy;
    private final RepositoriesMetrics repositoriesMetrics;
    private static final int INITIAL_ATTEMPT = 1;
    private static final String REPOSITORY_TYPE = "cloud_provider";

    public TenaciousRetryBlobContainer(
        BlobContainer delegate,
        int maxRetries,
        BackoffPolicy backoffPolicy,
        RepositoriesMetrics repositoriesMetrics
    ) {
        super(delegate);
        this.maxRetries = maxRetries;
        this.backoffPolicy = backoffPolicy;
        this.repositoriesMetrics = repositoriesMetrics;
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

    private <T, E extends Exception> T execute(CheckedSupplier<T, E> operation) throws E {
        int attempts = INITIAL_ATTEMPT;
        final Iterator<TimeValue> iterator = backoffPolicy.iterator();
        while (true) {
            try {
                T t = operation.get();
                maybeLogSuccessfulRetry(attempts);
                return t;
            } catch (Exception e) {
                if (isExceptionRetryable(e) && attempts < maxRetries) {
                    attempts++;
                    logRetryAttempt();
                    try {
                        Thread.sleep(iterator.next().millis());
                    } catch (InterruptedException exception) {
                        Thread.currentThread().interrupt();
                    }
                } else {
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

}
