/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.blobstore.support;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.core.CheckedSupplier;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.repositories.RepositoriesMetrics;

import java.io.IOException;
import java.util.Map;

/**
 * Cloud storage services use an eventual consistency model for identity, access control, and metadata.
 * As a result, they may occasionally return transient 403 Forbidden errors even when permissions are correctly configured.
 * This class wraps the blobContainer to provide a dedicated retry mechanism initially for handling these 403 errors.
 *
 */
public abstract class TenaciousRetryBlobContainer extends FilterBlobContainer {

    private final int maxRetries;
    private final TimeValue delayIncrement;
    private final RepositoriesMetrics repositoriesMetrics;

    public TenaciousRetryBlobContainer(
        BlobContainer delegate,
        int maxRetries,
        TimeValue delayIncrement,
        RepositoriesMetrics repositoriesMetrics
    ) {
        super(delegate);
        this.maxRetries = maxRetries;
        this.delayIncrement = delayIncrement;
        this.repositoriesMetrics = repositoriesMetrics;
    }

    protected abstract boolean isExceptionRetryable(Exception e);

    protected abstract String cloudServiceProvider();

    @Override
    public Map<String, BlobMetadata> listBlobs(OperationPurpose purpose) throws IOException {
        if (purpose == OperationPurpose.INDICES) {
            return execute(() -> super.listBlobs(purpose));
        }

        return super.listBlobs(purpose);
    }

    @Override
    public Map<String, BlobMetadata> listBlobsByPrefix(OperationPurpose purpose, String blobNamePrefix) throws IOException {
        if (purpose == OperationPurpose.INDICES) {
            return execute(() -> super.listBlobsByPrefix(purpose, blobNamePrefix));
        }

        return super.listBlobsByPrefix(purpose, blobNamePrefix);
    }

    private <T, E extends Exception> T execute(CheckedSupplier<T, E> operation) throws E {
        int attempts = 0;
        while (true) {
            try {
                T t = operation.get();
                if (attempts > 1) {
                    repositoriesMetrics.allocationTransientErrorRetrySuccessCounter()
                        .incrementBy(1, Map.of("cloud_provider", cloudServiceProvider()));
                }
                return t;
            } catch (Exception e) {
                if (isExceptionRetryable(e) && attempts < maxRetries) {
                    attempts++;
                    repositoriesMetrics.allocationTransientErrorRetryCounter()
                        .incrementBy(1, Map.of("cloud_provider", cloudServiceProvider()));
                    try {
                        Thread.sleep(delayIncrement.millis() * attempts);
                    } catch (InterruptedException exception) {
                        Thread.currentThread().interrupt();
                    }
                } else {
                    throw e;
                }
            }
        }
    }
}
