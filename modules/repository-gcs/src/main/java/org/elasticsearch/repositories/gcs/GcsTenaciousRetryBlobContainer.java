/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.TenaciousRetryBlobContainer;
import org.elasticsearch.repositories.RepositoriesMetrics;

import java.net.UnknownHostException;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;

public class GcsTenaciousRetryBlobContainer extends TenaciousRetryBlobContainer {

    private final RepositoriesMetrics repositoriesMetrics;

    public GcsTenaciousRetryBlobContainer(BlobContainer delegate, RepositoriesMetrics repositoriesMetrics) {
        super(delegate, repositoriesMetrics);
        this.repositoriesMetrics = repositoriesMetrics;
    }

    @Override
    protected boolean isExceptionRetryable(Exception e) {
        return ExceptionsHelper.unwrap(e, UnknownHostException.class) != null;
    }

    @Override
    protected Map<String, Object> getMetricsAttributes(RetryMethod method, OperationPurpose purpose) {
        return Map.of(
            "repo_type",
            GoogleCloudStorageRepository.TYPE,
            "purpose",
            purpose.getKey(),
            "operation",
            lookUpOperationNameByMethod(method)
        );
    }

    @Override
    protected BlobContainer wrapChild(BlobContainer child) {
        return new GcsTenaciousRetryBlobContainer(child, repositoriesMetrics);
    }

    private String lookUpOperationNameByMethod(RetryMethod method) {
        assert METHODS_TO_OPERATIONS.containsKey(method);
        return METHODS_TO_OPERATIONS.get(method).key();
    }

    private static final EnumMap<RetryMethod, StorageOperation> METHODS_TO_OPERATIONS;

    static {
        METHODS_TO_OPERATIONS = new EnumMap<>(
            Map.ofEntries(
                Map.entry(RetryMethod.LIST_BLOBS, StorageOperation.LIST),
                Map.entry(RetryMethod.LIST_BLOBS_BY_PREFIX, StorageOperation.LIST),
                Map.entry(RetryMethod.CHILDREN, StorageOperation.LIST)
            )
        );
        assert METHODS_TO_OPERATIONS.keySet().containsAll(EnumSet.allOf(RetryMethod.class));
    }

}
