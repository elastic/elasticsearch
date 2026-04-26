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
import org.elasticsearch.common.blobstore.support.TenaciousRetryBlobContainer;
import org.elasticsearch.repositories.RepositoriesMetrics;

import java.net.UnknownHostException;
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
    protected String getRepositoryType() {
        return GoogleCloudStorageRepository.TYPE;
    }

    @Override
    protected Map<String, Object> getMetricsAttributes(RetryMethod method) {
        return Map.of();
    }

    @Override
    protected BlobContainer wrapChild(BlobContainer child) {
        return new GcsTenaciousRetryBlobContainer(child, repositoriesMetrics);
    }
}
