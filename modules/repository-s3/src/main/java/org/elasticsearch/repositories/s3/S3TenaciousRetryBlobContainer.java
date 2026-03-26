/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import software.amazon.awssdk.services.s3.model.S3Exception;

import org.elasticsearch.common.BackoffPolicy;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.support.TenaciousRetryBlobContainer;
import org.elasticsearch.repositories.RepositoriesMetrics;

import static software.amazon.awssdk.http.HttpStatusCode.FORBIDDEN;

public class S3TenaciousRetryBlobContainer extends TenaciousRetryBlobContainer {

    private final int maxRetries;
    private final BackoffPolicy backoffPolicy;
    private final RepositoriesMetrics repositoriesMetrics;

    public S3TenaciousRetryBlobContainer(
        BlobContainer delegate,
        int maxRetries,
        BackoffPolicy backoffPolicy,
        RepositoriesMetrics repositoriesMetrics
    ) {
        super(delegate, maxRetries, backoffPolicy, repositoriesMetrics);
        this.maxRetries = maxRetries;
        this.backoffPolicy = backoffPolicy;
        this.repositoriesMetrics = repositoriesMetrics;
    }

    @Override
    protected boolean isExceptionRetryable(Exception e) {
        return e instanceof S3Exception && ((S3Exception) e).statusCode() == FORBIDDEN;
    }

    @Override
    protected String getRepositoryType() {
        return S3Repository.TYPE;
    }

    @Override
    protected BlobContainer wrapChild(BlobContainer child) {
        return new S3TenaciousRetryBlobContainer(child, maxRetries, backoffPolicy, repositoriesMetrics);
    }
}
