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

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.support.TenaciousRetryBlobContainer;
import org.elasticsearch.repositories.RepositoriesMetrics;

import java.util.Map;

import static software.amazon.awssdk.http.HttpStatusCode.FORBIDDEN;
import static software.amazon.awssdk.http.HttpStatusCode.REQUEST_TIMEOUT;
import static software.amazon.awssdk.http.HttpStatusCode.SERVICE_UNAVAILABLE;
import static software.amazon.awssdk.http.HttpStatusCode.THROTTLING;

public class S3TenaciousRetryBlobContainer extends TenaciousRetryBlobContainer {

    private final RepositoriesMetrics repositoriesMetrics;

    public S3TenaciousRetryBlobContainer(BlobContainer delegate, RepositoriesMetrics repositoriesMetrics) {
        super(delegate, repositoriesMetrics);
        this.repositoriesMetrics = repositoriesMetrics;
    }

    @Override
    protected boolean isExceptionRetryable(Exception e) {
        Throwable throwable = ExceptionsHelper.unwrap(e, S3Exception.class);
        if (throwable == null) {
            return false;
        }

        S3Exception exception = (S3Exception) throwable;
        return exception.statusCode() == FORBIDDEN && "InvalidAccessKeyId".equals(exception.awsErrorDetails().errorCode())
            || exception.statusCode() == SERVICE_UNAVAILABLE
            || exception.statusCode() == THROTTLING
            || exception.statusCode() == REQUEST_TIMEOUT;
    }

    @Override
    protected Map<String, Object> getMetricsAttributes(RetryMethod method) {
        return Map.of("repo_type", S3Repository.TYPE, "blob_path", blobPath, "operation", lookUpOperationNameByMethod(method));
    }

    @Override
    protected BlobContainer wrapChild(BlobContainer child) {
        return new S3TenaciousRetryBlobContainer(child, repositoriesMetrics);
    }

    private String lookUpOperationNameByMethod(RetryMethod method) {
        assert METHODS_TO_OPERATIONS.containsKey(method.getName());
        return METHODS_TO_OPERATIONS.get(method.getName()).getKey();
    }

    private static final Map<String, S3BlobStore.Operation> METHODS_TO_OPERATIONS = Map.ofEntries(
        Map.entry("blobExists", S3BlobStore.Operation.HEAD_OBJECT),
        Map.entry("readBlob", S3BlobStore.Operation.GET_OBJECT),
        Map.entry("readBlobPreferredLength", S3BlobStore.Operation.GET_OBJECT),
        Map.entry("writeBlob", S3BlobStore.Operation.PUT_OBJECT),
        Map.entry("writeMetadataBlob", S3BlobStore.Operation.PUT_OBJECT),
        Map.entry("writeBlobAtomic", S3BlobStore.Operation.PUT_OBJECT), // stream/bytes overloads; see note below
        Map.entry("copyBlob", S3BlobStore.Operation.COPY_OBJECT),
        Map.entry("delete", S3BlobStore.Operation.DELETE_OBJECTS),
        Map.entry("deleteBlobsIgnoringIfNotExists", S3BlobStore.Operation.DELETE_OBJECTS),
        Map.entry("listBlobs", S3BlobStore.Operation.LIST_OBJECTS),
        Map.entry("listBlobsByPrefix", S3BlobStore.Operation.LIST_OBJECTS),
        Map.entry("children", S3BlobStore.Operation.LIST_OBJECTS),
        Map.entry("getRegister", S3BlobStore.Operation.GET_OBJECT),
        Map.entry("compareAndSetRegister", S3BlobStore.Operation.PUT_OBJECT),
        Map.entry("compareAndExchangeRegister", S3BlobStore.Operation.PUT_OBJECT)
    );
}
