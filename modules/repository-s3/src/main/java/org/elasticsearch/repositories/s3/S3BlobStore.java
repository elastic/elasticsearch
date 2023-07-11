/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.s3;

import com.amazonaws.Request;
import com.amazonaws.Response;
import com.amazonaws.metrics.RequestMetricCollector;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.util.AWSRequestMetrics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.BlobStoreException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

class S3BlobStore implements BlobStore {

    private static final Logger logger = LogManager.getLogger(S3BlobStore.class);

    private final S3Service service;

    private final BigArrays bigArrays;

    private final String bucket;

    private final ByteSizeValue bufferSize;

    private final boolean serverSideEncryption;

    private final CannedAccessControlList cannedACL;

    private final StorageClass storageClass;

    private final RepositoryMetadata repositoryMetadata;

    private final ThreadPool threadPool;

    private final Stats stats = new Stats();

    final RequestMetricCollector getMetricCollector;
    final RequestMetricCollector listMetricCollector;
    final RequestMetricCollector putMetricCollector;
    final RequestMetricCollector multiPartUploadMetricCollector;

    S3BlobStore(
        S3Service service,
        String bucket,
        boolean serverSideEncryption,
        ByteSizeValue bufferSize,
        String cannedACL,
        String storageClass,
        RepositoryMetadata repositoryMetadata,
        BigArrays bigArrays,
        ThreadPool threadPool
    ) {
        this.service = service;
        this.bigArrays = bigArrays;
        this.bucket = bucket;
        this.serverSideEncryption = serverSideEncryption;
        this.bufferSize = bufferSize;
        this.cannedACL = initCannedACL(cannedACL);
        this.storageClass = initStorageClass(storageClass);
        this.repositoryMetadata = repositoryMetadata;
        this.threadPool = threadPool;
        this.getMetricCollector = new IgnoreNoResponseMetricsCollector() {
            @Override
            public void collectMetrics(Request<?> request) {
                assert request.getHttpMethod().name().equals("GET");
                stats.getCount.addAndGet(getRequestCount(request));
            }
        };
        this.listMetricCollector = new IgnoreNoResponseMetricsCollector() {
            @Override
            public void collectMetrics(Request<?> request) {
                assert request.getHttpMethod().name().equals("GET");
                stats.listCount.addAndGet(getRequestCount(request));
            }
        };
        this.putMetricCollector = new IgnoreNoResponseMetricsCollector() {
            @Override
            public void collectMetrics(Request<?> request) {
                assert request.getHttpMethod().name().equals("PUT");
                stats.putCount.addAndGet(getRequestCount(request));
            }
        };
        this.multiPartUploadMetricCollector = new IgnoreNoResponseMetricsCollector() {
            @Override
            public void collectMetrics(Request<?> request) {
                assert request.getHttpMethod().name().equals("PUT") || request.getHttpMethod().name().equals("POST");
                stats.postCount.addAndGet(getRequestCount(request));
            }
        };
    }

    // metrics collector that ignores null responses that we interpret as the request not reaching the S3 endpoint due to a network
    // issue
    private abstract static class IgnoreNoResponseMetricsCollector extends RequestMetricCollector {

        @Override
        public final void collectMetrics(Request<?> request, Response<?> response) {
            if (response != null) {
                collectMetrics(request);
            }
        }

        protected abstract void collectMetrics(Request<?> request);
    }

    private static long getRequestCount(Request<?> request) {
        Number requestCount = request.getAWSRequestMetrics().getTimingInfo().getCounter(AWSRequestMetrics.Field.RequestCount.name());
        if (requestCount == null) {
            logger.warn("Expected request count to be tracked for request [{}] but found not count.", request);
            return 0L;
        }
        return requestCount.longValue();
    }

    @Override
    public String toString() {
        return bucket;
    }

    public AmazonS3Reference clientReference() {
        return service.client(repositoryMetadata);
    }

    int getMaxRetries() {
        return service.settings(repositoryMetadata).maxRetries;
    }

    public String bucket() {
        return bucket;
    }

    public BigArrays bigArrays() {
        return bigArrays;
    }

    public boolean serverSideEncryption() {
        return serverSideEncryption;
    }

    public long bufferSizeInBytes() {
        return bufferSize.getBytes();
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new S3BlobContainer(path, this);
    }

    @Override
    public void close() throws IOException {
        this.service.close();
    }

    @Override
    public Map<String, Long> stats() {
        return stats.toMap();
    }

    public CannedAccessControlList getCannedACL() {
        return cannedACL;
    }

    public StorageClass getStorageClass() {
        return storageClass;
    }

    public static StorageClass initStorageClass(String storageClass) {
        if ((storageClass == null) || storageClass.equals("")) {
            return StorageClass.Standard;
        }

        try {
            final StorageClass _storageClass = StorageClass.fromValue(storageClass.toUpperCase(Locale.ENGLISH));
            if (_storageClass.equals(StorageClass.Glacier)) {
                throw new BlobStoreException("Glacier storage class is not supported");
            }

            return _storageClass;
        } catch (final IllegalArgumentException illegalArgumentException) {
            throw new BlobStoreException("`" + storageClass + "` is not a valid S3 Storage Class.");
        }
    }

    /**
     * Constructs canned acl from string
     */
    public static CannedAccessControlList initCannedACL(String cannedACL) {
        if ((cannedACL == null) || cannedACL.equals("")) {
            return CannedAccessControlList.Private;
        }

        for (final CannedAccessControlList cur : CannedAccessControlList.values()) {
            if (cur.toString().equalsIgnoreCase(cannedACL)) {
                return cur;
            }
        }

        throw new BlobStoreException("cannedACL is not valid: [" + cannedACL + "]");
    }

    ThreadPool getThreadPool() {
        return threadPool;
    }

    static class Stats {

        final AtomicLong listCount = new AtomicLong();

        final AtomicLong getCount = new AtomicLong();

        final AtomicLong putCount = new AtomicLong();

        final AtomicLong postCount = new AtomicLong();

        Map<String, Long> toMap() {
            final Map<String, Long> results = new HashMap<>();
            results.put("GetObject", getCount.get());
            results.put("ListObjects", listCount.get());
            results.put("PutObject", putCount.get());
            results.put("PutMultipartObject", postCount.get());
            return results;
        }
    }
}
