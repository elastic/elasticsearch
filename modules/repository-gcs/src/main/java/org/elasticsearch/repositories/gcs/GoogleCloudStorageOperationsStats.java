/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import org.elasticsearch.common.blobstore.BlobStoreActionStats;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

final class GoogleCloudStorageOperationsStats {

    private final AtomicLong getCount = new AtomicLong();
    private final AtomicLong listCount = new AtomicLong();
    private final AtomicLong putCount = new AtomicLong();
    private final AtomicLong postCount = new AtomicLong();

    private final String bucketName;

    GoogleCloudStorageOperationsStats(String bucketName) {
        this.bucketName = bucketName;
    }

    void trackGetOperation() {
        getCount.incrementAndGet();
    }

    void trackPutOperation() {
        putCount.incrementAndGet();
    }

    void trackPostOperation() {
        postCount.incrementAndGet();
    }

    void trackListOperation() {
        listCount.incrementAndGet();
    }

    String getTrackedBucket() {
        return bucketName;
    }

    // TODO: actually track requests and operations separately (see https://elasticco.atlassian.net/browse/ES-10213)
    Map<String, BlobStoreActionStats> toMap() {
        final Map<String, BlobStoreActionStats> results = new HashMap<>();
        final long getOperations = getCount.get();
        results.put("GetObject", new BlobStoreActionStats(getOperations, getOperations));
        final long listOperations = listCount.get();
        results.put("ListObjects", new BlobStoreActionStats(listOperations, listOperations));
        final long insertOperations = postCount.get() + putCount.get();
        results.put("InsertObject", new BlobStoreActionStats(insertOperations, insertOperations));
        return results;
    }
}
