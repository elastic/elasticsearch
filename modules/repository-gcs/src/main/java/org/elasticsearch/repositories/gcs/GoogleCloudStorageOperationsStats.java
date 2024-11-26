/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import org.elasticsearch.common.blobstore.EndpointStats;

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

    Map<String, EndpointStats> toMap() {
        final Map<String, EndpointStats> results = new HashMap<>();
        results.put("GetObject", new EndpointStats(getCount.get()));
        results.put("ListObjects", new EndpointStats(listCount.get()));
        results.put("InsertObject", new EndpointStats(postCount.get() + putCount.get()));
        return results;
    }
}
