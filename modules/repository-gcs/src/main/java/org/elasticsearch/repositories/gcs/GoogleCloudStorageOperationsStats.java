/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.gcs;

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

    Map<String, Long> toMap() {
        final Map<String, Long> results = new HashMap<>();
        results.put("GetObject", getCount.get());
        results.put("ListObjects", listCount.get());
        results.put("InsertObject", postCount.get() + putCount.get());
        return results;
    }
}
