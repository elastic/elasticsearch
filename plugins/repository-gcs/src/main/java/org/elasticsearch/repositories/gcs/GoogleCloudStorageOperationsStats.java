/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
        results.put("GET", getCount.get());
        results.put("LIST", listCount.get());
        results.put("PUT", putCount.get());
        results.put("POST", postCount.get());
        return results;
    }
}
