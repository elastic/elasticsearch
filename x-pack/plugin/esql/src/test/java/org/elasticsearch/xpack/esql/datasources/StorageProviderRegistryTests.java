/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

public class StorageProviderRegistryTests extends ESTestCase {

    public void testThrottleScopeIsPerBucketNotPerScheme() {
        // The adaptive-backoff scope is the store's hot unit (per-bucket), not per-scheme: two buckets on the same
        // scheme must get distinct scopes so a hot bucket backs off only its own traffic, while every object in one
        // bucket shares one scope.
        String bucketA = StorageProviderRegistry.throttleScope(StoragePath.of("s3://bucket-a/data/part-0.csv"));
        String bucketASibling = StorageProviderRegistry.throttleScope(StoragePath.of("s3://bucket-a/other/part-9.csv"));
        String bucketB = StorageProviderRegistry.throttleScope(StoragePath.of("s3://bucket-b/data/part-0.csv"));

        assertEquals("same bucket, different keys -> same throttle scope", bucketA, bucketASibling);
        assertNotEquals("different buckets -> different throttle scope (no cross-bucket backoff bleed)", bucketA, bucketB);
        assertEquals("s3://bucket-a", bucketA);
    }

    public void testThrottleScopePerStore() {
        assertEquals("gs://gbucket", StorageProviderRegistry.throttleScope(StoragePath.of("gs://gbucket/prefix/f.csv")));
        // The local filesystem has no host; it collapses to one scope (it never throttles, so the backoff is inert).
        assertEquals("file://", StorageProviderRegistry.throttleScope(StoragePath.of("file:///tmp/data/f.csv")));
    }
}
