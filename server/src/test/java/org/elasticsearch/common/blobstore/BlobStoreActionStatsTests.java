/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.blobstore;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class BlobStoreActionStatsTests extends ESTestCase {

    public void testAdd() {
        final BlobStoreActionStats lhs = randomBlobStoreActionStats(1 << 30);
        final BlobStoreActionStats rhs = randomBlobStoreActionStats(1 << 30);
        final BlobStoreActionStats result = lhs.add(rhs);
        assertEquals(lhs.operations() + rhs.operations(), result.operations());
        assertEquals(lhs.requests() + rhs.requests(), result.requests());
    }

    public void testAddOverflow() {
        final BlobStoreActionStats lhs = randomBlobStoreActionStats(50, 1 << 30);
        // We can only overflow requests, or both values (we can't just overflow operations because requests >= operations
        final boolean overflowRequestsOnly = randomBoolean();
        final long valueToCauseOverflow = (Long.MAX_VALUE - lhs.operations()) + 1;
        final long operationsValue = overflowRequestsOnly ? 1 : valueToCauseOverflow;
        final BlobStoreActionStats rhs = new BlobStoreActionStats(operationsValue, valueToCauseOverflow);
        assertThrows(ArithmeticException.class, () -> lhs.add(rhs));
    }

    public void testIsZero() {
        assertTrue(new BlobStoreActionStats(0, 0).isZero());
        assertFalse(new BlobStoreActionStats(0, randomLongBetween(1, Long.MAX_VALUE)).isZero());
        assertFalse(randomBlobStoreActionStats(1, Long.MAX_VALUE).isZero());
    }

    public void testSerialization() throws IOException {
        final BlobStoreActionStats original = randomBlobStoreActionStats(Long.MAX_VALUE);
        BlobStoreActionStats deserializedModel = copyWriteable(original, null, BlobStoreActionStats::new);
        assertEquals(original, deserializedModel);
        assertNotSame(original, deserializedModel);
    }

    private BlobStoreActionStats randomBlobStoreActionStats(long upperBound) {
        return randomBlobStoreActionStats(0, upperBound);
    }

    private BlobStoreActionStats randomBlobStoreActionStats(long lowerBound, long upperBound) {
        assert upperBound >= lowerBound;
        long operations = randomLongBetween(lowerBound, upperBound);
        long requests = randomLongBetween(operations, upperBound);
        return new BlobStoreActionStats(operations, requests);
    }
}
