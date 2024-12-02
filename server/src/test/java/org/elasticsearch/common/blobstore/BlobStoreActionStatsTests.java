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

public class BlobStoreActionStatsTests extends ESTestCase {

    public void testEquals() {
        BlobStoreActionStats expected = randomEndpointStats();
        assertEquals(expected, new BlobStoreActionStats(expected.operations(), expected.requests()));
        assertNotEquals(
            expected,
            new BlobStoreActionStats(randomValueOtherThan(expected.operations(), ESTestCase::randomNonNegativeLong), expected.requests())
        );
        assertNotEquals(
            expected,
            new BlobStoreActionStats(expected.operations(), randomValueOtherThan(expected.requests(), ESTestCase::randomNonNegativeLong))
        );
    }

    public void testAdd() {
        final BlobStoreActionStats lhs = randomEndpointStats(1 << 30);
        final BlobStoreActionStats rhs = randomEndpointStats(1 << 30);
        final BlobStoreActionStats result = lhs.add(rhs);
        assertEquals(lhs.operations() + rhs.operations(), result.operations());
        assertEquals(lhs.requests() + rhs.requests(), result.requests());
    }

    public void testAddOverflow() {
        final BlobStoreActionStats lhs = new BlobStoreActionStats(randomLongBetween(50, 1 << 30), randomLongBetween(50, 1 << 30));
        final int fieldToOverflow = randomIntBetween(0, 1);
        final BlobStoreActionStats rhs = new BlobStoreActionStats(
            fieldToOverflow == 0 ? (Long.MAX_VALUE - lhs.operations()) + 1 : 1,
            fieldToOverflow == 1 ? (Long.MAX_VALUE - lhs.requests()) + 1 : 1
        );
        assertThrows(ArithmeticException.class, () -> lhs.add(rhs));
    }

    public void testIsZero() {
        assertTrue(new BlobStoreActionStats(0, 0).isZero());
        assertFalse(new BlobStoreActionStats(randomLongBetween(1, Long.MAX_VALUE), 0).isZero());
        assertFalse(new BlobStoreActionStats(0, randomLongBetween(1, Long.MAX_VALUE)).isZero());
    }

    private BlobStoreActionStats randomEndpointStats() {
        return randomEndpointStats(Long.MAX_VALUE);
    }

    private BlobStoreActionStats randomEndpointStats(long upperBound) {
        return new BlobStoreActionStats(randomLongBetween(0, upperBound), randomLongBetween(0, upperBound));
    }
}
