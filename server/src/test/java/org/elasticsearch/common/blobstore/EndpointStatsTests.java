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

public class EndpointStatsTests extends ESTestCase {

    public void testEquals() {
        EndpointStats expected = randomEndpointStats();
        assertEquals(expected, new EndpointStats(expected.operations(), expected.requests()));
        assertNotEquals(
            expected,
            new EndpointStats(randomValueOtherThan(expected.operations(), ESTestCase::randomNonNegativeLong), expected.requests())
        );
        assertNotEquals(
            expected,
            new EndpointStats(expected.operations(), randomValueOtherThan(expected.requests(), ESTestCase::randomNonNegativeLong))
        );
    }

    public void testAdd() {
        final EndpointStats lhs = randomEndpointStats(1 << 30);
        final EndpointStats rhs = randomEndpointStats(1 << 30);
        final EndpointStats result = lhs.add(rhs);
        assertEquals(lhs.operations() + rhs.operations(), result.operations());
        assertEquals(lhs.requests() + rhs.requests(), result.requests());
    }

    public void testAddOverflow() {
        final EndpointStats lhs = new EndpointStats(randomLongBetween(50, 1 << 30), randomLongBetween(50, 1 << 30));
        final int fieldToOverflow = randomIntBetween(0, 1);
        final EndpointStats rhs = new EndpointStats(
            fieldToOverflow == 0 ? (Long.MAX_VALUE - lhs.operations()) + 1 : 1,
            fieldToOverflow == 1 ? (Long.MAX_VALUE - lhs.requests()) + 1 : 1
        );
        assertThrows(ArithmeticException.class, () -> lhs.add(rhs));
    }

    public void testIsZero() {
        assertTrue(new EndpointStats(0, 0).isZero());
        assertFalse(new EndpointStats(randomLongBetween(1, Long.MAX_VALUE), 0).isZero());
        assertFalse(new EndpointStats(0, randomLongBetween(1, Long.MAX_VALUE)).isZero());
    }

    private EndpointStats randomEndpointStats() {
        return randomEndpointStats(Long.MAX_VALUE);
    }

    private EndpointStats randomEndpointStats(long upperBound) {
        return new EndpointStats(randomLongBetween(0, upperBound), randomLongBetween(0, upperBound));
    }
}
