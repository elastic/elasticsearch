/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.network;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.greaterThan;

public class HandlingTimeTrackerTests extends ESTestCase {

    public void testHistogram() {
        final HandlingTimeTracker handlingTimeTracker = new HandlingTimeTracker();

        assertArrayEquals(new long[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, handlingTimeTracker.getSnapshot());

        handlingTimeTracker.addObservation(0L);
        assertArrayEquals(new long[] { 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, handlingTimeTracker.getSnapshot());

        handlingTimeTracker.addObservation(1L);
        assertArrayEquals(new long[] { 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, handlingTimeTracker.getSnapshot());

        handlingTimeTracker.addObservation(2L);
        assertArrayEquals(new long[] { 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, handlingTimeTracker.getSnapshot());

        handlingTimeTracker.addObservation(3L);
        assertArrayEquals(new long[] { 1, 1, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, handlingTimeTracker.getSnapshot());

        handlingTimeTracker.addObservation(4L);
        assertArrayEquals(new long[] { 1, 1, 2, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, handlingTimeTracker.getSnapshot());

        handlingTimeTracker.addObservation(127L);
        assertArrayEquals(new long[] { 1, 1, 2, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, handlingTimeTracker.getSnapshot());

        handlingTimeTracker.addObservation(128L);
        assertArrayEquals(new long[] { 1, 1, 2, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, handlingTimeTracker.getSnapshot());

        handlingTimeTracker.addObservation(65535L);
        assertArrayEquals(new long[] { 1, 1, 2, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0 }, handlingTimeTracker.getSnapshot());

        handlingTimeTracker.addObservation(65536L);
        assertArrayEquals(new long[] { 1, 1, 2, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1 }, handlingTimeTracker.getSnapshot());

        handlingTimeTracker.addObservation(Long.MAX_VALUE);
        assertArrayEquals(new long[] { 1, 1, 2, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 2 }, handlingTimeTracker.getSnapshot());

        handlingTimeTracker.addObservation(randomLongBetween(65536L, Long.MAX_VALUE));
        assertArrayEquals(new long[] { 1, 1, 2, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 3 }, handlingTimeTracker.getSnapshot());

        handlingTimeTracker.addObservation(randomLongBetween(Long.MIN_VALUE, 0L));
        assertArrayEquals(new long[] { 2, 1, 2, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 3 }, handlingTimeTracker.getSnapshot());
    }

    public void testHistogramRandom() {
        final int[] upperBounds = HandlingTimeTracker.getBucketUpperBounds();
        final long[] expectedCounts = new long[upperBounds.length + 1];
        final HandlingTimeTracker handlingTimeTracker = new HandlingTimeTracker();
        for (int i = between(0, 1000); i > 0; i--) {
            final int bucket = between(0, expectedCounts.length - 1);
            expectedCounts[bucket] += 1;

            final int lowerBound = bucket == 0 ? 0 : upperBounds[bucket - 1];
            final int upperBound = bucket == upperBounds.length ? randomBoolean() ? 100000 : Integer.MAX_VALUE : upperBounds[bucket] - 1;
            handlingTimeTracker.addObservation(between(lowerBound, upperBound));
        }

        assertArrayEquals(expectedCounts, handlingTimeTracker.getSnapshot());
    }

    public void testBoundsConsistency() {
        final int[] upperBounds = HandlingTimeTracker.getBucketUpperBounds();
        assertThat(upperBounds[0], greaterThan(0));
        for (int i = 1; i < upperBounds.length; i++) {
            assertThat(upperBounds[i], greaterThan(upperBounds[i - 1]));
        }
    }

}
