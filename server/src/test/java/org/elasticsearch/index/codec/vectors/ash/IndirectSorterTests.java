/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.ash;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.function.IntUnaryOperator;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Tests for {@link IndirectSorter}.
 * <p>
 * Regression coverage for a bug where {@code sortAscendingByDouble}'s pivot comparison had its
 * operands swapped, causing it to silently sort in descending order instead of ascending. That bug
 * was invisible to {@code sortDescendingByFloat} (used only by the 2-bit quantization path) because
 * the same swapped-operand mistake happens to produce the intended descending order there, but it
 * broke {@link AshSphericalScalarQuantizer}'s general (bitsPerDim &gt;= 3) quantization path, which
 * relies on events being processed in ascending order to greedily improve on the base assignment.
 */
public class IndirectSorterTests extends ESTestCase {

    public void testSortAscendingByDoubleSmall() {
        double[] keys = { 5.0, 1.0, 3.0, 2.0, 4.0 };
        int[] indices = { 0, 1, 2, 3, 4 };
        IndirectSorter.sortAscendingByDouble(indices, keys, indices.length);
        assertSortedAscending(keys, indices);
        assertPermutation(indices, 5);
    }

    public void testSortDescendingByFloatSmall() {
        float[] keys = { 5.0f, 1.0f, 3.0f, 2.0f, 4.0f };
        int[] indices = { 0, 1, 2, 3, 4 };
        IndirectSorter.sortDescendingByFloat(indices, keys, indices.length);
        assertSortedDescending(keys, indices);
        assertPermutation(indices, 5);
    }

    public void testSortAscendingByDoubleRandomized() {
        for (int iter = 0; iter < 50; iter++) {
            // Cover both the insertion-sort path (small ranges) and the quicksort/heapsort path.
            int n = randomIntBetween(1, 300);
            double[] keys = new double[n];
            for (int i = 0; i < n; i++) {
                keys[i] = random().nextGaussian();
            }
            int[] indices = new int[n];
            Arrays.setAll(indices, IntUnaryOperator.identity());

            IndirectSorter.sortAscendingByDouble(indices, keys, n);

            assertSortedAscending(keys, indices);
            assertPermutation(indices, n);
        }
    }

    public void testSortDescendingByFloatRandomized() {
        for (int iter = 0; iter < 50; iter++) {
            int n = randomIntBetween(1, 300);
            float[] keys = new float[n];
            for (int i = 0; i < n; i++) {
                keys[i] = (float) random().nextGaussian();
            }
            int[] indices = new int[n];
            Arrays.setAll(indices, IntUnaryOperator.identity());

            IndirectSorter.sortDescendingByFloat(indices, keys, n);

            assertSortedDescending(keys, indices);
            assertPermutation(indices, n);
        }
    }

    public void testSortAscendingByDoubleWithDuplicates() {
        for (int iter = 0; iter < 20; iter++) {
            int n = randomIntBetween(5, 100);
            double[] keys = new double[n];
            for (int i = 0; i < n; i++) {
                // Small value range to force many duplicate keys / ties.
                keys[i] = randomIntBetween(0, 4);
            }
            int[] indices = new int[n];
            Arrays.setAll(indices, IntUnaryOperator.identity());

            IndirectSorter.sortAscendingByDouble(indices, keys, n);

            assertSortedAscending(keys, indices);
            assertPermutation(indices, n);
        }
    }

    public void testSortEmptyAndSingleton() {
        double[] emptyKeys = new double[0];
        int[] emptyIndices = new int[0];
        IndirectSorter.sortAscendingByDouble(emptyIndices, emptyKeys, 0);
        assertEquals(0, emptyIndices.length);

        double[] oneKey = { 42.0 };
        int[] oneIndex = { 0 };
        IndirectSorter.sortAscendingByDouble(oneIndex, oneKey, 1);
        assertArrayEquals(new int[] { 0 }, oneIndex);
    }

    private static void assertSortedAscending(double[] keys, int[] indices) {
        for (int i = 0; i + 1 < indices.length; i++) {
            assertThat("Expected ascending order at position " + i, keys[indices[i + 1]], greaterThanOrEqualTo(keys[indices[i]]));
        }
    }

    private static void assertSortedDescending(float[] keys, int[] indices) {
        for (int i = 0; i + 1 < indices.length; i++) {
            assertThat("Expected descending order at position " + i, keys[indices[i + 1]], lessThanOrEqualTo(keys[indices[i]]));
        }
    }

    private static void assertPermutation(int[] indices, int n) {
        boolean[] seen = new boolean[n];
        for (int idx : indices) {
            assertFalse("Index " + idx + " appeared more than once", seen[idx]);
            seen[idx] = true;
        }
    }
}
