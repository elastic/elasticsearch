/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import java.util.Objects;

/**
 * Marks selected vectors within a block of at most 32 contiguous vectors: bit {@code i} of the mask selects the {@code i}-th vector.
 * Masked bulk scoring writes the score of each selected vector at its position in the block.
 * This is a static/utility class on purpose: it is used in hot paths and we want the bit operations to be fully inlined,
 * regardless of EA and C2 decisions.
 */
public final class BulkMask {

    private BulkMask() {}

    /** A mask selecting the first {@code count} vectors of a block. */
    public static int all(int count) {
        return (int) ((1L << count) - 1);
    }

    /** {@code mask} with the {@code index}-th vector selected too. */
    public static int select(int mask, int index) {
        return mask | (1 << index);
    }

    public static boolean isSelected(int mask, int index) {
        return (mask & (1 << index)) != 0;
    }

    /** The index of the lowest selected vector; {@code mask} must not be 0. */
    public static int first(int mask) {
        return Integer.numberOfTrailingZeros(mask);
    }

    /** {@code mask} with its lowest selected vector deselected. */
    public static int deselectFirst(int mask) {
        return mask & (mask - 1);
    }

    /** Checks that a block holds at most 32 vectors, and that {@code mask} selects none past the end of the block. */
    public static boolean validate(int mask, int count) {
        Objects.checkIndex(count, Integer.SIZE + 1);
        if (count < Integer.SIZE && (mask >>> count) != 0) {
            throw new IllegalArgumentException("mask " + Integer.toHexString(mask) + " selects vectors beyond count " + count);
        }
        return true;
    }
}
