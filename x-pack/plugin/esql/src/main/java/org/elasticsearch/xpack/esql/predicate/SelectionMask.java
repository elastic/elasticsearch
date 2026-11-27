/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

import java.util.Arrays;

/**
 * Positions in a batch that are still selected. The bits are the prototype representation; a source may use a doc-id
 * iterator, a selection vector, or a remote row subset instead.
 */
public final class SelectionMask {
    private final boolean[] bits;

    private SelectionMask(boolean[] bits) {
        this.bits = bits;
    }

    public static SelectionMask of(boolean... bits) {
        return new SelectionMask(bits.clone());
    }

    public static SelectionMask all(int positionCount) {
        boolean[] bits = new boolean[positionCount];
        Arrays.fill(bits, true);
        return new SelectionMask(bits);
    }

    public static SelectionMask none(int positionCount) {
        return new SelectionMask(new boolean[positionCount]);
    }

    public int positionCount() {
        return bits.length;
    }

    public boolean get(int position) {
        return bits[position];
    }

    public boolean isEmpty() {
        return cardinality() == 0;
    }

    public int cardinality() {
        int count = 0;
        for (boolean bit : bits) {
            if (bit) {
                count++;
            }
        }
        return count;
    }

    /** Batch positions of the selected bits, in ascending order. Ordinal {@code i} is the position of loaded value {@code i}. */
    public int[] positions() {
        int[] positions = new int[cardinality()];
        for (int position = 0, ordinal = 0; position < bits.length; position++) {
            if (bits[position]) {
                positions[ordinal++] = position;
            }
        }
        return positions;
    }

    public SelectionMask or(SelectionMask other) {
        return combine(other, (left, right) -> left || right);
    }

    public SelectionMask and(SelectionMask other) {
        return combine(other, (left, right) -> left && right);
    }

    /** Positions selected here and not selected by {@code other}. */
    public SelectionMask andNot(SelectionMask other) {
        return combine(other, (left, right) -> left && right == false);
    }

    private SelectionMask combine(SelectionMask other, BitOp op) {
        if (other.bits.length != bits.length) {
            throw new IllegalArgumentException("masks cover [" + bits.length + "] and [" + other.bits.length + "] positions");
        }
        boolean[] combined = new boolean[bits.length];
        for (int i = 0; i < bits.length; i++) {
            combined[i] = op.apply(bits[i], other.bits[i]);
        }
        return new SelectionMask(combined);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof SelectionMask other && Arrays.equals(bits, other.bits);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bits);
    }

    @Override
    public String toString() {
        return Arrays.toString(bits);
    }

    private interface BitOp {
        boolean apply(boolean left, boolean right);
    }
}
