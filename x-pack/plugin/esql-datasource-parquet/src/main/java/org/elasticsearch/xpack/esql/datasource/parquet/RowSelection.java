/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import java.util.BitSet;

/**
 * Tracks which rows within a batch survive predicate evaluation during late materialization.
 * Wraps a {@link BitSet} with pre-computed cardinality and fast-path checks for the common
 * "all selected" and "none selected" cases.
 *
 * <p>Used as a selection vector between Phase 1 (filter evaluation) and Phase 2 (selective
 * materialization) of the two-phase late materialization pipeline. Non-predicate columns
 * use this to skip non-surviving values at the byte level during decode.
 */
final class RowSelection {

    private final BitSet selected;
    private final int totalRows;
    private final int selectedCount;

    RowSelection(BitSet selected, int totalRows, int selectedCount) {
        this.selected = selected;
        this.totalRows = totalRows;
        this.selectedCount = selectedCount;
    }

    static RowSelection all(int totalRows) {
        BitSet bits = new BitSet(totalRows);
        bits.set(0, totalRows);
        return new RowSelection(bits, totalRows, totalRows);
    }

    static RowSelection none(int totalRows) {
        return new RowSelection(new BitSet(totalRows), totalRows, 0);
    }

    /**
     * Creates a RowSelection from a boolean survivor mask (true = selected).
     */
    static RowSelection fromMask(boolean[] mask, int count) {
        BitSet bits = new BitSet(count);
        int selected = 0;
        for (int i = 0; i < count; i++) {
            if (mask[i]) {
                bits.set(i);
                selected++;
            }
        }
        return new RowSelection(bits, count, selected);
    }

    boolean isAllSelected() {
        return selectedCount == totalRows;
    }

    boolean isNoneSelected() {
        return selectedCount == 0;
    }

    int totalRows() {
        return totalRows;
    }

    int selectedCount() {
        return selectedCount;
    }

    int nextSelected(int fromIndex) {
        return selected.nextSetBit(fromIndex);
    }

    boolean isSelected(int index) {
        return selected.get(index);
    }

    /**
     * Returns a view of this selection for rows {@code [offset, offset+length)}, re-indexed
     * to start at 0. Used when a batch spans multiple Parquet pages.
     */
    RowSelection slice(int offset, int length) {
        if (offset == 0 && length == totalRows) {
            return this;
        }
        BitSet sliced = selected.get(offset, offset + length);
        int slicedCount = sliced.cardinality();
        return new RowSelection(sliced, length, slicedCount);
    }

    /**
     * Converts this selection to a boolean array (true = selected) for compatibility
     * with the existing {@link PageColumnReader#filterBlock} API.
     */
    boolean[] toBooleanArray() {
        boolean[] result = new boolean[totalRows];
        for (int i = selected.nextSetBit(0); i >= 0; i = selected.nextSetBit(i + 1)) {
            result[i] = true;
        }
        return result;
    }
}
