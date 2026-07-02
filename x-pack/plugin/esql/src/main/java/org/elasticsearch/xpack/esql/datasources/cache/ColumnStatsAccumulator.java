/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Accumulates per-column null count + min + max across the pages of a single cold scan.
 * Tracked types: BOOLEAN, INTEGER, LONG (incl. DATETIME / DATE_NANOS), DOUBLE, KEYWORD / TEXT / IP.
 * UNSIGNED_LONG and VERSION are deliberately untracked (their stored byte order disagrees with the
 * type's semantic order); all other types are untracked too. Untracked types contribute null counts
 * only — min and max stay null so the warm path bails out rather than serving an unbounded answer.
 * <p>
 * Hot-path discipline: the iterator passes blocks by their position index in the page (not by name)
 * via {@link #acceptBlockAt(int, Block)} — accumulator state is held in a flat array so each block
 * dispatch is a single array load.
 * <p>
 * Multi-valued cells: every value in a position contributes to the running min/max, matching the
 * Parquet stats contract.
 * <p>
 * Single-instance reuse rule: each instance is owned by exactly one batch-iterator and accumulates
 * over that iterator's full lifetime. Concurrent invocation is not supported.
 */
public final class ColumnStatsAccumulator {

    private final ColumnState[] states;
    private final String[] columnNames;
    private final BytesRef scratch = new BytesRef();

    /**
     * Builds an accumulator covering the page-block positions {@code 0..projectedAttrs.length}.
     * State for each index is sized for the corresponding attribute's data type.
     */
    public static ColumnStatsAccumulator forProjectedAttributes(Attribute[] projectedAttrs) {
        if (projectedAttrs == null || projectedAttrs.length == 0) {
            return new ColumnStatsAccumulator(new ColumnState[0], new String[0]);
        }
        ColumnState[] s = new ColumnState[projectedAttrs.length];
        String[] names = new String[projectedAttrs.length];
        for (int i = 0; i < projectedAttrs.length; i++) {
            s[i] = new ColumnState(projectedAttrs[i].dataType());
            names[i] = projectedAttrs[i].name();
        }
        return new ColumnStatsAccumulator(s, names);
    }

    private ColumnStatsAccumulator(ColumnState[] states, String[] columnNames) {
        this.states = states;
        this.columnNames = columnNames;
    }

    /** True when no columns are being tracked — capture loop should skip block iteration. */
    public boolean isEmpty() {
        return states.length == 0;
    }

    /**
     * Feeds the block at page position {@code blockIndex} into the accumulator. Caller's
     * responsibility: indices must match the layout the accumulator was built for. Out-of-range
     * indices are silently ignored.
     */
    public void acceptBlockAt(int blockIndex, Block block) {
        if (blockIndex < 0 || blockIndex >= states.length) {
            return;
        }
        states[blockIndex].accept(block, scratch);
    }

    /**
     * Snapshots the current state into an immutable {@link ExternalStats.ColumnStats} map.
     * Safe to call only once per accumulator instance — call it from the iterator's close-time hook.
     */
    public Map<String, ExternalStats.ColumnStats> snapshot() {
        if (states.length == 0) {
            return Map.of();
        }
        Map<String, ExternalStats.ColumnStats> out = new LinkedHashMap<>(states.length);
        for (int i = 0; i < states.length; i++) {
            ColumnState s = states[i];
            out.put(columnNames[i], new ExternalStats.ColumnStats(s.nullCount, s.min, s.max));
        }
        return out;
    }

    private static final class ColumnState {
        /**
         * Cached classification of the column's data type so the per-value dispatch is a single
         * array indirection rather than a switch on an enum every cell.
         */
        private final byte typeOrdinal;
        private long nullCount;
        private Object min;
        private Object max;

        private static final byte T_UNTRACKED = 0;
        private static final byte T_BOOLEAN = 1;
        private static final byte T_INT = 2;
        private static final byte T_LONG = 3;
        private static final byte T_DOUBLE = 4;
        private static final byte T_BYTESREF = 5;

        ColumnState(DataType type) {
            this.typeOrdinal = classify(type);
        }

        private static byte classify(DataType type) {
            // Min/max bucketing is keyed strictly on whether the SIGNED comparator on the stored
            // representation matches the type's semantic order. Types where that contract fails
            // are pinned to T_UNTRACKED so the cache never captures wrong-ordering min/max:
            // - UNSIGNED_LONG: stored as signed long; signed compare flips for values >= 2^63.
            // - VERSION: byte-lex (e.g. "1.10" < "1.2") disagrees with semver ordering.
            // IP stays in T_BYTESREF because the 16-byte InetAddressPoint encoding's byte-lex
            // order matches IPv4/IPv6 address order by construction.
            return switch (type) {
                case BOOLEAN -> T_BOOLEAN;
                case INTEGER -> T_INT;
                case LONG, DATETIME, DATE_NANOS -> T_LONG;
                case DOUBLE -> T_DOUBLE;
                case KEYWORD, TEXT, IP -> T_BYTESREF;
                default -> T_UNTRACKED;
            };
        }

        void accept(Block block, BytesRef scratch) {
            // Null count is the only stat we maintain for untracked types — it is cheap enough to
            // keep universal because COUNT(col) pushdown depends on it for every column, even those
            // whose ESQL type lacks an ordered comparator.
            int positions = block.getPositionCount();
            byte t = typeOrdinal;
            for (int p = 0; p < positions; p++) {
                if (block.isNull(p)) {
                    nullCount++;
                    continue;
                }
                if (t == T_UNTRACKED) {
                    continue;
                }
                int valueCount = block.getValueCount(p);
                int firstValue = block.getFirstValueIndex(p);
                for (int v = 0; v < valueCount; v++) {
                    int idx = firstValue + v;
                    switch (t) {
                        case T_BOOLEAN -> updateBoolean(((BooleanBlock) block).getBoolean(idx));
                        case T_INT -> updateInt(((IntBlock) block).getInt(idx));
                        case T_LONG -> updateLong(((LongBlock) block).getLong(idx));
                        case T_DOUBLE -> updateDouble(((DoubleBlock) block).getDouble(idx));
                        case T_BYTESREF -> updateBytesRef(((BytesRefBlock) block).getBytesRef(idx, scratch));
                        default -> throw new AssertionError("unexpected type ordinal: " + t);
                    }
                }
            }
        }

        private void updateBoolean(boolean val) {
            Boolean cur = (Boolean) min;
            if (cur == null || (cur && val == false)) {
                min = val;
            }
            Boolean curMax = (Boolean) max;
            if (curMax == null || (curMax == false && val)) {
                max = val;
            }
        }

        private void updateInt(int val) {
            Integer cur = (Integer) min;
            if (cur == null || val < cur) {
                min = val;
            }
            Integer curMax = (Integer) max;
            if (curMax == null || val > curMax) {
                max = val;
            }
        }

        private void updateLong(long val) {
            Long cur = (Long) min;
            if (cur == null || val < cur) {
                min = val;
            }
            Long curMax = (Long) max;
            if (curMax == null || val > curMax) {
                max = val;
            }
        }

        private void updateDouble(double val) {
            // NaN comparisons always return false, so a leading NaN would wedge the tracker on NaN
            // forever (no value compares < NaN or > NaN). Skip NaN entirely — it neither shrinks min
            // nor extends max under standard SQL ordering. The non-null count still includes the cell
            // because the outer block.isNull(p) check has already ruled out the SQL-null case.
            if (Double.isNaN(val)) {
                return;
            }
            Double cur = (Double) min;
            if (cur == null || val < cur) {
                min = val;
            }
            Double curMax = (Double) max;
            if (curMax == null || val > curMax) {
                max = val;
            }
        }

        private void updateBytesRef(BytesRef val) {
            BytesRef cur = (BytesRef) min;
            if (cur == null || val.compareTo(cur) < 0) {
                min = BytesRef.deepCopyOf(val);
            }
            BytesRef curMax = (BytesRef) max;
            if (curMax == null || val.compareTo(curMax) > 0) {
                max = BytesRef.deepCopyOf(val);
            }
        }
    }
}
