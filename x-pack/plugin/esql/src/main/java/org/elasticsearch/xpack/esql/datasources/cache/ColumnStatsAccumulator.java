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
import org.elasticsearch.xpack.esql.datasources.ColumnStatTypeSupport;
import org.elasticsearch.xpack.esql.datasources.spi.StripeColumnScope;

import java.util.LinkedHashMap;
import java.util.List;
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

    /**
     * Builds an accumulator keyed to a file's FULL positional schema, for the {@link
     * org.elasticsearch.xpack.esql.datasources.spi.StripeColumnScope#ALL ALL} harvest scope. Identical
     * machinery to {@link #forProjectedAttributes} — every tracked file column gets a {@link ColumnState}
     * indexed by its position in {@code fileSchema} — but the row-format readers feed it raw parsed/typed
     * values via {@link #acceptValueAt(int, Object)} (one per file column, regardless of projection) rather
     * than output {@link Block}s, because the output page only carries the query's projected columns. Feeding
     * a Block via {@link #acceptBlockAt(int, Block)} and feeding a single value via {@link #acceptValueAt}
     * update the same per-column state, so a mixed call sequence is well-defined.
     */
    public static ColumnStatsAccumulator forSchema(List<Attribute> fileSchema) {
        if (fileSchema == null || fileSchema.isEmpty()) {
            return new ColumnStatsAccumulator(new ColumnState[0], new String[0]);
        }
        return forProjectedAttributes(fileSchema.toArray(new Attribute[0]));
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

    // Primitive single-value feed: lets a typed producer (e.g. the direct-to-block CSV parser) accumulate
    // stats straight from the values it already has, avoiding a second, megamorphic pass over the built
    // blocks. Each call contributes exactly one (single-valued) cell at page position {@code blockIndex};
    // the result is identical to feeding the equivalent single-valued block through {@link #acceptBlockAt}.
    // Values for columns whose type is untracked contribute nothing beyond the null count.

    public void acceptNullAt(int blockIndex) {
        if (blockIndex >= 0 && blockIndex < states.length) {
            states[blockIndex].nullCount++;
        }
    }

    public void acceptBooleanAt(int blockIndex, boolean value) {
        if (blockIndex >= 0 && blockIndex < states.length) {
            states[blockIndex].acceptBoolean(value);
        }
    }

    public void acceptIntAt(int blockIndex, int value) {
        if (blockIndex >= 0 && blockIndex < states.length) {
            states[blockIndex].acceptInt(value);
        }
    }

    public void acceptLongAt(int blockIndex, long value) {
        if (blockIndex >= 0 && blockIndex < states.length) {
            states[blockIndex].acceptLong(value);
        }
    }

    public void acceptDoubleAt(int blockIndex, double value) {
        if (blockIndex >= 0 && blockIndex < states.length) {
            states[blockIndex].acceptDouble(value);
        }
    }

    public void acceptBytesRefAt(int blockIndex, BytesRef value) {
        if (blockIndex >= 0 && blockIndex < states.length) {
            states[blockIndex].acceptBytesRef(value);
        }
    }

    /**
     * Feeds a single already-typed value into the accumulator at column position {@code columnIndex}.
     * This is the {@link StripeColumnScope#ALL ALL}-scope entry point: the row-format readers hand a
     * boxed value parsed straight from the raw record (no output {@link Block} exists for an unprojected
     * file column). Accepted value shapes match what the readers' type-conversion produces:
     * <ul>
     *   <li>{@code null} — counts toward {@code nullCount}.</li>
     *   <li>{@link Boolean} / {@link Integer} / {@link Long} / {@link Double} / {@link BytesRef} —
     *   a single value, folded into min/max for tracked types.</li>
     *   <li>{@link List} — a multi-valued cell; every element folds individually (matching the Block
     *   path's per-value contract). An empty list counts as null.</li>
     * </ul>
     * Out-of-range indices are silently ignored, matching {@link #acceptBlockAt}.
     */
    public void acceptValueAt(int columnIndex, Object value) {
        if (columnIndex < 0 || columnIndex >= states.length) {
            return;
        }
        states[columnIndex].acceptValue(value);
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
            out.put(columnNames[i], new ExternalStats.ColumnStats(s.nullCount, s.valueCount, s.min, s.max));
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
        private long valueCount; // non-null VALUES (multivalue-aware); COUNT(col) is served from this
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
            // representation matches the type's semantic order. Types where that contract fails are
            // T_UNTRACKED so the cache never captures wrong-ordering min/max:
            // - UNSIGNED_LONG: stored as signed long; signed compare flips for values >= 2^63.
            // - VERSION: byte-lex (e.g. "1.10" < "1.2") disagrees with semver ordering.
            // - counters: no MIN/MAX harvest.
            // IP stays in T_BYTESREF because the 16-byte InetAddressPoint encoding's byte-lex
            // order matches IPv4/IPv6 address order by construction.
            // The harvestable + blockKind facts come from the shared ColumnStatTypeSupport table so this
            // classification cannot drift from the warm-path serving / text-pushdown gates.
            ColumnStatTypeSupport support = ColumnStatTypeSupport.of(type);
            if (support == null || support.harvestable() == false) {
                return T_UNTRACKED;
            }
            return switch (support.blockKind()) {
                case BOOLEAN -> T_BOOLEAN;
                case INT -> T_INT;
                case LONG -> T_LONG;
                case DOUBLE -> T_DOUBLE;
                case BYTES_REF -> T_BYTESREF;
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
                int vc = block.getValueCount(p);
                // COUNT(col) counts non-null VALUES, not rows: a multivalued position contributes vc>1.
                // Tracked over every column (incl. T_UNTRACKED) so COUNT(col) pushdown is correct regardless
                // of whether min/max are also tracked.
                valueCount += vc;
                if (t == T_UNTRACKED) {
                    continue;
                }
                int firstValue = block.getFirstValueIndex(p);
                for (int v = 0; v < vc; v++) {
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

        /**
         * Value-oriented twin of {@link #accept(Block, BytesRef)} for the ALL-scope side-pass: folds one
         * boxed value (or every element of a multi-valued {@link List}) into this column's running stats.
         * Null and empty-list both count toward {@code nullCount}; untracked types contribute null counts
         * only, exactly like the Block path.
         */
        void acceptValue(Object value) {
            if (value == null) {
                nullCount++;
                return;
            }
            if (value instanceof List<?> list) {
                if (list.isEmpty()) {
                    nullCount++;
                    return;
                }
                for (Object element : list) {
                    acceptScalar(element);
                }
                return;
            }
            acceptScalar(value);
        }

        private void acceptScalar(Object value) {
            if (value == null) {
                nullCount++;
                return;
            }
            valueCount++; // one non-null value (called once per scalar and per non-null multivalue element)
            switch (typeOrdinal) {
                case T_UNTRACKED -> {
                    // Null count only — mirrors the Block path's universal null tracking for untracked types.
                }
                case T_BOOLEAN -> updateBoolean((Boolean) value);
                case T_INT -> updateInt((Integer) value);
                case T_LONG -> updateLong((Long) value);
                case T_DOUBLE -> updateDouble((Double) value);
                case T_BYTESREF -> updateBytesRef((BytesRef) value);
                default -> throw new AssertionError("unexpected type ordinal: " + typeOrdinal);
            }
        }

        // Single-value feed counterparts of the block walk in accept(...), for main's direct-to-block CSV/TSV
        // path (elastic/elasticsearch#152300). Each is called once per non-null staged cell, so it increments
        // valueCount UNCONDITIONALLY (COUNT(col) is served from it — see accept(Block), which counts every
        // non-null value before the T_UNTRACKED branch), then guards on the column's cached typeOrdinal so a
        // value whose kind does not match the tracked type (e.g. an untracked column) contributes nothing to
        // min/max, exactly as the block path's T_UNTRACKED branch does.

        void acceptBoolean(boolean val) {
            valueCount++;
            if (typeOrdinal == T_BOOLEAN) {
                updateBoolean(val);
            }
        }

        void acceptInt(int val) {
            valueCount++;
            if (typeOrdinal == T_INT) {
                updateInt(val);
            }
        }

        void acceptLong(long val) {
            valueCount++;
            if (typeOrdinal == T_LONG) {
                updateLong(val);
            }
        }

        void acceptDouble(double val) {
            valueCount++;
            if (typeOrdinal == T_DOUBLE) {
                updateDouble(val);
            }
        }

        void acceptBytesRef(BytesRef val) {
            assert val != null : "acceptBytesRef requires a non-null value; nulls route through acceptNullAt";
            valueCount++;
            if (typeOrdinal == T_BYTESREF) {
                updateBytesRef(val);
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
            // The runtime Min/MaxDoubleAggregator use Math.min/Math.max, which PROPAGATE NaN: once any value
            // is NaN the aggregate is NaN. Record NaN as both extremes so a served stat matches a full scan;
            // it stays NaN because no later value compares < or > NaN. The cross-stripe/cross-file fold
            // (SplitStats.mergedMin/mergedMax) treats a NaN operand as unmergeable, so a multi-unit column
            // safe-misses there instead of serving a non-NaN extreme — MIN/MAX matches a scan in every shape.
            if (Double.isNaN(val)) {
                min = Double.NaN;
                max = Double.NaN;
                return;
            }
            // Use the SAME Math.min/Math.max the runtime Min/MaxDoubleAggregator use so a served extreme is
            // bit-identical to a full scan's — a primitive `<`/`>` compares -0.0 == 0.0 and could not cross
            // them, but Math.min/max order -0.0 below +0.0 (and MIN(-0.0, +0.0) must be -0.0).
            Double cur = (Double) min;
            min = cur == null ? val : Math.min(cur, val);
            Double curMax = (Double) max;
            max = curMax == null ? val : Math.max(curMax, val);
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
