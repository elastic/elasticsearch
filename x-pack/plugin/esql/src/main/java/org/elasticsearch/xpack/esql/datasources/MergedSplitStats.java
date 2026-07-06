/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Lazily merges statistics from a list of child {@link org.elasticsearch.xpack.esql.datasources.spi.SplitStats} instances.
 * Used by {@link CoalescedSplit#splitStats()} to expose aggregate statistics for
 * composite splits without eagerly materializing a full {@link SplitStats}.
 * <p>
 * Each accessor method computes its result on demand by iterating the children.
 * This is appropriate because the optimizer calls these methods at most a few times
 * per planning phase, and the child count is typically small (a few dozen splits).
 */
public final class MergedSplitStats implements org.elasticsearch.xpack.esql.datasources.spi.SplitStats {

    private final List<org.elasticsearch.xpack.esql.datasources.spi.SplitStats> children;
    /**
     * Per-child column name to ESQL {@link DataType}, parallel to {@link #children}. Entries (and the whole
     * list) may be {@code null} when a child carries no per-file schema (older nodes, self-inferring sources).
     * Used only to reconcile temporal resolution during {@link #columnMin}/{@link #columnMax}: {@code DATETIME}
     * stats are epoch-millis and {@code DATE_NANOS} stats are epoch-nanos, but both are {@code Long} at the Java
     * level, so a value-only merge would compare them unit-blind. See {@link #mergeExtremum}.
     */
    @Nullable
    private final List<Map<String, DataType>> childColumnTypes;

    public MergedSplitStats(List<org.elasticsearch.xpack.esql.datasources.spi.SplitStats> children) {
        this(children, null);
    }

    public MergedSplitStats(
        List<org.elasticsearch.xpack.esql.datasources.spi.SplitStats> children,
        @Nullable List<Map<String, DataType>> childColumnTypes
    ) {
        if (children == null || children.isEmpty()) {
            throw new IllegalArgumentException("children cannot be null or empty");
        }
        if (childColumnTypes != null && childColumnTypes.size() != children.size()) {
            throw new IllegalArgumentException("childColumnTypes must be parallel to children");
        }
        this.children = List.copyOf(children);
        // Entries may be null, so this defensive copy cannot use List.copyOf (which rejects null elements).
        this.childColumnTypes = childColumnTypes == null ? null : Collections.unmodifiableList(new ArrayList<>(childColumnTypes));
    }

    /** Returns the list of child stats that this instance merges. */
    public List<org.elasticsearch.xpack.esql.datasources.spi.SplitStats> children() {
        return children;
    }

    /**
     * Extracts a column-name to ESQL {@link DataType} map from a split's per-file {@code readSchema}, or
     * {@code null} when the split carries no schema pin ({@link FileSplit#readSchema()} is {@code null}, e.g.
     * older nodes or self-inferring sources). Used to reconcile temporal resolution (epoch-millis
     * {@code DATETIME} vs epoch-nanos {@code DATE_NANOS}) across files before merging min/max stats. Under
     * UNION_BY_NAME the per-file schema names match the unified stat keys, so no mapping translation is needed.
     */
    @Nullable
    static Map<String, DataType> readSchemaTypes(ExternalSplit split) {
        List<Attribute> schema = split instanceof FileSplit fileSplit ? fileSplit.readSchema() : null;
        if (schema == null || schema.isEmpty()) {
            return null;
        }
        Map<String, DataType> types = new HashMap<>(schema.size());
        for (Attribute attr : schema) {
            types.put(attr.name(), attr.dataType());
        }
        return types;
    }

    @Override
    public long rowCount() {
        long total = 0;
        for (org.elasticsearch.xpack.esql.datasources.spi.SplitStats child : children) {
            total += child.rowCount();
        }
        return total;
    }

    /**
     * Returns the sum of children's uncompressed sizes, or {@code -1} if any child
     * reports an unknown size. A single unknown child poisons the aggregate because
     * we cannot give a meaningful partial sum to the optimizer.
     */
    @Override
    public long sizeInBytes() {
        long total = 0;
        for (org.elasticsearch.xpack.esql.datasources.spi.SplitStats child : children) {
            long s = child.sizeInBytes();
            if (s < 0) {
                return -1;
            }
            total += s;
        }
        return total;
    }

    /**
     * Returns the sum of children's compressed sizes, or {@code -1} if any child
     * reports an unknown compressed size.
     */
    @Override
    public long compressedSizeInBytes() {
        long total = 0;
        for (org.elasticsearch.xpack.esql.datasources.spi.SplitStats child : children) {
            long s = child.compressedSizeInBytes();
            if (s < 0) {
                return -1;
            }
            total += s;
        }
        return total;
    }

    /**
     * Returns the sum of null counts across children for the named column under the SPI's
     * "implicit nulls" contract: a child whose split lacks the column contributes its full
     * row count (every row is an implicit null), and explicit nulls in present columns are
     * summed normally. Returns {@code -1} only if a child returns {@code -1}, which signals
     * the rare "column physically present but stats unknown" case (Parquet stats disabled).
     */
    @Override
    public long columnNullCount(String name) {
        long total = 0;
        for (org.elasticsearch.xpack.esql.datasources.spi.SplitStats child : children) {
            long nc = child.columnNullCount(name);
            if (nc < 0) {
                return -1;
            }
            total += nc;
        }
        return total;
    }

    /**
     * Returns the minimum of children's min values for the named column under the SPI's
     * "implicit nulls" contract:
     * <ul>
     *   <li>A child with {@code columnNullCount(name) == child.rowCount()} contributes no candidate
     *       min — the column is either absent from that file or its rows are all null. We
     *       <b>skip</b> that child rather than poison.</li>
     *   <li>A child with {@code columnNullCount(name) < 0} represents the rare "present but stats
     *       unknown" case (Parquet stats disabled) and <b>poisons</b> the aggregate; we cannot
     *       know whether that child has a smaller value than the running min.</li>
     *   <li>A child with a known, finite null count and a non-null min participates in the merge
     *       via {@link SplitStats#mergedMin}; incompatible numeric types poison defensively.</li>
     * </ul>
     * Returns {@code null} when poisoned or when no child contributes a value.
     */
    @Override
    @Nullable
    public Object columnMin(String name) {
        return mergeExtremum(name, true);
    }

    /**
     * Returns the maximum of children's max values for the named column under the SPI's
     * "implicit nulls" contract. Mirrors {@link #columnMin} — children whose null count equals
     * their row count have no max value to contribute and are skipped; only an explicit
     * unknown ({@code columnNullCount &lt; 0}) poisons the aggregate.
     */
    @Override
    @Nullable
    public Object columnMax(String name) {
        return mergeExtremum(name, false);
    }

    /**
     * Shared min/max merge across children, unit-aware for temporal columns. Follows the SPI's implicit-nulls
     * contract (children whose null count equals their row count contribute no candidate; an unknown null count
     * poisons; a present-but-valueless child poisons).
     * <p>
     * When {@link #childColumnTypes} shows the column mixes temporal resolutions across files (a
     * {@code DATETIME}/epoch-millis file and a {@code DATE_NANOS}/epoch-nanos file for the same column, which
     * {@code SchemaReconciliation} widens to {@code DATE_NANOS}), each contributing value is rescaled to the
     * finer unit (epoch-nanos) before comparison, so the returned extremum is in the reconciled unit. A
     * contributing child whose type is unknown while others are temporal, or a millis value that overflows the
     * nanosecond range, poisons the stat (returns {@code null}) so the aggregate falls back to a scan.
     */
    @Nullable
    private Object mergeExtremum(String name, boolean wantMin) {
        DataType targetUnit = temporalTarget(name);
        boolean temporal = targetUnit != null;
        Object result = null;
        for (int i = 0; i < children.size(); i++) {
            org.elasticsearch.xpack.esql.datasources.spi.SplitStats child = children.get(i);
            long nc = child.columnNullCount(name);
            if (nc < 0) {
                return null;
            }
            if (nc == child.rowCount()) {
                continue;
            }
            Object value = wantMin ? child.columnMin(name) : child.columnMax(name);
            if (value == null) {
                // Present, not all-null, but reader produced no extremum — inconsistent; poison defensively.
                return null;
            }
            if (temporal) {
                DataType childType = columnType(i, name);
                if (childType != DataType.DATETIME && childType != DataType.DATE_NANOS) {
                    // Column is temporal in at least one file but this contributing child's unit is unknown;
                    // we cannot safely rescale it, so poison rather than compare unit-blind.
                    return null;
                }
                value = rescaleTemporal(value, childType, targetUnit);
                if (value == null) {
                    return null;
                }
            }
            result = wantMin ? SplitStats.mergedMin(result, value) : SplitStats.mergedMax(result, value);
            if (result == null) {
                // Incompatible types — clear the stat.
                return null;
            }
        }
        return result;
    }

    /**
     * Returns the finest temporal resolution ({@code DATE_NANOS} &gt; {@code DATETIME}) declared for the column
     * across children that carry per-file types, or {@code null} when the column is non-temporal or no type
     * information is available (in which case the merge stays value-only, preserving prior behavior).
     */
    @Nullable
    private DataType temporalTarget(String name) {
        if (childColumnTypes == null) {
            return null;
        }
        DataType target = null;
        for (int i = 0; i < children.size(); i++) {
            DataType t = columnType(i, name);
            if (t == DataType.DATE_NANOS) {
                return DataType.DATE_NANOS;
            }
            if (t == DataType.DATETIME) {
                target = DataType.DATETIME;
            }
        }
        return target;
    }

    @Nullable
    private DataType columnType(int childIndex, String name) {
        if (childColumnTypes == null) {
            return null;
        }
        Map<String, DataType> types = childColumnTypes.get(childIndex);
        return types == null ? null : types.get(name);
    }

    /**
     * Rescales a temporal stat value from its source unit to {@code targetUnit}. Only a {@code DATETIME}
     * (epoch-millis) value widened to a {@code DATE_NANOS} (epoch-nanos) target needs scaling (×1_000_000);
     * all other combinations pass through. Returns {@code null} if the millis value has no nanosecond
     * representation (outside ~1677-2262), signalling the caller to poison the stat.
     */
    @Nullable
    private static Object rescaleTemporal(Object value, DataType fromUnit, DataType targetUnit) {
        if (fromUnit == DataType.DATETIME && targetUnit == DataType.DATE_NANOS) {
            try {
                return Math.multiplyExact(((Number) value).longValue(), 1_000_000L);
            } catch (ArithmeticException overflow) {
                return null;
            }
        }
        return value;
    }

    /**
     * Returns the sum of per-column sizes across children, or {@code -1} if any child
     * returns an unknown size for the column.
     */
    @Override
    public long columnSizeBytes(String name) {
        long total = 0;
        for (org.elasticsearch.xpack.esql.datasources.spi.SplitStats child : children) {
            long sz = child.columnSizeBytes(name);
            if (sz < 0) {
                return -1;
            }
            total += sz;
        }
        return total;
    }

    @Override
    public String toString() {
        return "MergedSplitStats[children=" + children.size() + "]";
    }
}
