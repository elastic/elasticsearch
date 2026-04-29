/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.util.List;
import java.util.Objects;

/**
 * Runtime, in-JVM specification handed to {@link AggregateScanReader#scanForAggregates}.
 * Describes the aggregate operations to compute, plus the parent {@code AggregateExec}'s
 * intermediate-state attributes so the reader knows the exact block shape to emit per
 * row group.
 * <p>
 * The {@code i}-th {@link AggOp} corresponds to the slice of {@code intermediateAttributes}
 * that backs that aggregate's intermediate state. For {@link AggOp.CountStar},
 * {@link AggOp.CountField}, {@link AggOp.MinField}, and {@link AggOp.MaxField} this slice
 * is always 2 attributes wide (value block + {@code seen} boolean block); concretely
 * {@code intermediateAttributes.get(i * 2)} is the value attribute and
 * {@code intermediateAttributes.get(i * 2 + 1)} is the seen attribute.
 * <p>
 * This type is purely runtime: the planner builds a physical plan node carrying
 * {@code List<NamedExpression>} as today; the data-node-local operator factory lowers
 * those expressions to {@link AggregateScanSpec} once at construction time. {@link AggOp}
 * is never serialized.
 */
public record AggregateScanSpec(List<AggOp> ops, List<Attribute> intermediateAttributes) {

    public AggregateScanSpec {
        Objects.requireNonNull(ops, "ops");
        Objects.requireNonNull(intermediateAttributes, "intermediateAttributes");
        if (intermediateAttributes.size() != ops.size() * 2) {
            throw new IllegalArgumentException(
                "intermediateAttributes size ["
                    + intermediateAttributes.size()
                    + "] must be exactly 2x ops size ["
                    + ops.size()
                    + "] (each Count/Min/Max produces value + seen blocks)"
            );
        }
        ops = List.copyOf(ops);
        intermediateAttributes = List.copyOf(intermediateAttributes);
    }

    /**
     * One supported aggregate operation. New cases (e.g. {@code Sum}) extend the sealed
     * hierarchy and require matching accumulator branches in each {@link AggregateScanReader}
     * implementation.
     */
    public sealed interface AggOp permits AggOp.CountStar, AggOp.CountField, AggOp.MinField, AggOp.MaxField {

        /** {@code COUNT(*)} or {@code COUNT(foldable)} — value derives only from row count. */
        record CountStar() implements AggOp {}

        /**
         * {@code COUNT(field)} — counts every non-null leaf value (multi-valued positions
         * contribute every value), matching
         * {@link org.elasticsearch.compute.aggregation.CountAggregatorFunction}. A row with
         * {@code [1, 2, 3]} contributes 3.
         * <p>
         * Whether the implementation actually achieves this depends on the path: the slow
         * (row-data) path sums {@code Block#getValueCount} per position and is correct, but
         * the fast (stats-only) path in {@code ParquetAggregateScanIterator} approximates
         * with {@code rowCount - nullCount}, which yields "rows with at least one value" on
         * multi-valued columns. The planning-time pushdown
         * ({@code PushAggregatesToExternalSource#resolveFromStats}) shares the same fast-path
         * divergence; tracked separately so a future fix can correct both stat-using sites
         * with {@code ColumnChunkMetaData.getValueCount() - getNumNulls()}.
         *
         * @param column dotted column name, supporting nested types (e.g. "height.float")
         */
        record CountField(String column) implements AggOp {
            public CountField {
                Objects.requireNonNull(column, "column");
            }
        }

        /**
         * {@code MIN(field)} — folds over all values within each position (multi-valued
         * positions contribute every value to the comparison).
         */
        record MinField(String column) implements AggOp {
            public MinField {
                Objects.requireNonNull(column, "column");
            }
        }

        /**
         * {@code MAX(field)} — folds over all values within each position (multi-valued
         * positions contribute every value to the comparison).
         */
        record MaxField(String column) implements AggOp {
            public MaxField {
                Objects.requireNonNull(column, "column");
            }
        }
    }
}
