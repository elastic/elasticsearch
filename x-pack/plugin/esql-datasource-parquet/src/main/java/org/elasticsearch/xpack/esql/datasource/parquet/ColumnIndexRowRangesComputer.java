/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.api.Binary;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * Computes {@link RowRanges} from a Parquet {@link FilterPredicate} by evaluating per-page
 * min/max statistics from {@link ColumnIndex}. This implements our own page-level filtering
 * rather than using parquet-mr's internal {@code ColumnIndexFilter}, which lives in an
 * unstable internal package and returns a different RowRanges type.
 *
 * <p>For each leaf predicate, the visitor checks every page's min/max values against the
 * predicate and builds row ranges from surviving pages. Logical operators compose these
 * ranges via set operations: AND → intersect, OR → union. NOT conservatively returns all
 * rows because complement of page-level ranges would drop rows from mixed pages.
 *
 * <p>When a column has no ColumnIndex or OffsetIndex (e.g., columns with no statistics,
 * or very old Parquet writers), the visitor conservatively returns {@link RowRanges#all}.
 *
 * <p>Correctness is maintained by RECHECK semantics: all pushed filters use
 * {@code Pushability.RECHECK}, so the original FilterExec remains in the ESQL plan
 * for per-row correctness. This visitor is a conservative approximation that may keep
 * pages with partial matches.
 */
final class ColumnIndexRowRangesComputer implements FilterPredicate.Visitor<RowRanges> {

    private final PreloadedRowGroupMetadata metadata;
    private final int rowGroupOrdinal;
    private final long rowGroupRowCount;

    private ColumnIndexRowRangesComputer(PreloadedRowGroupMetadata metadata, int rowGroupOrdinal, long rowGroupRowCount) {
        this.metadata = metadata;
        this.rowGroupOrdinal = rowGroupOrdinal;
        this.rowGroupRowCount = rowGroupRowCount;
    }

    /**
     * Computes the RowRanges for a given predicate within a row group.
     *
     * @param predicate the filter predicate (from parquet-mr FilterApi)
     * @param metadata preloaded ColumnIndex/OffsetIndex data
     * @param rowGroupOrdinal physical ordinal of the row group in the file
     * @param rowGroupRowCount total rows in the row group
     * @return selected row ranges, or {@code RowRanges.all()} if filtering is not beneficial
     */
    static RowRanges compute(FilterPredicate predicate, PreloadedRowGroupMetadata metadata, int rowGroupOrdinal, long rowGroupRowCount) {
        if (predicate == null) {
            return RowRanges.all(rowGroupRowCount);
        }
        var computer = new ColumnIndexRowRangesComputer(metadata, rowGroupOrdinal, rowGroupRowCount);
        RowRanges result = predicate.accept(computer);
        if (result.shouldDiscard()) {
            return RowRanges.all(rowGroupRowCount);
        }
        return result;
    }

    private RowRanges all() {
        return RowRanges.all(rowGroupRowCount);
    }

    // --- Leaf predicates ---

    @Override
    public <T extends Comparable<T>> RowRanges visit(Operators.Eq<T> eq) {
        T value = eq.getValue();
        if (value == null) {
            return all();
        }
        return evaluateLeaf(eq.getColumn(), (min, max) -> min.compareTo(value) <= 0 && value.compareTo(max) <= 0);
    }

    @Override
    public <T extends Comparable<T>> RowRanges visit(Operators.NotEq<T> notEq) {
        // NotEq can only skip a page when ALL values equal the target (min==max==value, no nulls).
        // This is rare and checking null counts adds complexity. Conservative: keep all pages.
        return all();
    }

    @Override
    public <T extends Comparable<T>> RowRanges visit(Operators.Lt<T> lt) {
        T value = lt.getValue();
        return evaluateLeaf(lt.getColumn(), (min, max) -> min.compareTo(value) < 0);
    }

    @Override
    public <T extends Comparable<T>> RowRanges visit(Operators.LtEq<T> ltEq) {
        T value = ltEq.getValue();
        return evaluateLeaf(ltEq.getColumn(), (min, max) -> min.compareTo(value) <= 0);
    }

    @Override
    public <T extends Comparable<T>> RowRanges visit(Operators.Gt<T> gt) {
        T value = gt.getValue();
        return evaluateLeaf(gt.getColumn(), (min, max) -> max.compareTo(value) > 0);
    }

    @Override
    public <T extends Comparable<T>> RowRanges visit(Operators.GtEq<T> gtEq) {
        T value = gtEq.getValue();
        return evaluateLeaf(gtEq.getColumn(), (min, max) -> max.compareTo(value) >= 0);
    }

    @Override
    public <T extends Comparable<T>> RowRanges visit(Operators.In<T> in) {
        return evaluateLeaf(in.getColumn(), (min, max) -> {
            for (T val : in.getValues()) {
                if (val != null && min.compareTo(val) <= 0 && val.compareTo(max) <= 0) {
                    return true;
                }
            }
            return false;
        });
    }

    @Override
    public <T extends Comparable<T>> RowRanges visit(Operators.NotIn<T> notIn) {
        return all();
    }

    @Override
    public <T extends Comparable<T>> RowRanges visit(Operators.Contains<T> contains) {
        return all();
    }

    // --- Logical operators ---

    @Override
    public RowRanges visit(Operators.And and) {
        RowRanges left = and.getLeft().accept(this);
        RowRanges right = and.getRight().accept(this);
        return left.intersect(right);
    }

    @Override
    public RowRanges visit(Operators.Or or) {
        RowRanges left = or.getLeft().accept(this);
        RowRanges right = or.getRight().accept(this);
        return left.union(right);
    }

    @Override
    public RowRanges visit(Operators.Not not) {
        // Conservative: complement of page-level ranges loses rows from mixed pages.
        // A page kept by the inner predicate may also contain non-matching rows; dropping
        // it via complement() silently loses those rows, violating the "never fewer" contract.
        return all();
    }

    @Override
    public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> RowRanges visit(Operators.UserDefined<T, U> udp) {
        return all();
    }

    @Override
    public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> RowRanges visit(Operators.LogicalNotUserDefined<T, U> udp) {
        return all();
    }

    // --- Core evaluation ---

    @FunctionalInterface
    private interface PagePredicate<T extends Comparable<T>> {
        boolean test(T min, T max);
    }

    /**
     * Evaluates a leaf predicate against each page's min/max from the ColumnIndex.
     * Builds RowRanges from the pages whose min/max satisfy the predicate.
     */
    private <T extends Comparable<T>> RowRanges evaluateLeaf(Operators.Column<T> column, PagePredicate<T> predicate) {
        String columnPath = column.getColumnPath().toDotString();
        ColumnIndex ci = metadata.getColumnIndex(rowGroupOrdinal, columnPath);
        OffsetIndex oi = metadata.getOffsetIndex(rowGroupOrdinal, columnPath);
        if (ci == null || oi == null) {
            return all();
        }

        int pageCount = oi.getPageCount();
        List<ByteBuffer> minValues = ci.getMinValues();
        List<ByteBuffer> maxValues = ci.getMaxValues();
        List<Boolean> nullPages = ci.getNullPages();

        if (minValues.size() != pageCount || maxValues.size() != pageCount) {
            return all();
        }

        List<long[]> surviving = new ArrayList<>();
        for (int p = 0; p < pageCount; p++) {
            if (nullPages != null && p < nullPages.size() && Boolean.TRUE.equals(nullPages.get(p))) {
                continue;
            }

            T min = decodeValue(minValues.get(p), column);
            T max = decodeValue(maxValues.get(p), column);
            if (min == null || max == null) {
                long pageStart = oi.getFirstRowIndex(p);
                long pageEnd = (p + 1 < pageCount) ? oi.getFirstRowIndex(p + 1) : rowGroupRowCount;
                surviving.add(new long[] { pageStart, pageEnd });
                continue;
            }

            if (predicate.test(min, max)) {
                long pageStart = oi.getFirstRowIndex(p);
                long pageEnd = (p + 1 < pageCount) ? oi.getFirstRowIndex(p + 1) : rowGroupRowCount;
                surviving.add(new long[] { pageStart, pageEnd });
            }
        }

        if (surviving.isEmpty()) {
            return RowRanges.of(0, 0, rowGroupRowCount);
        }
        return RowRanges.fromUnsorted(surviving, rowGroupRowCount);
    }

    @SuppressWarnings("unchecked")
    private static <T extends Comparable<T>> T decodeValue(ByteBuffer buf, Operators.Column<T> column) {
        if (buf == null || buf.remaining() == 0) {
            return null;
        }
        ByteBuffer ordered = buf.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        Class<T> type = column.getColumnType();
        if (type == Integer.class) {
            return (T) Integer.valueOf(ordered.getInt());
        } else if (type == Long.class) {
            return (T) Long.valueOf(ordered.getLong());
        } else if (type == Double.class) {
            // Buffer length distinguishes physical FLOAT (4 bytes) from DOUBLE (8 bytes);
            // parquet-mr's Column API only exposes Double.class for both floatColumn and doubleColumn.
            if (ordered.remaining() == Float.BYTES) {
                return (T) Double.valueOf(ordered.getFloat());
            }
            return (T) Double.valueOf(ordered.getDouble());
        } else if (type == Boolean.class) {
            return (T) Boolean.valueOf(ordered.get() != 0);
        } else if (type == Binary.class) {
            byte[] bytes = new byte[buf.remaining()];
            buf.duplicate().get(bytes);
            return (T) Binary.fromConstantByteArray(bytes);
        }
        return null;
    }
}
