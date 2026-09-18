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
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.elasticsearch.compute.data.UninitializedArrays;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Computes {@link RowRanges} from a Parquet {@link FilterPredicate} by evaluating per-page
 * min/max statistics from {@link ColumnIndex}. This implements our own page-level filtering
 * rather than using parquet-mr's internal {@code ColumnIndexFilter}, which lives in an
 * unstable internal package and returns a different RowRanges type.
 *
 * <p>For each leaf predicate, the visitor checks every page's min/max values against the
 * predicate and builds row ranges from surviving pages. Logical operators compose these
 * ranges via set operations: AND → intersect, OR → union. The logical {@code NOT}
 * conservatively returns all rows because complement of page-level ranges would drop rows
 * from mixed pages. Leaf {@code NotEq} is handled directly: pages with min == max == value
 * can be pruned because every row on such a page fails {@code != value}.
 *
 * <p>Comparisons never use {@link Comparable#compareTo} directly: for an unsigned physical
 * column (e.g. a Parquet {@code uint32} widened to ESQL {@code LONG} — esql-planning#1030),
 * {@code Integer}/{@code Long}'s natural ordering is signed and would misread a raw value
 * above {@code Integer.MAX_VALUE}/{@code Long.MAX_VALUE} as negative, causing this
 * conservative page-pruning visitor to actually drop matching pages instead of over-keeping
 * them. {@link #comparatorFor} resolves an unsigned-aware {@link Comparator} from the
 * column's {@link LogicalTypeAnnotation.IntLogicalTypeAnnotation} so ordered comparisons
 * agree with how the ColumnIndex's own min/max were computed.
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
        return evaluateLeaf(eq.getColumn(), (min, max, cmp) -> cmp.compare(min, value) <= 0 && cmp.compare(value, max) <= 0);
    }

    @Override
    public <T extends Comparable<T>> RowRanges visit(Operators.NotEq<T> notEq) {
        T value = notEq.getValue();
        if (value == null) {
            return all();
        }
        // A page survives NotEq(value) unless every non-null value on the page equals `value`,
        // i.e. min == max == value. Null rows also fail NotEq (NULL != X is NULL/false), so
        // when min == max == value all rows on the page can be pruned safely.
        return evaluateLeaf(notEq.getColumn(), (min, max, cmp) -> cmp.compare(min, value) != 0 || cmp.compare(max, value) != 0);
    }

    @Override
    public <T extends Comparable<T>> RowRanges visit(Operators.Lt<T> lt) {
        T value = lt.getValue();
        return evaluateLeaf(lt.getColumn(), (min, max, cmp) -> cmp.compare(min, value) < 0);
    }

    @Override
    public <T extends Comparable<T>> RowRanges visit(Operators.LtEq<T> ltEq) {
        T value = ltEq.getValue();
        return evaluateLeaf(ltEq.getColumn(), (min, max, cmp) -> cmp.compare(min, value) <= 0);
    }

    @Override
    public <T extends Comparable<T>> RowRanges visit(Operators.Gt<T> gt) {
        T value = gt.getValue();
        return evaluateLeaf(gt.getColumn(), (min, max, cmp) -> cmp.compare(max, value) > 0);
    }

    @Override
    public <T extends Comparable<T>> RowRanges visit(Operators.GtEq<T> gtEq) {
        T value = gtEq.getValue();
        return evaluateLeaf(gtEq.getColumn(), (min, max, cmp) -> cmp.compare(max, value) >= 0);
    }

    @Override
    public <T extends Comparable<T>> RowRanges visit(Operators.In<T> in) {
        return evaluateLeaf(in.getColumn(), (min, max, cmp) -> {
            for (T val : in.getValues()) {
                if (val != null && cmp.compare(min, val) <= 0 && cmp.compare(val, max) <= 0) {
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
        boolean test(T min, T max, Comparator<T> cmp);
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

        // Resolve the file-level primitive type from the captured MessageType so decodeValue can
        // pick the right reader (FLOAT vs DOUBLE) and reject non-native DOUBLE backings (DECIMAL,
        // Float16) instead of misreading the raw bytes. Uses the same containsField + getType
        // idiom as ParquetPushedExpressions#buildDatetimePredicate, which is correct for ESQL's
        // current flat-column pushdown surface (all leaf columns are top-level). When the column
        // is absent (e.g. legacy metadata built without a schema, or a path not in the file), we
        // fall back to keeping all rows so FilterExec rechecks per-row.
        MessageType schema = metadata.schema();
        if (schema.containsField(columnPath) == false) {
            return all();
        }
        PrimitiveType primitive = schema.getType(columnPath).asPrimitiveType();
        Comparator<T> cmp = comparatorFor(column.getColumnType(), primitive);

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

            T min = decodeValue(minValues.get(p), column, primitive);
            T max = decodeValue(maxValues.get(p), column, primitive);
            if (min == null || max == null) {
                long pageStart = oi.getFirstRowIndex(p);
                long pageEnd = (p + 1 < pageCount) ? oi.getFirstRowIndex(p + 1) : rowGroupRowCount;
                surviving.add(new long[] { pageStart, pageEnd });
                continue;
            }

            if (predicate.test(min, max, cmp)) {
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

    /**
     * Resolves the comparator ordered comparisons must use for this column's min/max. Parquet's
     * writer-side {@code ColumnIndexBuilder} computes min/max using the column's logical-type-aware
     * ordering, so an unsigned {@code INT32}/{@code INT64} column's stored max can be a raw value
     * that reads as negative under {@link Integer}/{@link Long}'s natural (signed) ordering (e.g. a
     * {@code uint32} value above {@code Integer.MAX_VALUE}). Comparisons here must use the same
     * unsigned ordering the writer used, or this visitor would misjudge such a page as not matching
     * and prune it — silently dropping matching rows rather than conservatively over-keeping them.
     */
    @SuppressWarnings("unchecked")
    private static <T extends Comparable<T>> Comparator<T> comparatorFor(Class<T> columnType, PrimitiveType primitive) {
        boolean unsigned = primitive.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogical
            && intLogical.isSigned() == false;
        if (unsigned && columnType == Integer.class) {
            return (Comparator<T>) (Comparator<Integer>) Integer::compareUnsigned;
        }
        if (unsigned && columnType == Long.class) {
            return (Comparator<T>) (Comparator<Long>) Long::compareUnsigned;
        }
        return Comparator.naturalOrder();
    }

    @SuppressWarnings("unchecked")
    private static <T extends Comparable<T>> T decodeValue(ByteBuffer buf, Operators.Column<T> column, PrimitiveType primitive) {
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
            // parquet-mr's Column API exposes Double.class for both floatColumn and doubleColumn,
            // so the predicate's column type alone is ambiguous. The file-level schema is the
            // authoritative source of the physical primitive — read the bytes accordingly and
            // reject anything else conservatively (returning null routes the page through the
            // `min == null || max == null` branch in evaluateLeaf, keeping the page so FilterExec
            // applies the predicate per row).
            //
            // NaN min/max are also unusable for page-level pruning: the parquet-format spec treats
            // NaN as a "no usable bound" sentinel, and Java's Comparable orders NaN above every
            // finite value, which both prunes real-data pages (e.g. Lt: Double.compare(NaN, V) < 0
            // is false) and relies on accidental correctness in other shapes.
            return switch (primitive.getPrimitiveTypeName()) {
                case FLOAT -> {
                    float f = ordered.getFloat();
                    yield Float.isNaN(f) ? null : (T) Double.valueOf(f);
                }
                case DOUBLE -> {
                    double d = ordered.getDouble();
                    yield Double.isNaN(d) ? null : (T) Double.valueOf(d);
                }
                // INT32/INT64/FIXED_LEN_BYTE_ARRAY/BINARY mapped to ESQL DOUBLE (DECIMAL, Float16):
                // the raw bytes are not a comparable Double, so suppress page-level pruning.
                case INT32, INT64, FIXED_LEN_BYTE_ARRAY, BINARY, INT96, BOOLEAN -> null;
            };
        } else if (type == Boolean.class) {
            return (T) Boolean.valueOf(ordered.get() != 0);
        } else if (type == Binary.class) {
            byte[] bytes = UninitializedArrays.newByteArray(buf.remaining());
            buf.duplicate().get(bytes);
            return (T) Binary.fromConstantByteArray(bytes);
        }
        return null;
    }

}
