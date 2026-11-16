/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfColumnBuilder;
import org.elasticsearch.escf.EscfColumnBuilder.CollisionPolicy;
import org.elasticsearch.escf.EscfColumnData;
import org.elasticsearch.escf.EscfColumnKind;
import org.elasticsearch.sourcebatch.SourceSchema;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.action.ResponseValueUtils;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntFunction;
import java.util.function.IntToLongFunction;

/**
 * Converts an ES|QL {@link Page} into an {@link EscfBatch} column by column. All-null columns are
 * omitted. When a {@link CircuitBreaker} is supplied, each column is charged to the breaker right
 * after it is built; on any failure the already-charged bytes are released and the already-built
 * columns are closed.
 */
final class PageToEscfBatch {
    private static final String BREAKER_LABEL = "into";

    private PageToEscfBatch() { /* static class */ }

    record ConvertResult(EscfBatch batch, long charged) {}

    /**
     * Converts the page. When {@code breaker} is non-null, each column is charged to it; the
     * returned {@link ConvertResult#charged()} is exactly what the caller must release. The caller
     * owns the returned batch.
     */
    static ConvertResult convert(
        Page page,
        List<String> columnNames,
        List<DataType> columnTypes,
        Recycler<BytesRef> recycler,
        @Nullable CircuitBreaker breaker
    ) {
        SourceSchema schema = new SourceSchema();
        List<EscfColumnData> columns = new ArrayList<>(columnNames.size());
        long charged = 0;
        try {
            for (int c = 0; c < columnNames.size(); c++) {
                Block block = page.getBlock(c);
                if (block.areAllValuesNull() == false) {
                    schema.appendLeaf(columnNames.get(c), 0);
                    EscfColumnData col = column(block, columnTypes.get(c), recycler);
                    columns.add(col);
                    if (breaker != null) {
                        long colBytes = columnBytes(col, recycler);
                        breaker.addEstimateBytesAndMaybeBreak(colBytes, BREAKER_LABEL);
                        charged += colBytes;
                    }
                }
            }
        } catch (Exception e) {
            if (breaker != null) {
                breaker.addWithoutBreaking(-charged, BREAKER_LABEL);
            }
            Releasables.close(columns);
            throw e;
        }
        return new ConvertResult(
            EscfBatch.of(schema, page.getPositionCount(), columns.toArray(EscfColumnData[]::new), Releasables.wrap(columns)),
            charged
        );
    }

    private static long columnBytes(EscfColumnData col, Recycler<BytesRef> recycler) {
        long n = recycler.pageSize();
        if (col.data() != null) {
            n += col.data().length();
        }
        if (col.offsets() != null) {
            n += (long) col.offsets().length * Integer.BYTES;
        }
        if (col.validity() != null) {
            n += (long) col.validity().getBits().length * Long.BYTES;
        }
        if (col.values() != null) {
            n += (long) col.values().getBits().length * Long.BYTES;
        }
        if (col.typeVector() != null) {
            n += col.typeVector().length;
        }
        if (col.child() != null) {
            n += columnBytes(col.child(), recycler);
        }
        return n;
    }

    private static EscfColumnData column(Block block, DataType type, Recycler<BytesRef> recycler) {
        return switch (type) {
            case LONG, COUNTER_LONG -> fixed64Column(EscfColumnKind.LONG, block, ((LongBlock) block)::getLong, recycler);
            case INTEGER, COUNTER_INTEGER -> {
                IntBlock ints = (IntBlock) block;
                yield fixed64Column(EscfColumnKind.LONG, block, ints::getInt, recycler);
            }
            case DOUBLE, COUNTER_DOUBLE -> {
                DoubleBlock doubles = (DoubleBlock) block;
                yield fixed64Column(EscfColumnKind.DOUBLE, block, i -> Double.doubleToRawLongBits(doubles.getDouble(i)), recycler);
            }
            case DENSE_VECTOR -> {
                // Use Float.toString to preserve shortest decimal (e.g., 1.1 not 1.100000023841858).
                FloatBlock floats = (FloatBlock) block;
                yield fixed64Column(
                    EscfColumnKind.DOUBLE,
                    block,
                    i -> Double.doubleToRawLongBits(Double.parseDouble(Float.toString(floats.getFloat(i)))),
                    recycler
                );
            }
            case BOOLEAN -> boolColumn((BooleanBlock) block, recycler);
            case UNSIGNED_LONG -> unsignedLongColumn((LongBlock) block, recycler);
            case KEYWORD, TEXT, FLATTENED -> {
                BytesRefBlock bref = (BytesRefBlock) block;
                BytesRef scratch = new BytesRef();
                yield stringColumn(block, i -> bref.getBytesRef(i, scratch), recycler);
            }
            case IP, VERSION, DATETIME, DATE_NANOS, GEO_POINT, GEO_SHAPE, CARTESIAN_POINT, CARTESIAN_SHAPE, GEOHASH, GEOTILE, GEOHEX,
                AGGREGATE_METRIC_DOUBLE, EXPONENTIAL_HISTOGRAM, DATE_RANGE, DOUBLE_RANGE, HISTOGRAM, TSID_DATA_TYPE -> {
                ResponseValueUtils.BlockValueExtractor extractor = ResponseValueUtils.valueExtractorFor(type, ZoneOffset.UTC);
                BytesRef scratch = new BytesRef();
                yield stringColumn(block, i -> new BytesRef(extractor.extract(block, i, scratch).toString()), recycler);
            }
            default -> throw EsqlIllegalArgumentException.illegalDataType(type);
        };
    }

    private static EscfColumnData fixed64Column(byte kind, Block block, IntToLongFunction valueAt, Recycler<BytesRef> recycler) {
        return block.doesHaveMultivaluedFields()
            ? arrayFixed64Column(kind, block, valueAt, recycler)
            : scalarFixed64Column(kind, block, valueAt, recycler);
    }

    private static EscfColumnData scalarFixed64Column(byte kind, Block block, IntToLongFunction valueAt, Recycler<BytesRef> recycler) {
        int docCount = block.getPositionCount();
        FixedBitSet validity = buildValidity(block);
        try (RecyclerBytesStreamOutput out = new RecyclerBytesStreamOutput(recycler)) {
            for (int p = 0; p < docCount; p++) {
                writeLongLE(out, block.isNull(p) ? 0L : valueAt.applyAsLong(block.getFirstValueIndex(p)));
            }
            return EscfColumnBuilder.buildFixed64Column(kind, docCount, validity, out.moveToBytesReference());
        }
    }

    private static EscfColumnData arrayFixed64Column(byte kind, Block block, IntToLongFunction valueAt, Recycler<BytesRef> recycler) {
        int docCount = block.getPositionCount();
        FixedBitSet validity = buildValidity(block);
        int[] rowOffsets = arrayRowOffsets(block);
        try (RecyclerBytesStreamOutput childOut = new RecyclerBytesStreamOutput(recycler)) {
            for (int p = 0; p < docCount; p++) {
                if (block.isNull(p) == false) {
                    int first = block.getFirstValueIndex(p);
                    for (int i = first, end = first + block.getValueCount(p); i < end; i++) {
                        writeLongLE(childOut, valueAt.applyAsLong(i));
                    }
                }
            }
            int childCount = rowOffsets[docCount];
            EscfColumnData child = EscfColumnBuilder.buildFixed64Column(kind, childCount, null, childOut.moveToBytesReference());
            return EscfColumnBuilder.buildArrayColumn(docCount, validity, rowOffsets, child);
        }
    }

    private static EscfColumnData stringColumn(Block block, IntFunction<BytesRef> valueAt, Recycler<BytesRef> recycler) {
        return block.doesHaveMultivaluedFields()
            ? arrayStringColumn(block, valueAt, recycler)
            : scalarStringColumn(block, valueAt, recycler);
    }

    private static EscfColumnData scalarStringColumn(Block block, IntFunction<BytesRef> valueAt, Recycler<BytesRef> recycler) {
        int docCount = block.getPositionCount();
        FixedBitSet validity = buildValidity(block);
        int[] offsets = new int[docCount + 1];
        try (RecyclerBytesStreamOutput out = new RecyclerBytesStreamOutput(recycler)) {
            for (int p = 0; p < docCount; p++) {
                offsets[p] = (int) out.position();
                if (block.isNull(p) == false) {
                    writeBytes(out, valueAt.apply(block.getFirstValueIndex(p)));
                }
            }
            offsets[docCount] = (int) out.position();
            return EscfColumnBuilder.buildStringColumn(docCount, validity, offsets, out.moveToBytesReference());
        }
    }

    private static EscfColumnData arrayStringColumn(Block block, IntFunction<BytesRef> valueAt, Recycler<BytesRef> recycler) {
        int docCount = block.getPositionCount();
        FixedBitSet validity = buildValidity(block);
        int[] rowOffsets = arrayRowOffsets(block);
        int childCount = rowOffsets[docCount];
        int[] childOffsets = new int[childCount + 1];
        try (RecyclerBytesStreamOutput childOut = new RecyclerBytesStreamOutput(recycler)) {
            int elem = 0;
            for (int p = 0; p < docCount; p++) {
                if (block.isNull(p) == false) {
                    int first = block.getFirstValueIndex(p);
                    for (int i = first, end = first + block.getValueCount(p); i < end; i++) {
                        childOffsets[elem] = (int) childOut.position();
                        writeBytes(childOut, valueAt.apply(i));
                        elem++;
                    }
                }
            }
            childOffsets[childCount] = (int) childOut.position();
            EscfColumnData child = EscfColumnBuilder.buildStringColumn(childCount, null, childOffsets, childOut.moveToBytesReference());
            return EscfColumnBuilder.buildArrayColumn(docCount, validity, rowOffsets, child);
        }
    }

    private static EscfColumnData boolColumn(BooleanBlock block, Recycler<BytesRef> recycler) {
        if (block.doesHaveMultivaluedFields()) {
            try (EscfColumnBuilder builder = new EscfColumnBuilder(CollisionPolicy.SPLIT, recycler)) {
                writeBooleans(builder, block);
                return builder.finish(block.getPositionCount());
            }
        }
        int docCount = block.getPositionCount();
        FixedBitSet validity = buildValidity(block);
        FixedBitSet values = null;
        for (int p = 0; p < docCount; p++) {
            if (block.isNull(p) == false && block.getBoolean(block.getFirstValueIndex(p))) {
                if (values == null) {
                    values = new FixedBitSet(docCount);
                }
                values.set(p);
            }
        }
        return EscfColumnBuilder.buildBoolColumn(docCount, validity, values);
    }

    private static void writeBooleans(EscfColumnBuilder builder, BooleanBlock block) {
        if (block.doesHaveMultivaluedFields()) {
            builder.hintUnion();
        }
        for (int p = 0; p < block.getPositionCount(); p++) {
            if (block.isNull(p)) {
                builder.addAbsent();
                continue;
            }
            int first = block.getFirstValueIndex(p);
            int count = block.getValueCount(p);
            if (count == 1) {
                builder.addBoolean(block.getBoolean(first));
            } else {
                byte[] packed = new byte[count];
                for (int i = 0; i < count; i++) {
                    packed[i] = block.getBoolean(first + i) ? SourceValueType.TRUE : SourceValueType.FALSE;
                }
                builder.addRawUnionRow(SourceValueType.UNION_ARRAY, new BytesRef(packed));
            }
        }
    }

    // Values above Long.MAX_VALUE are stored as non-negative signed longs; they must be rendered
    // as decimal strings. A column may mix both kinds, so use the instance API for the UNION path.
    private static EscfColumnData unsignedLongColumn(LongBlock block, Recycler<BytesRef> recycler) {
        try (EscfColumnBuilder builder = new EscfColumnBuilder(CollisionPolicy.SPLIT, recycler)) {
            for (int p = 0; p < block.getPositionCount(); p++) {
                if (block.isNull(p)) {
                    builder.addAbsent();
                    continue;
                }
                int first = block.getFirstValueIndex(p);
                int count = block.getValueCount(p);
                if (count == 1) {
                    writeUnsignedLong(builder, block.getLong(first));
                } else {
                    builder.beginArray(builder.nextAppendRow());
                    for (int i = first; i < first + count; i++) {
                        writeUnsignedLongArray(builder, block.getLong(i));
                    }
                    builder.endArray();
                }
            }
            return builder.finish(block.getPositionCount());
        }
    }

    private static void writeUnsignedLong(EscfColumnBuilder builder, long encoded) {
        if (encoded < 0) {
            builder.addLong(NumericUtils.unsignedLongAsLongExact(encoded));
        } else {
            builder.setString(builder.nextAppendRow(), unsignedLongDecimal(encoded));
        }
    }

    private static void writeUnsignedLongArray(EscfColumnBuilder builder, long encoded) {
        if (encoded < 0) {
            builder.appendLong(NumericUtils.unsignedLongAsLongExact(encoded));
        } else {
            builder.appendString(unsignedLongDecimal(encoded));
        }
    }

    private static BytesRef unsignedLongDecimal(long encoded) {
        return new BytesRef(NumericUtils.unsignedLongAsBigInteger(encoded).toString());
    }

    private static FixedBitSet buildValidity(Block block) {
        int docCount = block.getPositionCount();
        FixedBitSet validity = null;
        for (int p = 0; p < docCount; p++) {
            if (block.isNull(p)) {
                if (validity == null) {
                    validity = new FixedBitSet(docCount);
                    if (p > 0) {
                        validity.set(0, p);
                    }
                }
            } else if (validity != null) {
                validity.set(p);
            }
        }
        return validity;
    }

    // rowOffsets[p] is the first child index of row p and rowOffsets[docCount] is the child count,
    // so row p's children are [rowOffsets[p], rowOffsets[p + 1]). Index 0 stays 0: a new int[] is
    // zero-filled, and 0 is the start of row 0. The loop writes each row's exclusive end at p + 1.
    private static int[] arrayRowOffsets(Block block) {
        int docCount = block.getPositionCount();
        int[] rowOffsets = new int[docCount + 1];
        int childCount = 0;
        for (int p = 0; p < docCount; p++) {
            if (block.isNull(p) == false) {
                childCount += block.getValueCount(p);
            }
            rowOffsets[p + 1] = childCount;
        }
        return rowOffsets;
    }

    private static void writeLongLE(RecyclerBytesStreamOutput out, long value) {
        try {
            out.writeLongLE(value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void writeBytes(RecyclerBytesStreamOutput out, BytesRef ref) {
        out.writeBytes(ref.bytes, ref.offset, ref.length);
    }
}
