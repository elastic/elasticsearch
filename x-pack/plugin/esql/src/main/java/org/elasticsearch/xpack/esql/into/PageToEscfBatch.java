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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.common.xcontent.XContentHelper;
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
import org.elasticsearch.escf.EscfBatchBuilder;
import org.elasticsearch.escf.EscfColumnBuilder;
import org.elasticsearch.escf.EscfColumnBuilder.CollisionPolicy;
import org.elasticsearch.escf.EscfColumnData;
import org.elasticsearch.escf.EscfColumnKind;
import org.elasticsearch.escf.EscfRowBuffer;
import org.elasticsearch.sourcebatch.SourceBatchEncodeHelper;
import org.elasticsearch.sourcebatch.SourceSchema;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentString;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.action.ResponseValueUtils;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntFunction;
import java.util.function.IntToLongFunction;

/**
 * Converts an ES|QL {@link Page} into an {@link EscfBatch} column by column. All-null columns are
 * omitted. When a {@link CircuitBreaker} is supplied, bytes are reserved as each column's buffer
 * grows; on any failure the already-charged bytes are released and the already-built columns are closed.
 */
final class PageToEscfBatch {
    private static final String BREAKER_LABEL = "into";

    private PageToEscfBatch() { /* static class */ }

    record ConvertResult(EscfBatch batch, long charged) {}

    /**
     * Converts the page. When {@code breaker} is non-null, buffer growth is charged to it as each
     * column is written; the returned {@link ConvertResult#charged()} is exactly what the caller must
     * release. The caller owns the returned batch.
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
        Charge charge = new Charge(breaker);
        try {
            for (int c = 0; c < columnNames.size(); c++) {
                Block block = page.getBlock(c);
                if (block.areAllValuesNull() == false) {
                    schema.appendLeaf(columnNames.get(c), 0);
                    columns.add(column(block, columnTypes.get(c), recycler, charge));
                }
            }
        } catch (Exception e) {
            charge.release();
            Releasables.close(columns);
            throw e;
        }
        // TODO: append these columns into EscfBatchBuilder instead of building EscfColumnData here,
        // so EscfBulkWriter can merge pages up to the target bulk size. The column-append API is not in the tree yet.
        return new ConvertResult(
            EscfBatch.of(schema, page.getPositionCount(), columns.toArray(EscfColumnData[]::new), Releasables.wrap(columns)),
            charge.charged
        );
    }

    /** Appends the page to partition 0 of {@code batch}, one row at a time. */
    static void append(EscfBatchBuilder batch, Page page, List<String> names, List<DataType> types) {
        BytesRef scratch = new BytesRef();
        for (int row = 0; row < page.getPositionCount(); row++) {
            EscfRowBuffer buffer = batch.beginRow();
            for (int c = 0; c < names.size(); c++) {
                Block block = page.getBlock(c);
                if (block.areAllValuesNull() || block.isNull(row)) {
                    continue;
                }
                writeRowValue(buffer, names.get(c), types.get(c), block, row, scratch);
            }
            buffer.finishRow();
            batch.commit(0);
        }
    }

    private static void writeRowValue(EscfRowBuffer row, String name, DataType type, Block block, int position, BytesRef scratch) {
        int count = block.getValueCount(position);
        int start = block.getFirstValueIndex(position);
        if (count == 1) {
            writeScalar(row, name, type, block, start, scratch);
            return;
        }
        writeArray(row, name, type, block, start, count, scratch);
    }

    private static void writeScalar(EscfRowBuffer row, String name, DataType type, Block block, int index, BytesRef scratch) {
        switch (type) {
            case LONG, COUNTER_LONG -> row.longField(name, ((LongBlock) block).getLong(index));
            case INTEGER, COUNTER_INTEGER -> row.longField(name, ((IntBlock) block).getInt(index));
            case DOUBLE, COUNTER_DOUBLE -> row.doubleField(name, ((DoubleBlock) block).getDouble(index));
            case DENSE_VECTOR -> row.doubleField(name, Double.parseDouble(Float.toString(((FloatBlock) block).getFloat(index))));
            case BOOLEAN -> row.booleanField(name, ((BooleanBlock) block).getBoolean(index));
            case UNSIGNED_LONG -> row.longField(name, unsignedLong(block, index));
            case KEYWORD, TEXT, FLATTENED -> {
                BytesRef ref = ((BytesRefBlock) block).getBytesRef(index, scratch);
                row.stringField(name, ref.bytes, ref.offset, ref.length);
            }
            case SOURCE -> {
                byte[] packed = keyValue(((BytesRefBlock) block).getBytesRef(index, scratch));
                if (packed == null) {
                    throw new EsqlIllegalArgumentException("INTO cannot write a SOURCE value that contains a big integer or big decimal");
                }
                row.keyValueField(name, packed);
            }
            case IP, VERSION, DATETIME, DATE_NANOS, GEO_POINT, GEO_SHAPE, CARTESIAN_POINT, CARTESIAN_SHAPE, GEOHASH, GEOTILE, GEOHEX,
                AGGREGATE_METRIC_DOUBLE, EXPONENTIAL_HISTOGRAM, DATE_RANGE, DOUBLE_RANGE, HISTOGRAM, TSID_DATA_TYPE -> {
                String text = ResponseValueUtils.valueExtractorFor(type, ZoneOffset.UTC).extract(block, index, scratch).toString();
                row.stringField(name, new XContentString.UTF8Bytes(text.getBytes(StandardCharsets.UTF_8)));
            }
            default -> throw EsqlIllegalArgumentException.illegalDataType(type);
        }
    }

    private static void writeArray(EscfRowBuffer row, String name, DataType type, Block block, int start, int count, BytesRef scratch) {
        long[] numeric = new long[count];
        Object[] vars = new Object[count];
        byte elemType;
        boolean mixed = false;
        switch (type) {
            case LONG, COUNTER_LONG, INTEGER, COUNTER_INTEGER -> {
                elemType = SourceValueType.LONG;
                LongBlock longs = block instanceof LongBlock lb ? lb : null;
                IntBlock ints = block instanceof IntBlock ib ? ib : null;
                for (int i = 0; i < count; i++) {
                    numeric[i] = longs != null ? longs.getLong(start + i) : ints.getInt(start + i);
                }
            }
            case DOUBLE, COUNTER_DOUBLE -> {
                elemType = SourceValueType.DOUBLE;
                DoubleBlock doubles = (DoubleBlock) block;
                for (int i = 0; i < count; i++) {
                    numeric[i] = Double.doubleToRawLongBits(doubles.getDouble(start + i));
                }
            }
            case BOOLEAN -> {
                BooleanBlock bools = (BooleanBlock) block;
                byte[] types = new byte[count];
                for (int i = 0; i < count; i++) {
                    types[i] = bools.getBoolean(start + i) ? SourceValueType.TRUE : SourceValueType.FALSE;
                    mixed |= types[i] != types[0];
                }
                SourceBatchEncodeHelper.PackedArray packed = SourceBatchEncodeHelper.packAccumulatedElements(
                    types,
                    numeric,
                    vars,
                    count,
                    mixed
                );
                row.arrayField(name, packed.arrayType(), packed.packed());
                return;
            }
            case UNSIGNED_LONG -> {
                elemType = SourceValueType.LONG;
                LongBlock longs = (LongBlock) block;
                for (int i = 0; i < count; i++) {
                    numeric[i] = unsignedLong(longs, start + i);
                }
            }
            case KEYWORD, TEXT, FLATTENED -> {
                elemType = SourceValueType.STRING;
                BytesRefBlock refs = (BytesRefBlock) block;
                for (int i = 0; i < count; i++) {
                    BytesRef ref = refs.getBytesRef(start + i, scratch);
                    vars[i] = new XContentString.UTF8Bytes(ref.bytes, ref.offset, ref.length);
                }
            }
            case SOURCE -> throw new EsqlIllegalArgumentException("INTO cannot write a multi-value SOURCE column");
            default -> {
                elemType = SourceValueType.STRING;
                ResponseValueUtils.BlockValueExtractor extractor = ResponseValueUtils.valueExtractorFor(type, ZoneOffset.UTC);
                for (int i = 0; i < count; i++) {
                    String text = extractor.extract(block, start + i, scratch).toString();
                    vars[i] = new XContentString.UTF8Bytes(text.getBytes(StandardCharsets.UTF_8));
                }
            }
        }
        row.arrayField(name, SourceValueType.FIXED_ARRAY, SourceBatchEncodeHelper.packFixedArray(elemType, numeric, vars, count));
    }

    private static long unsignedLong(Block block, int index) {
        return unsignedLong((LongBlock) block, index);
    }

    private static long unsignedLong(LongBlock block, int index) {
        long encoded = block.getLong(index);
        if (encoded >= 0) {
            throw new EsqlIllegalArgumentException("INTO cannot write unsigned_long values above Long.MAX_VALUE");
        }
        return NumericUtils.unsignedLongAsLongExact(encoded);
    }

    private static EscfColumnData column(Block block, DataType type, Recycler<BytesRef> recycler, Charge charge) {
        return switch (type) {
            case LONG, COUNTER_LONG -> fixed64Column(EscfColumnKind.LONG, block, ((LongBlock) block)::getLong, recycler, charge);
            case INTEGER, COUNTER_INTEGER -> {
                IntBlock ints = (IntBlock) block;
                yield fixed64Column(EscfColumnKind.LONG, block, ints::getInt, recycler, charge);
            }
            case DOUBLE, COUNTER_DOUBLE -> {
                DoubleBlock doubles = (DoubleBlock) block;
                yield fixed64Column(EscfColumnKind.DOUBLE, block, i -> Double.doubleToRawLongBits(doubles.getDouble(i)), recycler, charge);
            }
            case DENSE_VECTOR -> {
                // Use Float.toString to preserve shortest decimal (e.g., 1.1 not 1.100000023841858).
                FloatBlock floats = (FloatBlock) block;
                yield fixed64Column(
                    EscfColumnKind.DOUBLE,
                    block,
                    i -> Double.doubleToRawLongBits(Double.parseDouble(Float.toString(floats.getFloat(i)))),
                    recycler,
                    charge
                );
            }
            case BOOLEAN -> boolColumn((BooleanBlock) block, recycler, charge);
            case UNSIGNED_LONG -> unsignedLongColumn((LongBlock) block, recycler, charge);
            case KEYWORD, TEXT, FLATTENED -> {
                BytesRefBlock bref = (BytesRefBlock) block;
                BytesRef scratch = new BytesRef();
                yield stringColumn(block, i -> bref.getBytesRef(i, scratch), recycler, charge);
            }
            case IP, VERSION, DATETIME, DATE_NANOS, GEO_POINT, GEO_SHAPE, CARTESIAN_POINT, CARTESIAN_SHAPE, GEOHASH, GEOTILE, GEOHEX,
                AGGREGATE_METRIC_DOUBLE, EXPONENTIAL_HISTOGRAM, DATE_RANGE, DOUBLE_RANGE, HISTOGRAM, TSID_DATA_TYPE -> {
                ResponseValueUtils.BlockValueExtractor extractor = ResponseValueUtils.valueExtractorFor(type, ZoneOffset.UTC);
                BytesRef scratch = new BytesRef();
                yield stringColumn(block, i -> new BytesRef(extractor.extract(block, i, scratch).toString()), recycler, charge);
            }
            case SOURCE -> sourceColumn((BytesRefBlock) block, recycler, charge);
            default -> throw EsqlIllegalArgumentException.illegalDataType(type);
        };
    }

    private static EscfColumnData fixed64Column(
        byte kind,
        Block block,
        IntToLongFunction valueAt,
        Recycler<BytesRef> recycler,
        Charge charge
    ) {
        if (mixedCardinality(block)) {
            return mixedFixed64(kind, block, valueAt, recycler, charge);
        }
        return block.doesHaveMultivaluedFields()
            ? arrayFixed64Column(kind, block, valueAt, recycler, charge)
            : scalarFixed64Column(kind, block, valueAt, recycler, charge);
    }

    private static EscfColumnData scalarFixed64Column(
        byte kind,
        Block block,
        IntToLongFunction valueAt,
        Recycler<BytesRef> recycler,
        Charge charge
    ) {
        int docCount = block.getPositionCount();
        FixedBitSet validity = buildValidity(block);
        charge.add(recycler.pageSize());
        charge.add(bitsetBytes(validity));
        try (RecyclerBytesStreamOutput out = new RecyclerBytesStreamOutput(recycler)) {
            for (int p = 0; p < docCount; p++) {
                charge.add(Long.BYTES);
                writeLongLE(out, block.isNull(p) ? 0L : valueAt.applyAsLong(block.getFirstValueIndex(p)));
            }
            return EscfColumnBuilder.buildFixed64Column(kind, docCount, validity, out.moveToBytesReference());
        }
    }

    private static EscfColumnData arrayFixed64Column(
        byte kind,
        Block block,
        IntToLongFunction valueAt,
        Recycler<BytesRef> recycler,
        Charge charge
    ) {
        int docCount = block.getPositionCount();
        FixedBitSet validity = buildValidity(block);
        int[] rowOffsets = arrayRowOffsets(block);
        charge.add(recycler.pageSize());
        charge.add(bitsetBytes(validity));
        charge.add((long) rowOffsets.length * Integer.BYTES);
        try (RecyclerBytesStreamOutput childOut = new RecyclerBytesStreamOutput(recycler)) {
            for (int p = 0; p < docCount; p++) {
                if (block.isNull(p) == false) {
                    int first = block.getFirstValueIndex(p);
                    for (int i = first, end = first + block.getValueCount(p); i < end; i++) {
                        charge.add(Long.BYTES);
                        writeLongLE(childOut, valueAt.applyAsLong(i));
                    }
                }
            }
            int childCount = rowOffsets[docCount];
            EscfColumnData child = EscfColumnBuilder.buildFixed64Column(kind, childCount, null, childOut.moveToBytesReference());
            return EscfColumnBuilder.buildArrayColumn(docCount, validity, rowOffsets, child);
        }
    }

    private static EscfColumnData mixedFixed64(
        byte kind,
        Block block,
        IntToLongFunction valueAt,
        Recycler<BytesRef> recycler,
        Charge charge
    ) {
        boolean asDouble = kind == EscfColumnKind.DOUBLE;
        try (EscfColumnBuilder builder = new EscfColumnBuilder(CollisionPolicy.SPLIT, recycler)) {
            long seen = 0;
            int docCount = block.getPositionCount();
            for (int p = 0; p < docCount; p++) {
                if (block.isNull(p)) {
                    builder.addAbsent();
                } else {
                    int first = block.getFirstValueIndex(p);
                    int count = block.getValueCount(p);
                    if (count == 1) {
                        long bits = valueAt.applyAsLong(first);
                        if (asDouble) {
                            builder.addDouble(Double.longBitsToDouble(bits));
                        } else {
                            builder.addLong(bits);
                        }
                    } else {
                        builder.beginArray(builder.nextAppendRow());
                        for (int i = first, end = first + count; i < end; i++) {
                            long bits = valueAt.applyAsLong(i);
                            if (asDouble) {
                                builder.appendDouble(Double.longBitsToDouble(bits));
                            } else {
                                builder.appendLong(bits);
                            }
                        }
                        builder.endArray();
                    }
                }
                seen = charge.grow(builder, seen);
            }
            return builder.finish(docCount);
        }
    }

    private static EscfColumnData stringColumn(Block block, IntFunction<BytesRef> valueAt, Recycler<BytesRef> recycler, Charge charge) {
        if (mixedCardinality(block)) {
            return mixedString(block, valueAt, recycler, charge);
        }
        return block.doesHaveMultivaluedFields()
            ? arrayStringColumn(block, valueAt, recycler, charge)
            : scalarStringColumn(block, valueAt, recycler, charge);
    }

    private static EscfColumnData scalarStringColumn(
        Block block,
        IntFunction<BytesRef> valueAt,
        Recycler<BytesRef> recycler,
        Charge charge
    ) {
        int docCount = block.getPositionCount();
        FixedBitSet validity = buildValidity(block);
        int[] offsets = new int[docCount + 1];
        charge.add(recycler.pageSize());
        charge.add(bitsetBytes(validity));
        charge.add((long) offsets.length * Integer.BYTES);
        try (RecyclerBytesStreamOutput out = new RecyclerBytesStreamOutput(recycler)) {
            for (int p = 0; p < docCount; p++) {
                offsets[p] = (int) out.position();
                if (block.isNull(p) == false) {
                    BytesRef ref = valueAt.apply(block.getFirstValueIndex(p));
                    charge.add(ref.length);
                    writeBytes(out, ref);
                }
            }
            offsets[docCount] = (int) out.position();
            return EscfColumnBuilder.buildStringColumn(docCount, validity, offsets, out.moveToBytesReference());
        }
    }

    private static EscfColumnData arrayStringColumn(
        Block block,
        IntFunction<BytesRef> valueAt,
        Recycler<BytesRef> recycler,
        Charge charge
    ) {
        int docCount = block.getPositionCount();
        FixedBitSet validity = buildValidity(block);
        int[] rowOffsets = arrayRowOffsets(block);
        int childCount = rowOffsets[docCount];
        int[] childOffsets = new int[childCount + 1];
        charge.add(recycler.pageSize());
        charge.add(bitsetBytes(validity));
        charge.add((long) rowOffsets.length * Integer.BYTES);
        charge.add((long) childOffsets.length * Integer.BYTES);
        try (RecyclerBytesStreamOutput childOut = new RecyclerBytesStreamOutput(recycler)) {
            int elem = 0;
            for (int p = 0; p < docCount; p++) {
                if (block.isNull(p) == false) {
                    int first = block.getFirstValueIndex(p);
                    for (int i = first, end = first + block.getValueCount(p); i < end; i++) {
                        childOffsets[elem] = (int) childOut.position();
                        BytesRef ref = valueAt.apply(i);
                        charge.add(ref.length);
                        writeBytes(childOut, ref);
                        elem++;
                    }
                }
            }
            childOffsets[childCount] = (int) childOut.position();
            EscfColumnData child = EscfColumnBuilder.buildStringColumn(childCount, null, childOffsets, childOut.moveToBytesReference());
            return EscfColumnBuilder.buildArrayColumn(docCount, validity, rowOffsets, child);
        }
    }

    private static EscfColumnData mixedString(Block block, IntFunction<BytesRef> valueAt, Recycler<BytesRef> recycler, Charge charge) {
        try (EscfColumnBuilder builder = new EscfColumnBuilder(CollisionPolicy.SPLIT, recycler)) {
            long seen = 0;
            int docCount = block.getPositionCount();
            for (int p = 0; p < docCount; p++) {
                if (block.isNull(p)) {
                    builder.addAbsent();
                } else {
                    int first = block.getFirstValueIndex(p);
                    int count = block.getValueCount(p);
                    if (count == 1) {
                        builder.setString(builder.nextAppendRow(), valueAt.apply(first));
                    } else {
                        builder.beginArray(builder.nextAppendRow());
                        for (int i = first, end = first + count; i < end; i++) {
                            builder.appendString(valueAt.apply(i));
                        }
                        builder.endArray();
                    }
                }
                seen = charge.grow(builder, seen);
            }
            return builder.finish(docCount);
        }
    }

    private static EscfColumnData boolColumn(BooleanBlock block, Recycler<BytesRef> recycler, Charge charge) {
        if (block.doesHaveMultivaluedFields()) {
            try (EscfColumnBuilder builder = new EscfColumnBuilder(CollisionPolicy.SPLIT, recycler)) {
                writeBooleans(builder, block, charge);
                return builder.finish(block.getPositionCount());
            }
        }
        int docCount = block.getPositionCount();
        charge.add(recycler.pageSize());
        FixedBitSet validity = buildValidity(block);
        FixedBitSet values = null;
        for (int p = 0; p < docCount; p++) {
            if (block.isNull(p) == false && block.getBoolean(block.getFirstValueIndex(p))) {
                if (values == null) {
                    values = new FixedBitSet(docCount);
                    charge.add(bitsetBytes(values));
                }
                values.set(p);
            }
        }
        charge.add(bitsetBytes(validity));
        return EscfColumnBuilder.buildBoolColumn(docCount, validity, values);
    }

    private static void writeBooleans(EscfColumnBuilder builder, BooleanBlock block, Charge charge) {
        if (block.doesHaveMultivaluedFields()) {
            builder.hintUnion();
        }
        long seen = 0;
        for (int p = 0; p < block.getPositionCount(); p++) {
            if (block.isNull(p)) {
                builder.addAbsent();
            } else {
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
            seen = charge.grow(builder, seen);
        }
    }

    // A value that fits in a signed long stays a long. A larger one is a double whose lexical form is the
    // exact decimal, so JSON emits a number token rather than a string. Rows in one block may mix the two.
    private static EscfColumnData unsignedLongColumn(LongBlock block, Recycler<BytesRef> recycler, Charge charge) {
        try (EscfColumnBuilder builder = new EscfColumnBuilder(CollisionPolicy.SPLIT, recycler)) {
            long seen = 0;
            int docCount = block.getPositionCount();
            for (int p = 0; p < docCount; p++) {
                if (block.isNull(p)) {
                    builder.addAbsent();
                } else {
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
                seen = charge.grow(builder, seen);
            }
            return builder.finish(docCount);
        }
    }

    // TODO: reject until ESCF can store the exact value. Array elements have no decimal spelling, and
    // numberValue() ignores the spelling on a scalar double, so a value above Long.MAX_VALUE would be
    // indexed as a rounded double.
    private static void writeUnsignedLong(EscfColumnBuilder builder, long encoded) {
        if (encoded >= 0) {
            throw new EsqlIllegalArgumentException("INTO cannot write unsigned_long values above Long.MAX_VALUE");
        }
        builder.addLong(NumericUtils.unsignedLongAsLongExact(encoded));
    }

    private static void writeUnsignedLongArray(EscfColumnBuilder builder, long encoded) {
        if (encoded >= 0) {
            throw new EsqlIllegalArgumentException("INTO cannot write unsigned_long values above Long.MAX_VALUE");
        }
        builder.appendLong(NumericUtils.unsignedLongAsLongExact(encoded));
    }

    private static EscfColumnData sourceColumn(BytesRefBlock block, Recycler<BytesRef> recycler, Charge charge) {
        try (EscfColumnBuilder builder = new EscfColumnBuilder(CollisionPolicy.SPLIT, recycler)) {
            builder.hintUnion();
            long seen = 0;
            BytesRef scratch = new BytesRef();
            int docCount = block.getPositionCount();
            for (int p = 0; p < docCount; p++) {
                if (block.isNull(p)) {
                    builder.addAbsent();
                } else {
                    int first = block.getFirstValueIndex(p);
                    int count = block.getValueCount(p);
                    if (count == 1) {
                        byte[] packed = keyValue(block.getBytesRef(first, scratch));
                        if (packed == null) {
                            throw new EsqlIllegalArgumentException(
                                "INTO cannot write a SOURCE value that contains a big integer or big decimal"
                            );
                        }
                        builder.addKeyValue(packed);
                    } else {
                        byte[][] objects = new byte[count][];
                        int bytes = 0;
                        for (int i = 0; i < count; i++) {
                            objects[i] = keyValue(block.getBytesRef(first + i, scratch));
                            if (objects[i] == null) {
                                throw new EsqlIllegalArgumentException(
                                    "INTO cannot write a SOURCE value that contains a big integer or big decimal"
                                );
                            }
                            bytes += 1 + Integer.BYTES + objects[i].length;
                        }
                        byte[] packed = new byte[bytes];
                        int pos = 0;
                        for (byte[] object : objects) {
                            packed[pos++] = SourceValueType.KEY_VALUE;
                            ByteUtils.writeIntLE(object.length, packed, pos);
                            pos += Integer.BYTES;
                            System.arraycopy(object, 0, packed, pos, object.length);
                            pos += object.length;
                        }
                        builder.addRawUnionRow(SourceValueType.UNION_ARRAY, new BytesRef(packed));
                    }
                }
                seen = charge.grow(builder, seen);
            }
            return builder.finish(block.getPositionCount());
        }
    }

    @Nullable
    private static byte[] keyValue(BytesRef json) {
        try (
            XContentParser parser = XContentHelper.createParser(
                XContentParserConfiguration.EMPTY,
                new BytesArray(json.bytes, json.offset, json.length)
            )
        ) {
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("SOURCE value must be a JSON object");
            }
            return SourceBatchEncodeHelper.serializeKeyValue(parser, true);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** True when some present row has one value and another has several. An all-array column can stay an array. */
    private static boolean mixedCardinality(Block block) {
        if (block.doesHaveMultivaluedFields() == false) {
            return false;
        }
        boolean sawOne = false;
        boolean sawMany = false;
        for (int p = 0; p < block.getPositionCount(); p++) {
            if (block.isNull(p)) {
                continue;
            }
            if (block.getValueCount(p) <= 1) {
                sawOne = true;
            } else {
                sawMany = true;
            }
            if (sawOne && sawMany) {
                return true;
            }
        }
        return false;
    }

    private static long bitsetBytes(FixedBitSet bits) {
        return bits == null ? 0 : (long) bits.getBits().length * Long.BYTES;
    }

    private static final class Charge {
        @Nullable
        private final CircuitBreaker breaker;
        private long charged;

        Charge(@Nullable CircuitBreaker breaker) {
            this.breaker = breaker;
        }

        void add(long bytes) {
            if (breaker == null || bytes == 0) {
                return;
            }
            breaker.addEstimateBytesAndMaybeBreak(bytes, BREAKER_LABEL);
            charged += bytes;
        }

        /** Charges the builder's growth since {@code seen} and returns the new total. */
        long grow(EscfColumnBuilder builder, long seen) {
            long used = builder.ramBytesUsed();
            if (used > seen) {
                add(used - seen);
            }
            return used;
        }

        void release() {
            if (breaker != null && charged != 0) {
                breaker.addWithoutBreaking(-charged, BREAKER_LABEL);
                charged = 0;
            }
        }
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
