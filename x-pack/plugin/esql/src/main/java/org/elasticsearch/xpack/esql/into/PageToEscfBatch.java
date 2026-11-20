/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
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
import org.elasticsearch.escf.EscfBatchBuilder;
import org.elasticsearch.escf.EscfRowBuffer;
import org.elasticsearch.sourcebatch.SourceBatchEncodeHelper;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentString;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.action.ResponseValueUtils;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.util.List;

/**
 * Appends an ES|QL {@link Page} into an {@link EscfBatchBuilder}, one row at a time.
 */
final class PageToEscfBatch {
    private PageToEscfBatch() { /* static class */ }

    /** Appends the page to partition 0 of {@code batch}, one row at a time. */
    static void append(EscfBatchBuilder batch, Page page, List<String> names, List<DataType> types) {
        boolean[] absent = new boolean[names.size()];
        for (int c = 0; c < names.size(); c++) {
            absent[c] = page.getBlock(c).areAllValuesNull();
        }
        BytesRef scratch = new BytesRef();
        for (int row = 0; row < page.getPositionCount(); row++) {
            EscfRowBuffer buffer = batch.beginRow();
            for (int c = 0; c < names.size(); c++) {
                if (absent[c]) {
                    continue;
                }
                Block block = page.getBlock(c);
                if (block.isNull(row)) {
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
        } else {
            writeArray(row, name, type, block, start, count, scratch);
        }
    }

    private static void writeScalar(EscfRowBuffer row, String name, DataType type, Block block, int index, BytesRef scratch) {
        switch (toKind(type)) {
            case LONG -> row.longField(name, ((LongBlock) block).getLong(index));
            case INT -> row.longField(name, ((IntBlock) block).getInt(index));
            case DOUBLE -> row.doubleField(name, ((DoubleBlock) block).getDouble(index));
            case DENSE_VECTOR -> row.doubleField(name, Double.parseDouble(Float.toString(((FloatBlock) block).getFloat(index))));
            case BOOLEAN -> row.booleanField(name, ((BooleanBlock) block).getBoolean(index));
            case UNSIGNED_LONG -> row.longField(name, unsignedLong(block, index));
            case KEYWORD -> row.stringField(name, copyUtf8(((BytesRefBlock) block).getBytesRef(index, scratch)));
            case SOURCE -> {
                byte[] packed = keyValue(((BytesRefBlock) block).getBytesRef(index, scratch));
                if (packed == null) {
                    throw new EsqlIllegalArgumentException("INTO cannot write a SOURCE value that contains a big integer or big decimal");
                }
                row.keyValueField(name, packed);
            }
            case STRINGIFIED -> {
                String text = ResponseValueUtils.valueExtractorFor(type, ZoneOffset.UTC).extract(block, index, scratch).toString();
                row.stringField(name, new XContentString.UTF8Bytes(text.getBytes(StandardCharsets.UTF_8)));
            }
            default -> throw EsqlIllegalArgumentException.illegalDataType(type);
        }
    }

    private static void writeArray(EscfRowBuffer row, String name, DataType type, Block block, int start, int count, BytesRef scratch) {
        long[] numeric = new long[count];
        Object[] vars = new Object[count];
        boolean mixed = false;
        Byte elemType = switch (toKind(type)) {
            case LONG -> {
                LongBlock longs = (LongBlock) block;
                for (int i = 0; i < count; i++) {
                    numeric[i] = longs.getLong(start + i);
                }
                yield SourceValueType.LONG;
            }
            case INT -> {
                IntBlock ints = (IntBlock) block;
                for (int i = 0; i < count; i++) {
                    numeric[i] = ints.getInt(start + i);
                }
                yield SourceValueType.LONG;
            }
            case DOUBLE -> {
                DoubleBlock doubles = (DoubleBlock) block;
                for (int i = 0; i < count; i++) {
                    numeric[i] = Double.doubleToRawLongBits(doubles.getDouble(start + i));
                }
                yield SourceValueType.DOUBLE;
            }
            case BOOLEAN -> {
                BooleanBlock bools = (BooleanBlock) block;
                byte[] types = new byte[count];
                for (int i = 0; i < count; i++) {
                    types[i] = bools.getBoolean(start + i) ? SourceValueType.TRUE : SourceValueType.FALSE;
                    mixed |= types[i] != types[0];
                }
                var packed = SourceBatchEncodeHelper.packAccumulatedElements(types, numeric, vars, count, mixed);
                row.arrayField(name, packed.arrayType(), packed.packed());
                yield null;
            }
            case UNSIGNED_LONG -> {
                LongBlock longs = (LongBlock) block;
                for (int i = 0; i < count; i++) {
                    numeric[i] = unsignedLong(longs, start + i);
                }
                yield SourceValueType.LONG;
            }
            case DENSE_VECTOR -> {
                FloatBlock floats = (FloatBlock) block;
                for (int i = 0; i < count; i++) {
                    numeric[i] = Double.doubleToRawLongBits(Double.parseDouble(Float.toString(floats.getFloat(start + i))));
                }
                yield SourceValueType.DOUBLE;
            }
            case KEYWORD -> {
                BytesRefBlock refs = (BytesRefBlock) block;
                for (int i = 0; i < count; i++) {
                    vars[i] = copyUtf8(refs.getBytesRef(start + i, scratch));
                }
                yield SourceValueType.STRING;
            }
            case SOURCE -> throw new EsqlIllegalArgumentException("INTO cannot write a multi-value SOURCE column");
            case STRINGIFIED -> {
                ResponseValueUtils.BlockValueExtractor extractor = ResponseValueUtils.valueExtractorFor(type, ZoneOffset.UTC);
                for (int i = 0; i < count; i++) {
                    vars[i] = new XContentString.UTF8Bytes(
                        extractor.extract(block, start + i, scratch).toString().getBytes(StandardCharsets.UTF_8)
                    );
                }
                yield SourceValueType.STRING;
            }
        };
        if (elemType != null) {
            row.arrayField(name, SourceValueType.FIXED_ARRAY, SourceBatchEncodeHelper.packFixedArray(elemType, numeric, vars, count));
        }
    }

    private enum Kind {
        LONG,
        INT,
        DOUBLE,
        DENSE_VECTOR,
        BOOLEAN,
        UNSIGNED_LONG,
        KEYWORD,
        SOURCE,
        STRINGIFIED
    }

    private static Kind toKind(DataType type) {
        return switch (type) {
            case LONG, COUNTER_LONG -> Kind.LONG;
            case INTEGER, COUNTER_INTEGER -> Kind.INT;
            case DOUBLE, COUNTER_DOUBLE -> Kind.DOUBLE;
            case DENSE_VECTOR -> Kind.DENSE_VECTOR;
            case BOOLEAN -> Kind.BOOLEAN;
            case UNSIGNED_LONG -> Kind.UNSIGNED_LONG;
            case KEYWORD, TEXT, FLATTENED -> Kind.KEYWORD;
            case SOURCE -> Kind.SOURCE;
            case IP, VERSION, DATETIME, DATE_NANOS, GEO_POINT, GEO_SHAPE, CARTESIAN_POINT, CARTESIAN_SHAPE, GEOHASH, GEOTILE, GEOHEX,
                AGGREGATE_METRIC_DOUBLE, EXPONENTIAL_HISTOGRAM, DATE_RANGE, DOUBLE_RANGE, HISTOGRAM, TSID_DATA_TYPE -> Kind.STRINGIFIED;
            default -> throw EsqlIllegalArgumentException.illegalDataType(type);
        };
    }

    private static XContentString.UTF8Bytes copyUtf8(BytesRef ref) {
        byte[] copy = new byte[ref.length];
        System.arraycopy(ref.bytes, ref.offset, copy, 0, ref.length);
        return new XContentString.UTF8Bytes(copy);
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

    @Nullable
    private static byte[] keyValue(BytesRef json) {
        try (
            XContentParser parser = XContentHelper.createParser(
                XContentParserConfiguration.EMPTY,
                new BytesArray(json.bytes, json.offset, json.length),
                XContentType.JSON
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

}
