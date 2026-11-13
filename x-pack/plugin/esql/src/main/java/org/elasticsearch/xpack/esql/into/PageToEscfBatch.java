/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfColumnBuilder;
import org.elasticsearch.escf.EscfColumnBuilder.CollisionPolicy;
import org.elasticsearch.escf.EscfColumnData;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.sourcebatch.SourceSchema;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;

/**
 * Converts an ES|QL {@link Page} into an {@link EscfBatch} for use with the pre-built batch indexing path.
 * All-null columns (and NULL / UNSUPPORTED types) are omitted from the schema so they do not create
 * spurious unmapped fields on the destination index.
 */
final class PageToEscfBatch {

    private static final DateFormatter UTC_DATE_FORMATTER = EsqlDataTypeConverter.DEFAULT_DATE_TIME_FORMATTER.withZone(ZoneOffset.UTC);
    private static final DateFormatter UTC_DATE_NANOS_FORMATTER = EsqlDataTypeConverter.DEFAULT_DATE_NANOS_FORMATTER.withZone(
        ZoneOffset.UTC
    );

    private PageToEscfBatch() {}

    /**
     * Converts the given {@link Page} to an {@link EscfBatch}.
     * The returned batch owns its column data; call {@link EscfBatch#close()} when done.
     */
    static EscfBatch convert(Page page, List<String> columnNames, List<DataType> columnTypes) {
        int docCount = page.getPositionCount();
        SourceSchema schema = new SourceSchema();

        int capacity = columnNames.size();
        EscfColumnData[] builtColumns = new EscfColumnData[capacity];
        int builtCount = 0;
        EscfColumnBuilder currentBuilder = null;

        try {
            for (int c = 0; c < capacity; c++) {
                DataType type = columnTypes.get(c);
                if (type == DataType.NULL || type == DataType.UNSUPPORTED) {
                    continue;
                }
                Block block = page.getBlock(c);
                if (block.areAllValuesNull()) {
                    continue;
                }

                schema.appendLeaf(columnNames.get(c), 0);
                currentBuilder = new EscfColumnBuilder(CollisionPolicy.SPLIT);
                writeColumn(currentBuilder, block, type, docCount);
                builtColumns[builtCount++] = currentBuilder.finish(docCount);
                currentBuilder = null;
            }
        } catch (Exception e) {
            if (currentBuilder != null) {
                currentBuilder.close();
            }
            for (int i = 0; i < builtCount; i++) {
                builtColumns[i].close();
            }
            throw e;
        }

        EscfColumnData[] finalColumns = Arrays.copyOf(builtColumns, builtCount);
        return EscfBatch.of(schema, docCount, finalColumns, () -> {
            for (EscfColumnData cd : finalColumns) {
                cd.close();
            }
        });
    }

    private static void writeColumn(EscfColumnBuilder builder, Block block, DataType type, int docCount) {
        switch (type) {
            case LONG, COUNTER_LONG -> writeLongColumn(builder, (LongBlock) block, docCount);
            case INTEGER, COUNTER_INTEGER -> writeIntegerColumn(builder, (IntBlock) block, docCount);
            case DOUBLE, COUNTER_DOUBLE -> writeDoubleColumn(builder, (DoubleBlock) block, docCount);
            case DENSE_VECTOR -> writeDenseVectorColumn(builder, (FloatBlock) block, docCount);
            case BOOLEAN -> writeBoolColumn(builder, (BooleanBlock) block, docCount);
            case KEYWORD, TEXT, FLATTENED, SOURCE -> writeBytesRefAsStringColumn(builder, (BytesRefBlock) block, docCount);
            case IP -> writeIpColumn(builder, (BytesRefBlock) block, docCount);
            case VERSION -> writeVersionColumn(builder, (BytesRefBlock) block, docCount);
            case DATETIME -> writeDatetimeColumn(builder, (LongBlock) block, docCount);
            case DATE_NANOS -> writeDateNanosColumn(builder, (LongBlock) block, docCount);
            case GEO_POINT, GEO_SHAPE, CARTESIAN_POINT, CARTESIAN_SHAPE -> writeSpatialColumn(builder, (BytesRefBlock) block, docCount);
            case GEOHEX -> writeGeoGridColumn(builder, (LongBlock) block, docCount, DataType.GEOHEX);
            case GEOHASH -> writeGeoGridColumn(builder, (LongBlock) block, docCount, DataType.GEOHASH);
            case GEOTILE -> writeGeoGridColumn(builder, (LongBlock) block, docCount, DataType.GEOTILE);
            case UNSIGNED_LONG -> writeUnsignedLongColumn(builder, (LongBlock) block, docCount);
            case TSID_DATA_TYPE -> writeTsidColumn(builder, (BytesRefBlock) block, docCount);
            default -> throw EsqlIllegalArgumentException.illegalDataType(type);
        }
    }

    private static void writeLongColumn(EscfColumnBuilder builder, LongBlock block, int docCount) {
        for (int pos = 0; pos < docCount; pos++) {
            if (block.isNull(pos)) {
                builder.addAbsent();
                continue;
            }
            int firstIdx = block.getFirstValueIndex(pos);
            int count = block.getValueCount(pos);
            if (count == 1) {
                builder.addLong(block.getLong(firstIdx));
            } else {
                builder.beginArray(builder.nextAppendRow());
                for (int i = 0; i < count; i++) {
                    builder.appendLong(block.getLong(firstIdx + i));
                }
                builder.endArray();
            }
        }
    }

    private static void writeIntegerColumn(EscfColumnBuilder builder, IntBlock block, int docCount) {
        for (int pos = 0; pos < docCount; pos++) {
            if (block.isNull(pos)) {
                builder.addAbsent();
                continue;
            }
            int firstIdx = block.getFirstValueIndex(pos);
            int count = block.getValueCount(pos);
            if (count == 1) {
                builder.addLong(block.getInt(firstIdx));
            } else {
                builder.beginArray(builder.nextAppendRow());
                for (int i = 0; i < count; i++) {
                    builder.appendLong(block.getInt(firstIdx + i));
                }
                builder.endArray();
            }
        }
    }

    private static void writeDoubleColumn(EscfColumnBuilder builder, DoubleBlock block, int docCount) {
        for (int pos = 0; pos < docCount; pos++) {
            if (block.isNull(pos)) {
                builder.addAbsent();
                continue;
            }
            int firstIdx = block.getFirstValueIndex(pos);
            int count = block.getValueCount(pos);
            if (count == 1) {
                builder.addDouble(block.getDouble(firstIdx));
            } else {
                builder.beginArray(builder.nextAppendRow());
                for (int i = 0; i < count; i++) {
                    builder.appendDouble(block.getDouble(firstIdx + i));
                }
                builder.endArray();
            }
        }
    }

    private static void writeDenseVectorColumn(EscfColumnBuilder builder, FloatBlock block, int docCount) {
        for (int pos = 0; pos < docCount; pos++) {
            if (block.isNull(pos)) {
                builder.addAbsent();
                continue;
            }
            int firstIdx = block.getFirstValueIndex(pos);
            int count = block.getValueCount(pos);
            if (count == 1) {
                builder.addDouble(block.getFloat(firstIdx));
            } else {
                builder.beginArray(builder.nextAppendRow());
                for (int i = 0; i < count; i++) {
                    builder.appendDouble(block.getFloat(firstIdx + i));
                }
                builder.endArray();
            }
        }
    }

    private static void writeBoolColumn(EscfColumnBuilder builder, BooleanBlock block, int docCount) {
        boolean hasMv = false;
        for (int pos = 0; pos < docCount; pos++) {
            if (block.getValueCount(pos) > 1) {
                hasMv = true;
                break;
            }
        }
        if (hasMv) {
            builder.hintUnion();
        }

        for (int pos = 0; pos < docCount; pos++) {
            if (block.isNull(pos)) {
                builder.addAbsent();
                continue;
            }
            int firstIdx = block.getFirstValueIndex(pos);
            int count = block.getValueCount(pos);
            if (count == 1) {
                builder.addBoolean(block.getBoolean(firstIdx));
            } else {
                byte[] packed = new byte[count];
                for (int i = 0; i < count; i++) {
                    packed[i] = block.getBoolean(firstIdx + i) ? SourceValueType.TRUE : SourceValueType.FALSE;
                }
                builder.addRawUnionRow(SourceValueType.UNION_ARRAY, new BytesRef(packed));
            }
        }
    }

    private static void writeBytesRefAsStringColumn(EscfColumnBuilder builder, BytesRefBlock block, int docCount) {
        BytesRef scratch = new BytesRef();
        for (int pos = 0; pos < docCount; pos++) {
            if (block.isNull(pos)) {
                builder.addAbsent();
                continue;
            }
            int firstIdx = block.getFirstValueIndex(pos);
            int count = block.getValueCount(pos);
            if (count == 1) {
                BytesRef ref = block.getBytesRef(firstIdx, scratch);
                builder.setString(builder.nextAppendRow(), ref);
            } else {
                builder.beginArray(builder.nextAppendRow());
                for (int i = 0; i < count; i++) {
                    builder.appendString(block.getBytesRef(firstIdx + i, scratch));
                }
                builder.endArray();
            }
        }
    }

    private static void writeIpColumn(EscfColumnBuilder builder, BytesRefBlock block, int docCount) {
        BytesRef scratch = new BytesRef();
        for (int pos = 0; pos < docCount; pos++) {
            if (block.isNull(pos)) {
                builder.addAbsent();
                continue;
            }
            int firstIdx = block.getFirstValueIndex(pos);
            int count = block.getValueCount(pos);
            if (count == 1) {
                String s = EsqlDataTypeConverter.ipToString(block.getBytesRef(firstIdx, scratch));
                builder.setString(builder.nextAppendRow(), new BytesRef(s));
            } else {
                builder.beginArray(builder.nextAppendRow());
                for (int i = 0; i < count; i++) {
                    String s = EsqlDataTypeConverter.ipToString(block.getBytesRef(firstIdx + i, scratch));
                    builder.appendString(new BytesRef(s));
                }
                builder.endArray();
            }
        }
    }

    private static void writeVersionColumn(EscfColumnBuilder builder, BytesRefBlock block, int docCount) {
        BytesRef scratch = new BytesRef();
        for (int pos = 0; pos < docCount; pos++) {
            if (block.isNull(pos)) {
                builder.addAbsent();
                continue;
            }
            int firstIdx = block.getFirstValueIndex(pos);
            int count = block.getValueCount(pos);
            if (count == 1) {
                String s = EsqlDataTypeConverter.versionToString(block.getBytesRef(firstIdx, scratch));
                builder.setString(builder.nextAppendRow(), new BytesRef(s));
            } else {
                builder.beginArray(builder.nextAppendRow());
                for (int i = 0; i < count; i++) {
                    String s = EsqlDataTypeConverter.versionToString(block.getBytesRef(firstIdx + i, scratch));
                    builder.appendString(new BytesRef(s));
                }
                builder.endArray();
            }
        }
    }

    private static void writeDatetimeColumn(EscfColumnBuilder builder, LongBlock block, int docCount) {
        for (int pos = 0; pos < docCount; pos++) {
            if (block.isNull(pos)) {
                builder.addAbsent();
                continue;
            }
            int firstIdx = block.getFirstValueIndex(pos);
            int count = block.getValueCount(pos);
            if (count == 1) {
                String s = EsqlDataTypeConverter.dateTimeToString(block.getLong(firstIdx), UTC_DATE_FORMATTER);
                builder.setString(builder.nextAppendRow(), new BytesRef(s));
            } else {
                builder.beginArray(builder.nextAppendRow());
                for (int i = 0; i < count; i++) {
                    String s = EsqlDataTypeConverter.dateTimeToString(block.getLong(firstIdx + i), UTC_DATE_FORMATTER);
                    builder.appendString(new BytesRef(s));
                }
                builder.endArray();
            }
        }
    }

    private static void writeDateNanosColumn(EscfColumnBuilder builder, LongBlock block, int docCount) {
        for (int pos = 0; pos < docCount; pos++) {
            if (block.isNull(pos)) {
                builder.addAbsent();
                continue;
            }
            int firstIdx = block.getFirstValueIndex(pos);
            int count = block.getValueCount(pos);
            if (count == 1) {
                String s = EsqlDataTypeConverter.nanoTimeToString(block.getLong(firstIdx), UTC_DATE_NANOS_FORMATTER);
                builder.setString(builder.nextAppendRow(), new BytesRef(s));
            } else {
                builder.beginArray(builder.nextAppendRow());
                for (int i = 0; i < count; i++) {
                    String s = EsqlDataTypeConverter.nanoTimeToString(block.getLong(firstIdx + i), UTC_DATE_NANOS_FORMATTER);
                    builder.appendString(new BytesRef(s));
                }
                builder.endArray();
            }
        }
    }

    private static void writeSpatialColumn(EscfColumnBuilder builder, BytesRefBlock block, int docCount) {
        BytesRef scratch = new BytesRef();
        for (int pos = 0; pos < docCount; pos++) {
            if (block.isNull(pos)) {
                builder.addAbsent();
                continue;
            }
            int firstIdx = block.getFirstValueIndex(pos);
            int count = block.getValueCount(pos);
            if (count == 1) {
                String s = EsqlDataTypeConverter.spatialToString(block.getBytesRef(firstIdx, scratch));
                builder.setString(builder.nextAppendRow(), new BytesRef(s));
            } else {
                builder.beginArray(builder.nextAppendRow());
                for (int i = 0; i < count; i++) {
                    String s = EsqlDataTypeConverter.spatialToString(block.getBytesRef(firstIdx + i, scratch));
                    builder.appendString(new BytesRef(s));
                }
                builder.endArray();
            }
        }
    }

    private static void writeGeoGridColumn(EscfColumnBuilder builder, LongBlock block, int docCount, DataType dataType) {
        for (int pos = 0; pos < docCount; pos++) {
            if (block.isNull(pos)) {
                builder.addAbsent();
                continue;
            }
            int firstIdx = block.getFirstValueIndex(pos);
            int count = block.getValueCount(pos);
            if (count == 1) {
                String s = EsqlDataTypeConverter.geoGridToString(block.getLong(firstIdx), dataType);
                builder.setString(builder.nextAppendRow(), new BytesRef(s));
            } else {
                builder.beginArray(builder.nextAppendRow());
                for (int i = 0; i < count; i++) {
                    String s = EsqlDataTypeConverter.geoGridToString(block.getLong(firstIdx + i), dataType);
                    builder.appendString(new BytesRef(s));
                }
                builder.endArray();
            }
        }
    }

    private static void writeUnsignedLongColumn(EscfColumnBuilder builder, LongBlock block, int docCount) {
        for (int pos = 0; pos < docCount; pos++) {
            if (block.isNull(pos)) {
                builder.addAbsent();
                continue;
            }
            int firstIdx = block.getFirstValueIndex(pos);
            int count = block.getValueCount(pos);
            if (count == 1) {
                String s = NumericUtils.unsignedLongAsNumber(block.getLong(firstIdx)).toString();
                builder.setString(builder.nextAppendRow(), new BytesRef(s));
            } else {
                builder.beginArray(builder.nextAppendRow());
                for (int i = 0; i < count; i++) {
                    String s = NumericUtils.unsignedLongAsNumber(block.getLong(firstIdx + i)).toString();
                    builder.appendString(new BytesRef(s));
                }
                builder.endArray();
            }
        }
    }

    private static void writeTsidColumn(EscfColumnBuilder builder, BytesRefBlock block, int docCount) {
        BytesRef scratch = new BytesRef();
        for (int pos = 0; pos < docCount; pos++) {
            if (block.isNull(pos)) {
                builder.addAbsent();
                continue;
            }
            int firstIdx = block.getFirstValueIndex(pos);
            int count = block.getValueCount(pos);
            if (count == 1) {
                String s = TimeSeriesIdFieldMapper.encodeTsid(block.getBytesRef(firstIdx, scratch)).toString();
                builder.setString(builder.nextAppendRow(), new BytesRef(s));
            } else {
                builder.beginArray(builder.nextAppendRow());
                for (int i = 0; i < count; i++) {
                    String s = TimeSeriesIdFieldMapper.encodeTsid(block.getBytesRef(firstIdx + i, scratch)).toString();
                    builder.appendString(new BytesRef(s));
                }
                builder.endArray();
            }
        }
    }
}
