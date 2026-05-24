/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.arrow;

import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.arrow.BooleanArrowBufBlock;
import org.elasticsearch.compute.data.arrow.BytesRefArrowBufBlock;
import org.elasticsearch.compute.data.arrow.DoubleArrowBufBlock;
import org.elasticsearch.compute.data.arrow.Float16ArrowBufBlock;
import org.elasticsearch.compute.data.arrow.FloatArrowBufBlock;
import org.elasticsearch.compute.data.arrow.Int16ArrowBufBlock;
import org.elasticsearch.compute.data.arrow.Int8ArrowBufBlock;
import org.elasticsearch.compute.data.arrow.IntArrowBufBlock;
import org.elasticsearch.compute.data.arrow.LongArrowBufBlock;
import org.elasticsearch.compute.data.arrow.LongMul1kArrowBufBlock;
import org.elasticsearch.compute.data.arrow.UInt16ArrowBufBlock;
import org.elasticsearch.compute.data.arrow.UInt8ArrowBufBlock;
import org.elasticsearch.xpack.esql.arrow.ArrowToBlockConverter;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.EnumMap;
import java.util.List;

/**
 * Mapping information from Arrow vectors to ES|QL blocks.
 * Provides both schema conversion (Arrow {@link Field} to ESQL {@link DataType}) and
 * data conversion (Arrow {@link FieldVector} to ESQL {@link Block}).
 *
 * @param dataType the logical type, used to convert the Arrow schema to ES|QL attributes
 * @param elementType the primitive type used to represent Arrow vector values
 * @param converter block converter: to create an ES|QL block from an Arrow vector
 */
public record ArrowToEsql(DataType dataType, ElementType elementType, ArrowToBlockConverter converter) {

    private static final EnumMap<Types.MinorType, ArrowToEsql> typeMapping;

    public static ArrowToEsql forType(Types.MinorType minorType) {
        return typeMapping.get(minorType);
    }

    public static ArrowToEsql forField(Field field) {
        var type = Types.getMinorTypeForArrowType(field.getType());

        return switch (type) {
            case LIST -> forField(field.getChildren().get(1));

            // TODO: handle these
            case LISTVIEW -> null;
            case LARGELIST -> null;
            case LARGELISTVIEW -> null;
            case FIXED_SIZE_LIST -> null;

            default -> forType(type);
        };
    }

    /**
     * Returns the ESQL {@link DataType} for an Arrow field, applying wide coercions suitable
     * for schema inference from file formats like Parquet where the full range of Arrow types
     * may appear. Unlike {@link #forField}, this method maps all common Arrow types to an ESQL
     * type (e.g. Decimal → DOUBLE, Binary → KEYWORD, UInt8 → INTEGER).
     */
    public static DataType dataTypeForField(Field field) {
        var type = Types.getMinorTypeForArrowType(field.getType());

        if (type == Types.MinorType.LIST || type == Types.MinorType.LARGELIST) {
            List<Field> children = field.getChildren();
            if (children.isEmpty()) {
                return DataType.UNSUPPORTED;
            }
            return dataTypeForField(children.get(children.size() - 1));
        }

        return wideDataType(type);
    }

    /**
     * Must only advertise types that the runtime converter registry
     * ({@link org.elasticsearch.xpack.esql.arrow.ArrowToBlockConverter#forType}) can actually convert,
     * otherwise schema inference would accept a column that batch conversion rejects at runtime with
     * {@code Unsupported Arrow type}.
     */
    private static DataType wideDataType(Types.MinorType type) {
        return switch (type) {
            case TINYINT, SMALLINT, INT, UINT1, UINT2 -> DataType.INTEGER;
            case BIGINT, UINT4 -> DataType.LONG;
            case FLOAT2, FLOAT4, FLOAT8 -> DataType.DOUBLE;
            case BIT -> DataType.BOOLEAN;
            case VARCHAR, VARBINARY -> DataType.KEYWORD;
            case TIMESTAMPSEC, TIMESTAMPMILLI, TIMESTAMPSECTZ, TIMESTAMPMILLITZ -> DataType.DATETIME;
            case TIMESTAMPMICRO, TIMESTAMPNANO, TIMESTAMPMICROTZ, TIMESTAMPNANOTZ -> DataType.DATE_NANOS;
            default -> DataType.UNSUPPORTED;
        };
    }

    public Block convert(FieldVector vector, BlockFactory blockFactory) {
        return converter.convert(vector, blockFactory);
    }

    static {
        var map = new EnumMap<Types.MinorType, ArrowToEsql>(Types.MinorType.class);
        for (var type : Types.MinorType.values()) {
            map.put(type, forType0(type));
        }
        typeMapping = map;
    }

    private static ArrowToEsql forType0(Types.MinorType minorType) {
        return switch (minorType) {
            case NULL -> null;

            // Signed integers
            case TINYINT -> new ArrowToEsql(DataType.BYTE, ElementType.INT, Int8ArrowBufBlock::of);
            case SMALLINT -> new ArrowToEsql(DataType.SHORT, ElementType.INT, Int16ArrowBufBlock::of);
            case INT -> new ArrowToEsql(DataType.INTEGER, ElementType.INT, IntArrowBufBlock::of);
            case BIGINT -> new ArrowToEsql(DataType.LONG, ElementType.LONG, LongArrowBufBlock::of);

            // Unsigned integers: data type is the immediate larger integer (except for unsigned longs)
            case UINT1 -> new ArrowToEsql(DataType.SHORT, ElementType.INT, UInt8ArrowBufBlock::of);
            case UINT2 -> new ArrowToEsql(DataType.INTEGER, ElementType.INT, UInt16ArrowBufBlock::of);
            case UINT4 -> new ArrowToEsql(DataType.LONG, ElementType.LONG, LongArrowBufBlock::of);
            case UINT8 -> new ArrowToEsql(DataType.UNSIGNED_LONG, ElementType.LONG, LongArrowBufBlock::of);

            // Floating point numbers: data type is always double
            case FLOAT2 -> new ArrowToEsql(DataType.HALF_FLOAT, ElementType.DOUBLE, Float16ArrowBufBlock::of);
            case FLOAT4 -> new ArrowToEsql(DataType.FLOAT, ElementType.DOUBLE, FloatArrowBufBlock::of);
            case FLOAT8 -> new ArrowToEsql(DataType.DOUBLE, ElementType.DOUBLE, DoubleArrowBufBlock::of);

            // Unsupported number types
            case DECIMAL -> null;
            case DECIMAL256 -> null;
            case FIXEDSIZEBINARY -> null;

            case DATEDAY -> null;
            case DATEMILLI -> null;
            case TIMESEC -> null;
            case TIMEMILLI -> null;
            case TIMEMICRO -> null;
            case TIMENANO -> null;

            // Convert to millis or nanos
            case TIMESTAMPSEC -> new ArrowToEsql(DataType.DATETIME, ElementType.LONG, LongMul1kArrowBufBlock::of);
            case TIMESTAMPMILLI -> new ArrowToEsql(DataType.DATETIME, ElementType.LONG, LongArrowBufBlock::of);
            case TIMESTAMPMICRO -> new ArrowToEsql(DataType.DATE_NANOS, ElementType.LONG, LongMul1kArrowBufBlock::of);
            case TIMESTAMPNANO -> new ArrowToEsql(DataType.DATE_NANOS, ElementType.LONG, LongArrowBufBlock::of);

            // Value is in UTC, timezone is dropped
            case TIMESTAMPSECTZ -> new ArrowToEsql(DataType.DATETIME, ElementType.LONG, LongMul1kArrowBufBlock::of);
            case TIMESTAMPMILLITZ -> new ArrowToEsql(DataType.DATETIME, ElementType.LONG, LongArrowBufBlock::of);
            case TIMESTAMPMICROTZ -> new ArrowToEsql(DataType.DATE_NANOS, ElementType.LONG, LongMul1kArrowBufBlock::of);
            case TIMESTAMPNANOTZ -> new ArrowToEsql(DataType.DATE_NANOS, ElementType.LONG, LongArrowBufBlock::of);

            case INTERVALDAY -> null;
            case INTERVALMONTHDAYNANO -> null;
            case INTERVALYEAR -> null;
            case DURATION -> null;

            case BIT -> new ArrowToEsql(DataType.BOOLEAN, ElementType.BOOLEAN, (v, b) -> BooleanArrowBufBlock.of((BitVector) v, b));

            case VARCHAR -> new ArrowToEsql(DataType.KEYWORD, ElementType.BYTES_REF, BytesRefArrowBufBlock::of);
            // TODO: add support for these
            case VIEWVARCHAR -> null;
            case LARGEVARCHAR -> null;

            // No support for binary in ESQL, even if BytesRef could fit
            case VARBINARY -> null;
            case VIEWVARBINARY -> null;
            case LARGEVARBINARY -> null;

            // Lists are handled in forField(Field)
            case LIST -> null;
            case LISTVIEW -> null;
            case LARGELIST -> null;
            case LARGELISTVIEW -> null;
            case FIXED_SIZE_LIST -> null;

            case STRUCT -> null;
            case UNION -> null;
            case DENSEUNION -> null;
            case MAP -> null;

            case EXTENSIONTYPE -> null;
            case RUNENDENCODED -> null;
        };
    }
}
