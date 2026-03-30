/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

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

/**
 * Mapping information from Arrow vectors to an ES|QL blocks.
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

    public Block convert(FieldVector vector, BlockFactory blockFactory) {
        return converter.convert(vector, blockFactory);
    }

    static {
        var map = new EnumMap<Types.MinorType, ArrowToEsql>(Types.MinorType.class);
        // Using a switch in forType0 ensures that all enum members are covered.
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

            // Lists are handled in esqlTypes(Field)
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
