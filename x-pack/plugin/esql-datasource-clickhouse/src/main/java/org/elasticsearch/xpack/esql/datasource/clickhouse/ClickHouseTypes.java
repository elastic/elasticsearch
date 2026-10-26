/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;

import java.math.BigInteger;

/** Preserves integer precision and converts temporal columns to ES|QL millisecond timestamps. */
final class ClickHouseTypes {
    private ClickHouseTypes() {}

    static DataType dataType(String type) {
        while ((type.startsWith("Nullable(") || type.startsWith("LowCardinality(")) && type.endsWith(")")) {
            type = type.substring(type.indexOf('(') + 1, type.length() - 1);
        }
        if (type.startsWith("DateTime(") || type.startsWith("DateTime64(")) {
            return DataType.DATETIME;
        }
        if (type.startsWith("Decimal") || type.startsWith("FixedString(") || type.startsWith("Enum8(") || type.startsWith("Enum16(")) {
            return DataType.KEYWORD;
        }
        return switch (type) {
            case "Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32" -> DataType.LONG;
            case "UInt64" -> DataType.UNSIGNED_LONG;
            case "Float32", "Float64" -> DataType.DOUBLE;
            case "Bool" -> DataType.BOOLEAN;
            case "Date", "Date32", "DateTime" -> DataType.DATETIME;
            case "String", "UUID", "IPv4", "IPv6", "Int128", "Int256", "UInt128", "UInt256" -> DataType.KEYWORD;
            default -> throw new IllegalArgumentException("ClickHouse PoC does not support column type [" + type + "]");
        };
    }

    static Block.Builder builder(DataType type, BlockFactory factory, int size) {
        return switch (type) {
            case LONG, UNSIGNED_LONG, DATETIME -> factory.newLongBlockBuilder(size);
            case DOUBLE -> factory.newDoubleBlockBuilder(size);
            case BOOLEAN -> factory.newBooleanBlockBuilder(size);
            case KEYWORD -> factory.newBytesRefBlockBuilder(size);
            default -> throw new IllegalArgumentException("Unsupported ClickHouse result type [" + type + "]");
        };
    }

    static void append(Block.Builder builder, DataType type, Object value) {
        if (value == null) {
            builder.appendNull();
            return;
        }
        switch (type) {
            case LONG, DATETIME -> ((LongBlock.Builder) builder).appendLong(Long.parseLong(value.toString()));
            case UNSIGNED_LONG -> {
                BigInteger number = new BigInteger(value.toString());
                if (NumericUtils.isUnsignedLong(number) == false) {
                    throw new IllegalArgumentException("ClickHouse UInt64 result is outside its valid range");
                }
                ((LongBlock.Builder) builder).appendLong(NumericUtils.asLongUnsigned(number));
            }
            case DOUBLE -> {
                String number = value.toString();
                if (number.equals("nan")) {
                    number = "NaN";
                } else if (number.equals("inf")) {
                    number = "Infinity";
                } else if (number.equals("-inf")) {
                    number = "-Infinity";
                }
                ((DoubleBlock.Builder) builder).appendDouble(Double.parseDouble(number));
            }
            case BOOLEAN -> {
                if (value instanceof Boolean bool) {
                    ((BooleanBlock.Builder) builder).appendBoolean(bool);
                } else {
                    throw new IllegalArgumentException("Invalid ClickHouse boolean result");
                }
            }
            case KEYWORD -> ((BytesRefBlock.Builder) builder).appendBytesRef(new BytesRef(value.toString()));
            default -> throw new IllegalArgumentException("Unsupported ClickHouse result type [" + type + "]");
        }
    }
}
