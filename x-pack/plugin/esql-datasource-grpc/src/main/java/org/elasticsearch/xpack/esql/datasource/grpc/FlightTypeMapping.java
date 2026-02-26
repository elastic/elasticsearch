/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.List;

/**
 * Maps between Apache Arrow types and ESQL types.
 * Handles both schema conversion (Arrow Field to ESQL Attribute) and
 * data conversion (Arrow FieldVector to ESQL Block).
 */
final class FlightTypeMapping {

    private FlightTypeMapping() {}

    static List<Attribute> toAttributes(Schema schema) {
        List<Attribute> attributes = new ArrayList<>(schema.getFields().size());
        for (Field field : schema.getFields()) {
            DataType dataType = toDataType(field.getType());
            attributes.add(new ReferenceAttribute(Source.EMPTY, field.getName(), dataType));
        }
        return attributes;
    }

    static Block toBlock(FieldVector vector, int rowCount, BlockFactory blockFactory) {
        if (vector instanceof IntVector intVec) {
            try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(rowCount)) {
                for (int i = 0; i < rowCount; i++) {
                    if (intVec.isNull(i)) {
                        builder.appendNull();
                    } else {
                        builder.appendInt(intVec.get(i));
                    }
                }
                return builder.build();
            }
        } else if (vector instanceof BigIntVector bigIntVec) {
            try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(rowCount)) {
                for (int i = 0; i < rowCount; i++) {
                    if (bigIntVec.isNull(i)) {
                        builder.appendNull();
                    } else {
                        builder.appendLong(bigIntVec.get(i));
                    }
                }
                return builder.build();
            }
        } else if (vector instanceof Float8Vector float8Vec) {
            try (DoubleBlock.Builder builder = blockFactory.newDoubleBlockBuilder(rowCount)) {
                for (int i = 0; i < rowCount; i++) {
                    if (float8Vec.isNull(i)) {
                        builder.appendNull();
                    } else {
                        builder.appendDouble(float8Vec.get(i));
                    }
                }
                return builder.build();
            }
        } else if (vector instanceof VarCharVector varCharVec) {
            try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(rowCount)) {
                for (int i = 0; i < rowCount; i++) {
                    if (varCharVec.isNull(i)) {
                        builder.appendNull();
                    } else {
                        byte[] bytes = varCharVec.get(i);
                        builder.appendBytesRef(new BytesRef(bytes));
                    }
                }
                return builder.build();
            }
        } else if (vector instanceof BitVector bitVec) {
            try (BooleanBlock.Builder builder = blockFactory.newBooleanBlockBuilder(rowCount)) {
                for (int i = 0; i < rowCount; i++) {
                    if (bitVec.isNull(i)) {
                        builder.appendNull();
                    } else {
                        builder.appendBoolean(bitVec.get(i) != 0);
                    }
                }
                return builder.build();
            }
        } else if (vector instanceof TimeStampMilliVector tsVec) {
            try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(rowCount)) {
                for (int i = 0; i < rowCount; i++) {
                    if (tsVec.isNull(i)) {
                        builder.appendNull();
                    } else {
                        builder.appendLong(tsVec.get(i));
                    }
                }
                return builder.build();
            }
        }
        throw new IllegalArgumentException("Unsupported Arrow vector type: " + vector.getClass().getSimpleName());
    }

    private static DataType toDataType(ArrowType arrowType) {
        if (arrowType instanceof ArrowType.Int intType) {
            return intType.getBitWidth() <= 32 ? DataType.INTEGER : DataType.LONG;
        } else if (arrowType instanceof ArrowType.FloatingPoint fpType) {
            return switch (fpType.getPrecision()) {
                case DOUBLE -> DataType.DOUBLE;
                case SINGLE -> DataType.DOUBLE;
                default -> DataType.DOUBLE;
            };
        } else if (arrowType instanceof ArrowType.Utf8) {
            return DataType.KEYWORD;
        } else if (arrowType instanceof ArrowType.Bool) {
            return DataType.BOOLEAN;
        } else if (arrowType instanceof ArrowType.Timestamp) {
            return DataType.DATETIME;
        }
        throw new IllegalArgumentException("Unsupported Arrow type: " + arrowType);
    }
}
