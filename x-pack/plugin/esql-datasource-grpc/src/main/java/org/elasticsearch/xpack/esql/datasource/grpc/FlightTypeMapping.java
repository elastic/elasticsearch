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
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.arrow.BooleanArrowBufBlock;
import org.elasticsearch.compute.data.arrow.BytesRefArrowBufBlock;
import org.elasticsearch.compute.data.arrow.DoubleArrowBufBlock;
import org.elasticsearch.compute.data.arrow.IntArrowBufBlock;
import org.elasticsearch.compute.data.arrow.LongArrowBufBlock;
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

    static <V extends ValueVector> V transfer(V vector, BlockFactory blockFactory) {
        var tp = vector.getTransferPair(blockFactory.arrowAllocator());
        tp.transfer();
        @SuppressWarnings("unchecked")
        var result = (V) tp.getTo();
        return result;
    }

    static Block toBlock(FieldVector flightVector, int rowCount, BlockFactory blockFactory) {
        // Trim the vector to the expected size (doesn't shrink buffers)
        if (flightVector.getValueCount() > rowCount) {
            flightVector.setValueCount(rowCount);
        }

        try (var vector = transfer(flightVector, blockFactory)) {

            if (vector instanceof IntVector intVec) {
                return IntArrowBufBlock.of(intVec, blockFactory);

            } else if (vector instanceof BigIntVector bigIntVec) {
                return LongArrowBufBlock.of(bigIntVec, blockFactory);

            } else if (vector instanceof Float8Vector float8Vec) {
                return DoubleArrowBufBlock.of(float8Vec, blockFactory);

            } else if (vector instanceof VarCharVector varCharVec) {
                return BytesRefArrowBufBlock.of(varCharVec, blockFactory);

            } else if (vector instanceof BitVector bitVec) {
                return BooleanArrowBufBlock.of(bitVec, blockFactory);

            } else if (vector instanceof TimeStampMilliVector tsVec) {
                return LongArrowBufBlock.of(tsVec, blockFactory);
            }
            throw new IllegalArgumentException("Unsupported Arrow vector type: " + vector.getClass().getSimpleName());
        }
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
