/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.arrow;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.Types;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;

/**
 * Converts Apache Arrow FieldVector to ESQL Blocks.
 * This is the inverse operation of {@link BlockConverter} (Block → Arrow).
 * Together they provide symmetric conversion: Block ↔ Arrow.
 *
 * <p>Type Mapping (symmetric with BlockConverter):
 * <ul>
 *   <li>Arrow FLOAT4 (Float4Vector) → ESQL double (DoubleBlock) - {@link FromFloat32} (ESQL maps FLOAT to DOUBLE)</li>
 *   <li>Arrow FLOAT8 (Float8Vector) ↔ ESQL double (DoubleBlock) - {@link FromFloat64} / {@link BlockConverter.AsFloat64}</li>
 *   <li>Arrow BIGINT (BigIntVector) ↔ ESQL long (LongBlock) - {@link FromInt64} / {@link BlockConverter.AsInt64}</li>
 *   <li>Arrow INT (IntVector) ↔ ESQL integer (IntBlock) - {@link FromInt32} / {@link BlockConverter.AsInt32}</li>
 *   <li>Arrow BIT (BitVector) ↔ ESQL boolean (BooleanBlock) - {@link FromBoolean} / {@link BlockConverter.AsBoolean}</li>
 *   <li>Arrow VARCHAR (VarCharVector) ↔ ESQL keyword (BytesRefBlock) - {@link FromVarChar} / {@link BlockConverter.AsVarChar}</li>
 *   <li>Arrow VARBINARY (VarBinaryVector) ↔ ESQL ip/binary (BytesRefBlock) -
 *          {@link FromVarBinary} / {@link BlockConverter.AsVarBinary}</li>
 *   <li>Arrow TIMESTAMPMICRO (TimeStampMicroVector) → ESQL datetime (LongBlock) - {@link FromTimestampMicro}</li>
 *   <li>Arrow TIMESTAMPMICROTZ (TimeStampMicroTZVector) → ESQL datetime (LongBlock) - {@link FromTimestampMicroTZ}</li>
 * </ul>
 *
 * <p>Note: Timestamp types convert from microseconds (Arrow) to milliseconds (ESQL).
 * Float types (FLOAT4) are converted to double (ESQL doesn't have a separate float type).
 *
 * <p>This converter is designed to be used in the arrow module to keep Arrow dependencies isolated,
 * preventing Arrow from leaking into the compute module.
 */
public abstract class ArrowToBlockConverter {

    /**
     * Convert an Arrow FieldVector to an ESQL Block.
     * @param vector the Arrow vector
     * @param factory the block factory for memory management
     * @return the ESQL block
     */
    public abstract Block convert(FieldVector vector, BlockFactory factory);

    /**
     * Create a converter for the given Arrow type.
     * @param arrowType the Arrow minor type
     * @return the appropriate converter, or null if the type is not supported
     */
    public static ArrowToBlockConverter forType(Types.MinorType arrowType) {
        return switch (arrowType) {
            case FLOAT4 -> new FromFloat32();
            case FLOAT8 -> new FromFloat64();
            case BIGINT -> new FromInt64();
            case INT -> new FromInt32();
            case BIT -> new FromBoolean();
            case VARCHAR -> new FromVarChar();
            case VARBINARY -> new FromVarBinary();
            case TIMESTAMPMICRO -> new FromTimestampMicro();
            case TIMESTAMPMICROTZ -> new FromTimestampMicroTZ();
            default -> null;
        };
    }

    /**
     * Conversion from Arrow Float4Vector (float) to ESQL DoubleBlock.
     * ESQL maps FLOAT to DOUBLE, so we convert float32 to double.
     */
    public static class FromFloat32 extends ArrowToBlockConverter {
        @Override
        public Block convert(FieldVector vector, BlockFactory factory) {
            Float4Vector f4v = (Float4Vector) vector;
            int valueCount = f4v.getValueCount();

            try (DoubleBlock.Builder builder = factory.newDoubleBlockBuilder(valueCount)) {
                for (int i = 0; i < valueCount; i++) {
                    if (f4v.isNull(i)) {
                        builder.appendNull();
                    } else {
                        // Convert float to double for ESQL
                        builder.appendDouble((double) f4v.get(i));
                    }
                }
                return builder.build();
            }
        }
    }

    /**
     * Conversion from Arrow Float8Vector (double) to ESQL DoubleBlock.
     * Symmetric with {@link BlockConverter.AsFloat64}.
     */
    public static class FromFloat64 extends ArrowToBlockConverter {
        @Override
        public Block convert(FieldVector vector, BlockFactory factory) {
            Float8Vector f8v = (Float8Vector) vector;
            int valueCount = f8v.getValueCount();

            try (DoubleBlock.Builder builder = factory.newDoubleBlockBuilder(valueCount)) {
                for (int i = 0; i < valueCount; i++) {
                    if (f8v.isNull(i)) {
                        builder.appendNull();
                    } else {
                        builder.appendDouble(f8v.get(i));
                    }
                }
                return builder.build();
            }
        }
    }

    /**
     * Conversion from Arrow BigIntVector (long) to ESQL LongBlock.
     * Symmetric with {@link BlockConverter.AsInt64}.
     */
    public static class FromInt64 extends ArrowToBlockConverter {
        @Override
        public Block convert(FieldVector vector, BlockFactory factory) {
            BigIntVector bigIntVector = (BigIntVector) vector;
            int valueCount = bigIntVector.getValueCount();

            try (LongBlock.Builder builder = factory.newLongBlockBuilder(valueCount)) {
                for (int i = 0; i < valueCount; i++) {
                    if (bigIntVector.isNull(i)) {
                        builder.appendNull();
                    } else {
                        builder.appendLong(bigIntVector.get(i));
                    }
                }
                return builder.build();
            }
        }
    }

    /**
     * Conversion from Arrow IntVector (int) to ESQL IntBlock.
     * Symmetric with {@link BlockConverter.AsInt32}.
     */
    public static class FromInt32 extends ArrowToBlockConverter {
        @Override
        public Block convert(FieldVector vector, BlockFactory factory) {
            IntVector intVector = (IntVector) vector;
            int valueCount = intVector.getValueCount();

            try (IntBlock.Builder builder = factory.newIntBlockBuilder(valueCount)) {
                for (int i = 0; i < valueCount; i++) {
                    if (intVector.isNull(i)) {
                        builder.appendNull();
                    } else {
                        builder.appendInt(intVector.get(i));
                    }
                }
                return builder.build();
            }
        }
    }

    /**
     * Conversion from Arrow BitVector (boolean) to ESQL BooleanBlock.
     * Symmetric with {@link BlockConverter.AsBoolean}.
     */
    public static class FromBoolean extends ArrowToBlockConverter {
        @Override
        public Block convert(FieldVector vector, BlockFactory factory) {
            BitVector bitVector = (BitVector) vector;
            int valueCount = bitVector.getValueCount();

            try (BooleanBlock.Builder builder = factory.newBooleanBlockBuilder(valueCount)) {
                for (int i = 0; i < valueCount; i++) {
                    if (bitVector.isNull(i)) {
                        builder.appendNull();
                    } else {
                        builder.appendBoolean(bitVector.get(i) != 0);
                    }
                }
                return builder.build();
            }
        }
    }

    /**
     * Conversion from Arrow VarCharVector (string) to ESQL BytesRefBlock.
     * Symmetric with {@link BlockConverter.AsVarChar}.
     */
    public static class FromVarChar extends ArrowToBlockConverter {
        @Override
        public Block convert(FieldVector vector, BlockFactory factory) {
            VarCharVector varCharVector = (VarCharVector) vector;
            int valueCount = varCharVector.getValueCount();

            try (BytesRefBlock.Builder builder = factory.newBytesRefBlockBuilder(valueCount)) {
                for (int i = 0; i < valueCount; i++) {
                    if (varCharVector.isNull(i)) {
                        builder.appendNull();
                    } else {
                        byte[] bytes = varCharVector.get(i);
                        builder.appendBytesRef(new BytesRef(bytes));
                    }
                }
                return builder.build();
            }
        }
    }

    /**
     * Conversion from Arrow VarBinaryVector (binary) to ESQL BytesRefBlock.
     * Symmetric with {@link BlockConverter.AsVarBinary}.
     */
    public static class FromVarBinary extends ArrowToBlockConverter {
        @Override
        public Block convert(FieldVector vector, BlockFactory factory) {
            VarBinaryVector varBinaryVector = (VarBinaryVector) vector;
            int valueCount = varBinaryVector.getValueCount();

            try (BytesRefBlock.Builder builder = factory.newBytesRefBlockBuilder(valueCount)) {
                for (int i = 0; i < valueCount; i++) {
                    if (varBinaryVector.isNull(i)) {
                        builder.appendNull();
                    } else {
                        byte[] bytes = varBinaryVector.get(i);
                        builder.appendBytesRef(new BytesRef(bytes));
                    }
                }
                return builder.build();
            }
        }
    }

    /**
     * Conversion from Arrow TimeStampMicroVector (timestamp without timezone, microseconds) to ESQL LongBlock.
     * Arrow stores timestamps as microseconds since epoch; ESQL stores datetime as milliseconds.
     */
    public static class FromTimestampMicro extends ArrowToBlockConverter {
        @Override
        public Block convert(FieldVector vector, BlockFactory factory) {
            TimeStampMicroVector tsVector = (TimeStampMicroVector) vector;
            int valueCount = tsVector.getValueCount();

            try (LongBlock.Builder builder = factory.newLongBlockBuilder(valueCount)) {
                for (int i = 0; i < valueCount; i++) {
                    if (tsVector.isNull(i)) {
                        builder.appendNull();
                    } else {
                        // Convert from microseconds to milliseconds
                        long micros = tsVector.get(i);
                        builder.appendLong(micros / 1000);
                    }
                }
                return builder.build();
            }
        }
    }

    /**
     * Conversion from Arrow TimeStampMicroTZVector (timestamp with timezone, microseconds) to ESQL LongBlock.
     * Arrow stores timestamps as microseconds since epoch; ESQL stores datetime as milliseconds.
     * The timezone information is not preserved in ESQL's datetime type.
     */
    public static class FromTimestampMicroTZ extends ArrowToBlockConverter {
        @Override
        public Block convert(FieldVector vector, BlockFactory factory) {
            TimeStampMicroTZVector tsVector = (TimeStampMicroTZVector) vector;
            int valueCount = tsVector.getValueCount();

            try (LongBlock.Builder builder = factory.newLongBlockBuilder(valueCount)) {
                for (int i = 0; i < valueCount; i++) {
                    if (tsVector.isNull(i)) {
                        builder.appendNull();
                    } else {
                        // Convert from microseconds to milliseconds
                        long micros = tsVector.get(i);
                        builder.appendLong(micros / 1000);
                    }
                }
                return builder.build();
            }
        }
    }
}
