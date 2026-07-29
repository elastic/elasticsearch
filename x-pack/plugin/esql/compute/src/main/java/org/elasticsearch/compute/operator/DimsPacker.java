/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
import org.elasticsearch.compute.operator.topn.TopNEncoder;
import org.elasticsearch.core.Releasables;

final class DimsPacker {
    private static final TopNEncoder ENCODER = TopNEncoder.DEFAULT_UNSORTABLE;
    static final int INITIAL_SIZE_IN_BYTES = 6 * 1024;

    private DimsPacker() {}

    static int estimateForBytesBuilder(int positionCount) {
        // allocate at least one page for the bytes block builder to avoid copying during resizing
        return Math.max(INITIAL_SIZE_IN_BYTES, positionCount);
    }

    static BytesRefVector packBytesValues(DriverContext driverContext, BytesRefBlock raw) {
        BytesRefVector vector = raw.asVector();
        if (vector != null) {
            OrdinalBytesRefVector ordinals = vector.asOrdinals();
            if (ordinals != null) {
                var encoded = packBytesVector(driverContext, ordinals.getDictionaryVector());
                ordinals.getOrdinalsVector().incRef();
                return new OrdinalBytesRefVector(ordinals.getOrdinalsVector(), encoded);
            } else {
                return packBytesVector(driverContext, vector);
            }
        }
        int positionCount = raw.getPositionCount();
        try (
            var builder = driverContext.blockFactory().newBytesRefVectorBuilder(estimateForBytesBuilder(positionCount));
            var work = new BreakingBytesRefBuilder(driverContext.breaker(), "pack_dims", 1024)
        ) {
            BytesRef scratch = new BytesRef();
            for (int p = 0; p < positionCount; p++) {
                work.clear();
                int valueCount = raw.getValueCount(p);
                ENCODER.encodeInt(valueCount, work);
                int first = raw.getFirstValueIndex(p);
                for (int i = first; i < first + valueCount; i++) {
                    ENCODER.encodeBytesRef(raw.getBytesRef(i, scratch), work);
                }
                builder.appendBytesRef(work.bytesRefView());
            }
            return builder.build();
        }
    }

    static BytesRefVector packBytesVector(DriverContext driverContext, BytesRefVector encode) {
        int positionCount = encode.getPositionCount();
        try (
            var builder = driverContext.blockFactory().newBytesRefVectorBuilder(estimateForBytesBuilder(positionCount));
            var work = new BreakingBytesRefBuilder(driverContext.breaker(), "pack_dims", 1024)
        ) {
            BytesRef scratch = new BytesRef();
            work.clear();
            ENCODER.encodeInt(1, work);
            int countLen = work.length();
            for (int p = 0; p < positionCount; p++) {
                work.setLength(countLen);
                ENCODER.encodeBytesRef(encode.getBytesRef(p, scratch), work);
                builder.appendBytesRef(work.bytesRefView());
            }
            return builder.build();
        }
    }

    static BytesRefBlock unpackBytesValues(DriverContext driverContext, BytesRefVector encoded) {
        int positionCount = encoded.getPositionCount();
        try (var builder = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
            BytesRef inScratch = new BytesRef();
            BytesRef outScratch = new BytesRef();
            for (int p = 0; p < positionCount; p++) {
                BytesRef row = encoded.getBytesRef(p, inScratch);
                int valueCount = ENCODER.decodeInt(row);
                if (valueCount == 0) {
                    builder.appendNull();
                } else if (valueCount == 1) {
                    builder.appendBytesRef(ENCODER.decodeBytesRef(row, outScratch));
                } else {
                    builder.beginPositionEntry();
                    for (int v = 0; v < valueCount; v++) {
                        builder.appendBytesRef(ENCODER.decodeBytesRef(row, outScratch));
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    static BytesRefVector packLongValues(DriverContext driverContext, LongBlock raw) {
        int positionCount = raw.getPositionCount();
        try (
            var builder = driverContext.blockFactory().newBytesRefVectorBuilder(estimateForBytesBuilder(positionCount));
            var work = new BreakingBytesRefBuilder(driverContext.breaker(), "pack_dims", 32)
        ) {
            for (int p = 0; p < positionCount; p++) {
                work.clear();
                int valueCount = raw.getValueCount(p);
                ENCODER.encodeInt(valueCount, work);
                int first = raw.getFirstValueIndex(p);
                for (int i = first; i < first + valueCount; i++) {
                    ENCODER.encodeLong(raw.getLong(i), work);
                }
                builder.appendBytesRef(work.bytesRefView());
            }
            return builder.build();
        }
    }

    static LongBlock unpackLongValues(DriverContext driverContext, BytesRefVector encoded) {
        int positionCount = encoded.getPositionCount();
        try (var builder = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
            BytesRef inScratch = new BytesRef();
            for (int p = 0; p < positionCount; p++) {
                BytesRef row = encoded.getBytesRef(p, inScratch);
                int valueCount = ENCODER.decodeInt(row);
                if (valueCount == 0) {
                    builder.appendNull();
                } else if (valueCount == 1) {
                    builder.appendLong(ENCODER.decodeLong(row));
                } else {
                    builder.beginPositionEntry();
                    for (int v = 0; v < valueCount; v++) {
                        builder.appendLong(ENCODER.decodeLong(row));
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    static BytesRefVector packIntValues(DriverContext driverContext, IntBlock raw) {
        int positionCount = raw.getPositionCount();
        try (
            var builder = driverContext.blockFactory().newBytesRefVectorBuilder(estimateForBytesBuilder(positionCount));
            var work = new BreakingBytesRefBuilder(driverContext.breaker(), "pack_dims", 32)
        ) {
            for (int p = 0; p < positionCount; p++) {
                work.clear();
                int valueCount = raw.getValueCount(p);
                ENCODER.encodeInt(valueCount, work);
                int first = raw.getFirstValueIndex(p);
                for (int i = first; i < first + valueCount; i++) {
                    ENCODER.encodeInt(raw.getInt(i), work);
                }
                builder.appendBytesRef(work.bytesRefView());
            }
            return builder.build();
        }
    }

    static IntBlock unpackIntValues(DriverContext driverContext, BytesRefVector encoded) {
        int positionCount = encoded.getPositionCount();
        try (IntBlock.Builder builder = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
            BytesRef inScratch = new BytesRef();
            for (int p = 0; p < positionCount; p++) {
                BytesRef row = encoded.getBytesRef(p, inScratch);
                int valueCount = ENCODER.decodeInt(row);
                if (valueCount == 0) {
                    builder.appendNull();
                } else if (valueCount == 1) {
                    builder.appendInt(ENCODER.decodeInt(row));
                } else {
                    builder.beginPositionEntry();
                    for (int v = 0; v < valueCount; v++) {
                        builder.appendInt(ENCODER.decodeInt(row));
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    static BytesRefVector packBooleanValues(DriverContext driverContext, BooleanBlock raw) {
        int positionCount = raw.getPositionCount();
        try (
            var builder = driverContext.blockFactory().newBytesRefVectorBuilder(estimateForBytesBuilder(positionCount));
            var work = new BreakingBytesRefBuilder(driverContext.breaker(), "pack_dims", 32)
        ) {
            for (int p = 0; p < positionCount; p++) {
                work.clear();
                int valueCount = raw.getValueCount(p);
                ENCODER.encodeInt(valueCount, work);
                int first = raw.getFirstValueIndex(p);
                for (int i = first; i < first + valueCount; i++) {
                    ENCODER.encodeBoolean(raw.getBoolean(i), work);
                }
                builder.appendBytesRef(work.bytesRefView());
            }
            return builder.build();
        }
    }

    static BooleanBlock unpackBooleanValues(DriverContext driverContext, BytesRefVector encoded) {
        int positionCount = encoded.getPositionCount();
        try (var builder = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
            BytesRef inScratch = new BytesRef();
            for (int p = 0; p < positionCount; p++) {
                BytesRef row = encoded.getBytesRef(p, inScratch);
                int valueCount = ENCODER.decodeInt(row);
                if (valueCount == 0) {
                    builder.appendNull();
                } else if (valueCount == 1) {
                    builder.appendBoolean(ENCODER.decodeBoolean(row));
                } else {
                    builder.beginPositionEntry();
                    for (int v = 0; v < valueCount; v++) {
                        builder.appendBoolean(ENCODER.decodeBoolean(row));
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    static BytesRefVector packNulls(DriverContext driverContext, int positionCount) {
        try (var work = new BreakingBytesRefBuilder(driverContext.breaker(), "pack_dims", 4)) {
            ENCODER.encodeInt(0, work);
            BytesRef bytesRef = work.bytesRefView();
            return driverContext.blockFactory().newConstantBytesRefVector(bytesRef, positionCount);
        }
    }

    static BytesRefVector packMultiColumns(DriverContext driverContext, Block[] blocks) {
        int positionCount = blocks[0].getPositionCount();
        try (
            var builder = driverContext.blockFactory().newBytesRefVectorBuilder(estimateForBytesBuilder(positionCount));
            var work = new BreakingBytesRefBuilder(driverContext.breaker(), "pack_dims", 32)
        ) {
            BytesRef scratch = new BytesRef();
            for (int p = 0; p < positionCount; p++) {
                work.clear();
                for (Block block : blocks) {
                    int valueCount = block.getValueCount(p);
                    ENCODER.encodeInt(valueCount, work);
                    int first = block.getFirstValueIndex(p);
                    for (int v = 0; v < valueCount; v++) {
                        int index = first + v;
                        switch (block.elementType()) {
                            case BYTES_REF -> ENCODER.encodeBytesRef(((BytesRefBlock) block).getBytesRef(index, scratch), work);
                            case LONG -> ENCODER.encodeLong(((LongBlock) block).getLong(index), work);
                            case INT -> ENCODER.encodeInt(((IntBlock) block).getInt(index), work);
                            case BOOLEAN -> ENCODER.encodeBoolean(((BooleanBlock) block).getBoolean(index), work);
                            default -> throw new IllegalStateException("unsupported element type [" + block.elementType() + "]");
                        }
                    }
                }
                builder.appendBytesRef(work.bytesRefView());
            }
            return builder.build();
        }
    }

    static Block[] unpackMultiColumns(DriverContext driverContext, BytesRefVector packed, ElementType[] outputTypes) {
        int positionCount = packed.getPositionCount();
        Block.Builder[] builders = new Block.Builder[outputTypes.length];
        try {
            for (int i = 0; i < outputTypes.length; i++) {
                builders[i] = outputTypes[i].newBlockBuilder(positionCount, driverContext.blockFactory());
            }
            BytesRef inScratch = new BytesRef();
            BytesRef outScratch = new BytesRef();
            for (int p = 0; p < positionCount; p++) {
                BytesRef row = packed.getBytesRef(p, inScratch);
                for (int col = 0; col < outputTypes.length; col++) {
                    int valueCount = ENCODER.decodeInt(row);
                    if (valueCount == 0) {
                        builders[col].appendNull();
                    } else if (valueCount == 1) {
                        appendDecoded(builders[col], outputTypes[col], row, outScratch);
                    } else {
                        builders[col].beginPositionEntry();
                        for (int v = 0; v < valueCount; v++) {
                            appendDecoded(builders[col], outputTypes[col], row, outScratch);
                        }
                        builders[col].endPositionEntry();
                    }
                }
            }
            return Block.Builder.buildAll(builders);
        } finally {
            Releasables.close(builders);
        }
    }

    private static void appendDecoded(Block.Builder builder, ElementType type, BytesRef row, BytesRef scratch) {
        switch (type) {
            case BYTES_REF -> ((BytesRefBlock.Builder) builder).appendBytesRef(ENCODER.decodeBytesRef(row, scratch));
            case LONG -> ((LongBlock.Builder) builder).appendLong(ENCODER.decodeLong(row));
            case INT -> ((IntBlock.Builder) builder).appendInt(ENCODER.decodeInt(row));
            case BOOLEAN -> ((BooleanBlock.Builder) builder).appendBoolean(ENCODER.decodeBoolean(row));
            default -> throw new IllegalStateException("unsupported element type [" + type + "]");
        }
    }

    static BytesRefVector packSingleColumn(DriverContext driverContext, Block block) {
        ElementType elementType = block.elementType();
        return switch (elementType) {
            case NULL -> packNulls(driverContext, block.getPositionCount());
            case BYTES_REF -> packBytesValues(driverContext, (BytesRefBlock) block);
            case LONG -> packLongValues(driverContext, (LongBlock) block);
            case INT -> packIntValues(driverContext, (IntBlock) block);
            case BOOLEAN -> packBooleanValues(driverContext, (BooleanBlock) block);
            default -> throw new IllegalStateException("unsupported element type [" + elementType + "]");
        };
    }

    static Block unpackSingleColumn(DriverContext driverContext, BytesRefVector input, ElementType elementType) {
        return switch (elementType) {
            case NULL -> driverContext.blockFactory().newConstantNullBlock(input.getPositionCount());
            case BYTES_REF -> unpackBytesValues(driverContext, input);
            case LONG -> unpackLongValues(driverContext, input);
            case INT -> unpackIntValues(driverContext, input);
            case BOOLEAN -> unpackBooleanValues(driverContext, input);
            default -> throw new IllegalStateException("unsupported element type [" + elementType + "]");
        };
    }
}
