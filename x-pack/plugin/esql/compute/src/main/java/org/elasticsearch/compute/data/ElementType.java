/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The type of elements in {@link Block} and {@link Vector}
 */
public enum ElementType {
    BOOLEAN(0, "Boolean", BlockFactory::newBooleanBlockBuilder, BooleanBlock::readFrom),
    INT(1, "Int", BlockFactory::newIntBlockBuilder, IntBlock::readFrom),
    LONG(2, "Long", BlockFactory::newLongBlockBuilder, LongBlock::readFrom),
    FLOAT(3, "Float", BlockFactory::newFloatBlockBuilder, FloatBlock::readFrom),
    DOUBLE(4, "Double", BlockFactory::newDoubleBlockBuilder, DoubleBlock::readFrom),
    /**
     * Blocks containing only null values.
     */
    NULL(5, "Null", (blockFactory, estimatedSize) -> new ConstantNullBlock.Builder(blockFactory), BlockStreamInput::readConstantNullBlock),

    BYTES_REF(6, "BytesRef", BlockFactory::newBytesRefBlockBuilder, BytesRefBlock::readFrom),

    /**
     * Blocks that reference individual lucene documents.
     */
    DOC(7, "Doc", DocBlock::newBlockBuilder, in -> { throw new UnsupportedOperationException("can't read doc blocks"); }),

    /**
     * Composite blocks which contain array of sub-blocks.
     */
    COMPOSITE(
        8,
        "Composite",
        (blockFactory, estimatedSize) -> { throw new UnsupportedOperationException("can't build composite blocks"); },
        CompositeBlock::readFrom
    ),

    /**
     * Intermediate blocks which don't support retrieving elements.
     */
    UNKNOWN(9, "Unknown", (blockFactory, estimatedSize) -> { throw new UnsupportedOperationException("can't build null blocks"); }, in -> {
        throw new UnsupportedOperationException("can't read unknown blocks");
    }),

    /**
     * Blocks that contain aggregate_metric_doubles.
     */
    AGGREGATE_METRIC_DOUBLE(
        10,
        "AggregateMetricDouble",
        BlockFactory::newAggregateMetricDoubleBlockBuilder,
        AggregateMetricDoubleBlock::readFrom
    );

    private interface BuilderSupplier {
        Block.Builder newBlockBuilder(BlockFactory blockFactory, int estimatedSize);
    }

    private final String pascalCaseName;
    private final BuilderSupplier builder;

    private final byte writableCode;
    private final String legacyWritableName;
    final BlockReader reader;

    ElementType(int writeableCode, String pascalCaseName, BuilderSupplier builder, BlockReader reader) {
        this.writableCode = (byte) writeableCode;
        if (this.writableCode != writeableCode) {
            throw new IllegalArgumentException("code must be in the range [0, " + Byte.MAX_VALUE + "); got " + writeableCode);
        }
        this.pascalCaseName = pascalCaseName;
        this.legacyWritableName = "Null".equals(pascalCaseName) ? "ConstantNullBlock" : pascalCaseName + "Block";
        this.builder = builder;
        this.reader = reader;
    }

    /**
     * Create a new {@link Block.Builder} for blocks of this type.
     */
    public Block.Builder newBlockBuilder(int estimatedSize, BlockFactory blockFactory) {
        return builder.newBlockBuilder(blockFactory, estimatedSize);
    }

    public static ElementType fromJava(Class<?> type) {
        ElementType elementType;
        if (type == Integer.class) {
            elementType = INT;
        } else if (type == Long.class) {
            elementType = LONG;
        } else if (type == Float.class) {
            elementType = FLOAT;
        } else if (type == Double.class) {
            elementType = DOUBLE;
        } else if (type == String.class || type == BytesRef.class) {
            elementType = BYTES_REF;
        } else if (type == Boolean.class) {
            elementType = BOOLEAN;
        } else if (type == AggregateMetricDoubleBlockBuilder.AggregateMetricDoubleLiteral.class) {
            elementType = AGGREGATE_METRIC_DOUBLE;
        } else if (type == null || type == Void.class) {
            elementType = NULL;
        } else {
            throw new IllegalArgumentException("Unrecognized class type " + type);
        }
        return elementType;
    }

    public String pascalCaseName() {
        return pascalCaseName;
    }

    private static final Map<String, ElementType> fromLegacyNames;

    static {
        ElementType[] values = values();
        for (int i = 0; i < values.length; i++) {
            assert values[i].writableCode == i;
        }
        fromLegacyNames = Arrays.stream(values).collect(Collectors.toUnmodifiableMap(e -> e.legacyWritableName, Function.identity()));
    }

    /**
     * Read element type from an input stream
     */
    static ElementType readFrom(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersions.ESQL_SERIALIZE_BLOCK_TYPE_CODE)
            || in.getTransportVersion().isPatchFrom(TransportVersions.ESQL_SERIALIZE_BLOCK_TYPE_CODE_8_19)) {
            byte b = in.readByte();
            return values()[b];
        } else {
            final String writeableName = in.readString();
            ElementType elementType = fromLegacyNames.get(writeableName);
            if (elementType == null) {
                throw new IllegalArgumentException("Unknown element type for named writable [" + writeableName + "]");
            }
            return elementType;
        }
    }

    void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_SERIALIZE_BLOCK_TYPE_CODE)
            || out.getTransportVersion().isPatchFrom(TransportVersions.ESQL_SERIALIZE_BLOCK_TYPE_CODE_8_19)) {
            out.writeByte(writableCode);
        } else {
            out.writeString(legacyWritableName);
        }
    }

    interface BlockReader {
        Block readBlock(BlockStreamInput in) throws IOException;
    }
}
