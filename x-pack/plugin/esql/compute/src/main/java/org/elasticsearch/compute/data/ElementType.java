/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;

/**
 * The type of elements in {@link Block} and {@link Vector}
 */
public enum ElementType {
    BOOLEAN(BooleanBlock::newBlockBuilder),
    INT(IntBlock::newBlockBuilder),
    LONG(LongBlock::newBlockBuilder),
    DOUBLE(DoubleBlock::newBlockBuilder),
    /**
     * Blocks containing only null values.
     */
    NULL((estimatedSize, blockFactory) -> new ConstantNullBlock.Builder()),

    BYTES_REF(BytesRefBlock::newBlockBuilder),

    /**
     * Blocks that reference individual lucene documents.
     */
    DOC(DocBlock::newBlockBuilder),

    /**
     * Intermediate blocks which don't support retrieving elements.
     */
    UNKNOWN((estimatedSize, blockFactory) -> { throw new UnsupportedOperationException("can't build null blocks"); });

    interface BuilderSupplier {
        Block.Builder newBlockBuilder(int estimatedSize, BlockFactory blockFactory);
    }

    private final BuilderSupplier builder;

    ElementType(BuilderSupplier builder) {
        this.builder = builder;
    }

    /**
     * Create a new {@link Block.Builder} for blocks of this type.
     * @deprecated use {@link #newBlockBuilder(int, BlockFactory)}
     */
    @Deprecated
    public Block.Builder newBlockBuilder(int estimatedSize) {
        return builder.newBlockBuilder(estimatedSize, BlockFactory.getNonBreakingInstance());
    }

    /**
     * Create a new {@link Block.Builder} for blocks of this type.
     */
    public Block.Builder newBlockBuilder(int estimatedSize, BlockFactory blockFactory) {
        return builder.newBlockBuilder(estimatedSize, blockFactory);
    }

    public static ElementType fromJava(Class<?> type) {
        ElementType elementType;
        if (type == Integer.class) {
            elementType = INT;
        } else if (type == Long.class) {
            elementType = LONG;
        } else if (type == Double.class) {
            elementType = DOUBLE;
        } else if (type == String.class || type == BytesRef.class) {
            elementType = BYTES_REF;
        } else if (type == Boolean.class) {
            elementType = BOOLEAN;
        } else if (type == null || type == Void.class) {
            elementType = NULL;
        } else {
            throw new IllegalArgumentException("Unrecognized class type " + type);
        }
        return elementType;
    }
}
