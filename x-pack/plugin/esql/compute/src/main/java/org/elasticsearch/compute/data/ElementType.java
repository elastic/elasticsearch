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
    BOOLEAN(BlockFactory::newBooleanBlockBuilder),
    INT(BlockFactory::newIntBlockBuilder),
    LONG(BlockFactory::newLongBlockBuilder),
    DOUBLE(BlockFactory::newDoubleBlockBuilder),
    /**
     * Blocks containing only null values.
     */
    NULL((blockFactory, estimatedSize) -> new ConstantNullBlock.Builder(blockFactory)),

    BYTES_REF(BlockFactory::newBytesRefBlockBuilder),

    /**
     * Blocks that reference individual lucene documents.
     */
    DOC(DocBlock::newBlockBuilder),

    /**
     * Intermediate blocks which don't support retrieving elements.
     */
    UNKNOWN((blockFactory, estimatedSize) -> { throw new UnsupportedOperationException("can't build null blocks"); });

    private interface BuilderSupplier {
        Block.Builder newBlockBuilder(BlockFactory blockFactory, int estimatedSize);
    }

    private final BuilderSupplier builder;

    ElementType(BuilderSupplier builder) {
        this.builder = builder;
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
