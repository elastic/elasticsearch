/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import java.util.function.IntFunction;

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
    NULL(estimatedSize -> new ConstantNullBlock.Builder()),

    BYTES_REF(BytesRefBlock::newBlockBuilder),

    /**
     * Blocks that reference individual lucene documents.
     */
    DOC(DocBlock::newBlockBuilder),

    /**
     * Intermediate blocks which don't support retrieving elements.
     */
    UNKNOWN(estimatedSize -> { throw new UnsupportedOperationException("can't build null blocks"); });

    private final IntFunction<Block.Builder> builder;

    ElementType(IntFunction<Block.Builder> builder) {
        this.builder = builder;
    }

    /**
     * Create a new {@link Block.Builder} for blocks of this type.
     */
    public Block.Builder newBlockBuilder(int estimatedSize) {
        return builder.apply(estimatedSize);
    }
}
