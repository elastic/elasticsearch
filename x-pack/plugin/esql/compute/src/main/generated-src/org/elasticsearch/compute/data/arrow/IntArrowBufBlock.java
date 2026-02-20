/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.core.Nullable;

/**
 * Arrow buffer backed IntBlock.
 * This class is generated. Edit {@code X-ArrowBufBlock.java.st} instead.
 */
public final class IntArrowBufBlock extends AbstractArrowBufBlock<IntVector, IntBlock> implements IntBlock {

    public IntArrowBufBlock(
        ArrowBuf valueBuffer,
        @Nullable ArrowBuf validityBuffer,
        @Nullable ArrowBuf offsetBuffer,
        int valueCount,
        int offsetCount,
        BlockFactory blockFactory
    ) {
        super(valueBuffer, validityBuffer, offsetBuffer, valueCount, offsetCount, blockFactory);
    }

    @Override
    protected int byteSize() {
        return Integer.BYTES;
    }

    @Override
    protected ArrowBufVectorConstructor<IntVector> vectorConstructor() {
        return IntArrowBufVector::new;
    }

    @Override
    protected ArrowBufBlockConstructor<IntBlock> blockConstructor() {
        return IntArrowBufBlock::new;
    }

    @Override
    public int getInt(int valueIndex) {
        return valueBuffer.getInt((long) valueIndex * Integer.BYTES);
    }

    @Override
    public ElementType elementType() {
        return ElementType.INT;
    }
}
