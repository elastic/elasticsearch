/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;

import java.io.IOException;

/**
 * Arrow buffer backed IntVector.
 * This class is generated. Edit {@code X-ArrowBufVector.java.st} instead.
 */
public final class IntArrowBufVector extends AbstractArrowBufVector<IntVector, IntBlock> implements IntVector {

    public IntArrowBufVector(ArrowBuf valueBuffer, int positionCount, BlockFactory blockFactory) {
        super(valueBuffer, positionCount, blockFactory);
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
    public int getInt(int position) {
        return valueBuffer.getInt((long) position * Integer.BYTES);
    }

    @Override
    protected int byteSize() {
        return Integer.BYTES;
    }

    @Override
    public ElementType elementType() {
        return ElementType.INT;
    }

    @Override
    public int min() {
        int v = Integer.MAX_VALUE;
        for (int i = 0; i < positionCount; i++) {
            v = Math.min(v, getInt(i));
        }
        return v;
    }

    @Override
    public int max() {
        int v = Integer.MIN_VALUE;
        for (int i = 0; i < positionCount; i++) {
            v = Math.max(v, getInt(i));
        }
        return v;
    }

}
