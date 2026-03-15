/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

// begin generated imports
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.FixedWidthVector;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleLookup;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.core.ReleasableIterator;

import java.io.IOException;
// end generated imports

/**
 * Arrow buffer backed DoubleVector.
 * This class is generated. Edit {@code X-ArrowBufVector.java.st} instead.
 */
public final class DoubleArrowBufVector extends AbstractArrowBufVector<DoubleVector, DoubleBlock> implements DoubleVector {

    /**
     *  Create an ArrowBuf vector based on the constituents of an Arrow <code>ValueVector</code>. The caller must retain the buffers if they
     *  are shared with other blocks or Arrow vectors.
     */
    public DoubleArrowBufVector(ArrowBuf valueBuffer, int positionCount, BlockFactory blockFactory) {
        super(valueBuffer, positionCount, blockFactory);
    }

    private DoubleArrowBufVector(FixedWidthVector arrowVector, BlockFactory blockFactory) {
        super(arrowVector, blockFactory);
    }

    public static DoubleArrowBufVector of(FixedWidthVector arrowVector, BlockFactory blockFactory) {
        return new DoubleArrowBufVector(arrowVector, blockFactory);
    }

    @Override
    protected ArrowBufVectorConstructor<DoubleVector> vectorConstructor() {
        return DoubleArrowBufVector::new;
    }

    @Override
    protected ArrowBufBlockConstructor<DoubleBlock> blockConstructor() {
        return DoubleArrowBufBlock::new;
    }

    @Override
    public double getDouble(int position) {
        return valueBuffer.getDouble((long) position * Double.BYTES);
    }

    @Override
    protected int byteSize() {
        return Double.BYTES;
    }

    @Override
    public ElementType elementType() {
        return ElementType.DOUBLE;
    }

    @Override
    public ReleasableIterator<DoubleBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        return new DoubleLookup(asBlock(), positions, targetBlockSize);
    }

}
