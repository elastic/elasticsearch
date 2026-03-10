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
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.FloatLookup;
import org.elasticsearch.compute.data.FloatVector;
import org.elasticsearch.core.ReleasableIterator;

import java.io.IOException;
// end generated imports

/**
 * Arrow buffer backed FloatVector.
 * This class is generated. Edit {@code X-ArrowBufVector.java.st} instead.
 */
public final class FloatArrowBufVector extends AbstractArrowBufVector<FloatVector, FloatBlock> implements FloatVector {

    /**
     *  Create an ArrowBuf vector based on the constituents of an Arrow <code>ValueVector</code>. The caller must retain the buffers if they
     *  are shared with other blocks or Arrow vectors.
     */
    public FloatArrowBufVector(ArrowBuf valueBuffer, int positionCount, BlockFactory blockFactory) {
        super(valueBuffer, positionCount, blockFactory);
    }

    private FloatArrowBufVector(FixedWidthVector arrowVector, BlockFactory blockFactory) {
        super(arrowVector, blockFactory);
    }

    public static FloatArrowBufVector of(FixedWidthVector arrowVector, BlockFactory blockFactory) {
        return new FloatArrowBufVector(arrowVector, blockFactory);
    }

    @Override
    protected ArrowBufVectorConstructor<FloatVector> vectorConstructor() {
        return FloatArrowBufVector::new;
    }

    @Override
    protected ArrowBufBlockConstructor<FloatBlock> blockConstructor() {
        return FloatArrowBufBlock::new;
    }

    @Override
    public float getFloat(int position) {
        return valueBuffer.getFloat((long) position * Float.BYTES);
    }

    @Override
    protected int byteSize() {
        return Float.BYTES;
    }

    @Override
    public ElementType elementType() {
        return ElementType.FLOAT;
    }

    @Override
    public ReleasableIterator<FloatBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        return new FloatLookup(asBlock(), positions, targetBlockSize);
    }

}
