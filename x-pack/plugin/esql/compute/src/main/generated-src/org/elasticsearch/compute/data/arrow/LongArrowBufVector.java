/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

// begin generated imports
import org.apache.arrow.memory.ArrowBuf;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;

import java.io.IOException;
// end generated imports

/**
 * Arrow buffer backed LongVector.
 * This class is generated. Edit {@code X-ArrowBufVector.java.st} instead.
 */
public final class LongArrowBufVector extends AbstractArrowBufVector<LongVector, LongBlock> implements LongVector {

    /**
     *  Create an ArrowBuf block based on the constituents of an Arrow ValueVector. It does not take ownership of buffers but rather
     *  increases their reference count. This means that callers must release the buffers (and decrease their reference counters)
     *  if they don't need them anymore.
     */
    public LongArrowBufVector(ArrowBuf valueBuffer, int positionCount, BlockFactory blockFactory) {
        super(valueBuffer, positionCount, blockFactory);
    }

    @Override
    protected ArrowBufVectorConstructor<LongVector> vectorConstructor() {
        return LongArrowBufVector::new;
    }

    @Override
    protected ArrowBufBlockConstructor<LongBlock> blockConstructor() {
        return LongArrowBufBlock::new;
    }

    @Override
    public long getLong(int position) {
        return valueBuffer.getLong((long) position * Long.BYTES);
    }

    @Override
    protected int byteSize() {
        return Long.BYTES;
    }

    @Override
    public ElementType elementType() {
        return ElementType.LONG;
    }

}
