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
import org.elasticsearch.compute.data.$Type$Block;
import org.elasticsearch.compute.data.$Type$Vector;

import java.io.IOException;
// end generated imports

/**
 * Arrow buffer backed $Type$Vector.
 * This class is generated. Edit {@code X-ArrowBufVector.java.st} instead.
 */
public final class $Type$ArrowBufVector extends AbstractArrowBufVector<$Type$Vector, $Type$Block> implements $Type$Vector {

    /**
     *  Create an ArrowBuf block based on the constituents of an Arrow ValueVector. It does not take ownership of buffers but rather
     *  increases their reference count. This means that callers must release the buffers (and decrease their reference counters)
     *  if they don't need them anymore.
     */
    public $Type$ArrowBufVector(ArrowBuf valueBuffer, int positionCount, BlockFactory blockFactory) {
        super(valueBuffer, positionCount, blockFactory);
    }

    @Override
    protected ArrowBufVectorConstructor<$Type$Vector> vectorConstructor() {
        return $Type$ArrowBufVector::new;
    }

    @Override
    protected ArrowBufBlockConstructor<$Type$Block> blockConstructor() {
        return $Type$ArrowBufBlock::new;
    }

    @Override
    public $type$ get$Type$(int position) {
        return valueBuffer.get$Type$((long) position * $BYTES$);
    }

    @Override
    protected int byteSize() {
        return $BYTES$;
    }

    @Override
    public ElementType elementType() {
        return ElementType.$TYPE$;
    }

$if(int)$
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

$endif$
}
