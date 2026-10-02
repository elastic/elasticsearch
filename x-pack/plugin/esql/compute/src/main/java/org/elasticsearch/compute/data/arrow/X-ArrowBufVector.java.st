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
import org.elasticsearch.compute.data.$Type$Block;
import org.elasticsearch.compute.data.$Type$Lookup;
import org.elasticsearch.compute.data.$Type$Vector;
import org.elasticsearch.core.ReleasableIterator;

import java.io.IOException;
// end generated imports

/**
 * Implementation of $Type$Vector backed by an Arrow buffer holding $arrowDesc$.
 * <p>
 * This class is generated. Edit {@code X-ArrowBufVector.java.st} instead.
 */
public final class $Name$ArrowBufVector extends AbstractArrowBufVector<$Type$Vector, $Type$Block> implements $Type$Vector {

    /**
     *  Create an ArrowBuf vector based on the constituents of an Arrow <code>ValueVector</code>. The caller must retain the buffers if they
     *  are shared with other blocks or Arrow vectors.
     */
    public $Name$ArrowBufVector(ArrowBuf valueBuffer, int positionCount, BlockFactory blockFactory) {
        super(valueBuffer, positionCount, blockFactory);
    }

    private $Name$ArrowBufVector(FixedWidthVector arrowVector, BlockFactory blockFactory) {
        super(arrowVector, blockFactory);
    }

    public static $Name$ArrowBufVector of(FixedWidthVector arrowVector, BlockFactory blockFactory) {
        return new $Name$ArrowBufVector(arrowVector, blockFactory);
    }

    @Override
    protected ArrowBufVectorConstructor<$Type$Vector> vectorConstructor() {
        return $Name$ArrowBufVector::new;
    }

    @Override
    protected ArrowBufBlockConstructor<$Type$Block> blockConstructor() {
        return $Name$ArrowBufBlock::new;
    }

    @Override
    public $type$ get$Type$(int valueIndex) {
        return $valueRead$;
    }

    @Override
    protected int byteSize() {
        return $byteSize$;
    }

    @Override
    public int valueMaxByteSize() {
        return $BYTES$;
    }

    @Override
    public ElementType elementType() {
        return ElementType.$TYPE$;
    }

    @Override
    public ReleasableIterator<$Type$Block> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        return new $Type$Lookup(asBlock(), positions, targetBlockSize);
    }

    @Override
    public $Type$Vector slice(int beginInclusive, int endExclusive) {
        if (beginInclusive == 0 && endExclusive == getPositionCount()) {
            incRef();
            return this;
        }
        try ($Type$Vector.FixedBuilder builder = blockFactory().new$Type$VectorFixedBuilder(endExclusive - beginInclusive)) {
            for (int i = beginInclusive; i < endExclusive; i++) {
                builder.append$Type$(get$Type$(i));
            }
            return builder.build();
        }
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
