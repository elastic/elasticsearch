/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

// begin generated imports
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.ValueVector;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.$Type$Block;
import org.elasticsearch.compute.data.$Type$Vector;
import org.elasticsearch.core.Nullable;
// end generated imports

/**
 * Implementation of $Type$Block backed by an Arrow buffer holding $arrowDesc$.
 * <p>
 * This class is generated. Edit {@code X-ArrowBufBlock.java.st} instead.
 */
public final class $Name$ArrowBufBlock extends AbstractArrowBufBlock<$Type$Vector, $Type$Block> implements $Type$Block {

    /**
     *  Create an ArrowBuf block based on the constituents of an Arrow <code>ValueVector</code>. The caller must retain the buffers if they
     *  are shared with other blocks or Arrow vectors.
     */
    public $Name$ArrowBufBlock(
        ArrowBuf valueBuffer,
        @Nullable ArrowBuf validityBuffer,
        @Nullable ArrowBuf offsetBuffer,
        int valueCount,
        int offsetCount,
        BlockFactory blockFactory
    ) {
        super(valueBuffer, validityBuffer, offsetBuffer, valueCount, offsetCount, blockFactory);
    }

    private $Name$ArrowBufBlock(ValueVector arrowVector, BlockFactory blockFactory) {
        super(arrowVector, blockFactory);
    }

    public static $Name$ArrowBufBlock of(ValueVector arrowVector, BlockFactory blockFactory) {
        return new $Name$ArrowBufBlock(arrowVector, blockFactory);
    }

    @Override
    protected int byteSize() {
        return $byteSize$;
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
    public ElementType elementType() {
        return ElementType.$TYPE$;
    }
}
