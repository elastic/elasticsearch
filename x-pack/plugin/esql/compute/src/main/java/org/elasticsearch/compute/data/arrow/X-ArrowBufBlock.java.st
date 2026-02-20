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
import org.elasticsearch.compute.data.$Type$Block;
import org.elasticsearch.compute.data.$Type$Vector;
import org.elasticsearch.core.Nullable;

/**
 * Arrow buffer backed $Type$Block.
 * This class is generated. Edit {@code X-ArrowBufBlock.java.st} instead.
 */
public final class $Type$ArrowBufBlock extends AbstractArrowBufBlock<$Type$Vector, $Type$Block> implements $Type$Block {

    public $Type$ArrowBufBlock(
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
        return $BYTES$;
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
    public $type$ get$Type$(int valueIndex) {
        return valueBuffer.get$Type$((long) valueIndex * $BYTES$);
    }

    @Override
    public ElementType elementType() {
        return ElementType.$TYPE$;
    }
}
