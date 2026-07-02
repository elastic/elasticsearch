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

    public static $Type$Block of(ValueVector arrowVector, BlockFactory blockFactory) {
        $Type$Block constant = tryConstant(arrowVector, blockFactory);
        if (constant != null) {
            return constant;
        }
        return new $Name$ArrowBufBlock(arrowVector, blockFactory);
    }

    /**
     * Returns a constant block when the vector is fully present and all values are identical,
     * a constant-null block when all values are null, or {@code null} when the caller should
     * fall through to the zero-copy {@link $Name$ArrowBufBlock} path. Multi-valued (List)
     * inputs return {@code null}; their constant detection would require comparing whole
     * sequences and is not worth the added complexity.
     */
    private static $Type$Block tryConstant(ValueVector arrowVector, BlockFactory blockFactory) {
        if (arrowVector instanceof org.apache.arrow.vector.complex.ListVector) {
            return null;
        }
        // Validate the per-element byte stride before reading the buffer; the constructor
        // does the same check on the fall-through path, so failing fast here keeps both
        // paths' error semantics identical.
        ArrowUtils.checkItemSize((org.apache.arrow.vector.FixedWidthVector) arrowVector, $byteSize$);
        int rowCount = arrowVector.getValueCount();
        if (rowCount == 0) {
            return null;
        }
        if (arrowVector.getNullCount() == rowCount) {
            return ($Type$Block) blockFactory.newConstantNullBlock(rowCount);
        }
        if (arrowVector.getNullCount() != 0) {
            return null;
        }
        ArrowBuf valueBuffer = arrowVector.getDataBuffer();
        if (ArrowBufConstantDetection.isUniform(valueBuffer, rowCount, $byteSize$) == false) {
            return null;
        }
        int valueIndex = 0;
        $type$ value = $valueRead$;
        return blockFactory.newConstant$Type$BlockWith(value, rowCount);
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
