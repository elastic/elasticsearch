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
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.core.Nullable;
// end generated imports

/**
 * Implementation of DoubleBlock backed by an Arrow buffer holding 64 bits floats.
 * <p>
 * This class is generated. Edit {@code X-ArrowBufBlock.java.st} instead.
 */
public final class DoubleArrowBufBlock extends AbstractArrowBufBlock<DoubleVector, DoubleBlock> implements DoubleBlock {

    /**
     *  Create an ArrowBuf block based on the constituents of an Arrow <code>ValueVector</code>. The caller must retain the buffers if they
     *  are shared with other blocks or Arrow vectors.
     */
    public DoubleArrowBufBlock(
        ArrowBuf valueBuffer,
        @Nullable ArrowBuf validityBuffer,
        @Nullable ArrowBuf offsetBuffer,
        int valueCount,
        int offsetCount,
        BlockFactory blockFactory
    ) {
        super(valueBuffer, validityBuffer, offsetBuffer, valueCount, offsetCount, blockFactory);
    }

    private DoubleArrowBufBlock(ValueVector arrowVector, BlockFactory blockFactory) {
        super(arrowVector, blockFactory);
    }

    public static DoubleBlock of(ValueVector arrowVector, BlockFactory blockFactory) {
        DoubleBlock constant = tryConstant(arrowVector, blockFactory);
        if (constant != null) {
            return constant;
        }
        return new DoubleArrowBufBlock(arrowVector, blockFactory);
    }

    /**
     * Returns a constant block when the vector is fully present and all values are identical,
     * a constant-null block when all values are null, or {@code null} when the caller should
     * fall through to the zero-copy {@link DoubleArrowBufBlock} path. Multi-valued (List)
     * inputs return {@code null}; their constant detection would require comparing whole
     * sequences and is not worth the added complexity.
     */
    private static DoubleBlock tryConstant(ValueVector arrowVector, BlockFactory blockFactory) {
        if (arrowVector instanceof org.apache.arrow.vector.complex.ListVector) {
            return null;
        }
        // Validate the per-element byte stride before reading the buffer; the constructor
        // does the same check on the fall-through path, so failing fast here keeps both
        // paths' error semantics identical.
        ArrowUtils.checkItemSize((org.apache.arrow.vector.FixedWidthVector) arrowVector, Double.BYTES);
        int rowCount = arrowVector.getValueCount();
        if (rowCount == 0) {
            return null;
        }
        if (arrowVector.getNullCount() == rowCount) {
            return (DoubleBlock) blockFactory.newConstantNullBlock(rowCount);
        }
        if (arrowVector.getNullCount() != 0) {
            return null;
        }
        ArrowBuf valueBuffer = arrowVector.getDataBuffer();
        if (ArrowBufConstantDetection.isUniform(valueBuffer, rowCount, Double.BYTES) == false) {
            return null;
        }
        int valueIndex = 0;
        double value = valueBuffer.getDouble((long) valueIndex * Double.BYTES);
        return blockFactory.newConstantDoubleBlockWith(value, rowCount);
    }

    @Override
    protected int byteSize() {
        return Double.BYTES;
    }

    @Override
    public int valueMaxByteSize() {
        return Double.BYTES;
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
    public double getDouble(int valueIndex) {
        return valueBuffer.getDouble((long) valueIndex * Double.BYTES);
    }

    @Override
    public ElementType elementType() {
        return ElementType.DOUBLE;
    }
}
