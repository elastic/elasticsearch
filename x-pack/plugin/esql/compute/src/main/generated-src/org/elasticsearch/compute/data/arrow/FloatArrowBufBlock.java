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
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.FloatVector;
import org.elasticsearch.core.Nullable;
// end generated imports

/**
 * Implementation of FloatBlock backed by an Arrow buffer holding 32 bits floats.
 * <p>
 * This class is generated. Edit {@code X-ArrowBufBlock.java.st} instead.
 */
public final class FloatArrowBufBlock extends AbstractArrowBufBlock<FloatVector, FloatBlock> implements FloatBlock {

    /**
     *  Create an ArrowBuf block based on the constituents of an Arrow <code>ValueVector</code>. The caller must retain the buffers if they
     *  are shared with other blocks or Arrow vectors.
     */
    public FloatArrowBufBlock(
        ArrowBuf valueBuffer,
        @Nullable ArrowBuf validityBuffer,
        @Nullable ArrowBuf offsetBuffer,
        int valueCount,
        int offsetCount,
        BlockFactory blockFactory
    ) {
        super(valueBuffer, validityBuffer, offsetBuffer, valueCount, offsetCount, blockFactory);
    }

    private FloatArrowBufBlock(ValueVector arrowVector, BlockFactory blockFactory) {
        super(arrowVector, blockFactory);
    }

    public static FloatBlock of(ValueVector arrowVector, BlockFactory blockFactory) {
        FloatBlock constant = tryConstant(arrowVector, blockFactory);
        if (constant != null) {
            return constant;
        }
        return new FloatArrowBufBlock(arrowVector, blockFactory);
    }

    /**
     * Returns a constant block when the vector is fully present and all values are identical,
     * a constant-null block when all values are null, or {@code null} when the caller should
     * fall through to the zero-copy {@link FloatArrowBufBlock} path. Multi-valued (List)
     * inputs return {@code null}; their constant detection would require comparing whole
     * sequences and is not worth the added complexity.
     */
    private static FloatBlock tryConstant(ValueVector arrowVector, BlockFactory blockFactory) {
        if (arrowVector instanceof org.apache.arrow.vector.complex.ListVector) {
            return null;
        }
        // Validate the per-element byte stride before reading the buffer; the constructor
        // does the same check on the fall-through path, so failing fast here keeps both
        // paths' error semantics identical.
        ArrowUtils.checkItemSize((org.apache.arrow.vector.FixedWidthVector) arrowVector, Float.BYTES);
        int rowCount = arrowVector.getValueCount();
        if (rowCount == 0) {
            return null;
        }
        if (arrowVector.getNullCount() == rowCount) {
            return (FloatBlock) blockFactory.newConstantNullBlock(rowCount);
        }
        if (arrowVector.getNullCount() != 0) {
            return null;
        }
        ArrowBuf valueBuffer = arrowVector.getDataBuffer();
        if (ArrowBufConstantDetection.isUniform(valueBuffer, rowCount, Float.BYTES) == false) {
            return null;
        }
        int valueIndex = 0;
        float value = valueBuffer.getFloat((long) valueIndex * Float.BYTES);
        return blockFactory.newConstantFloatBlockWith(value, rowCount);
    }

    @Override
    protected int byteSize() {
        return Float.BYTES;
    }

    @Override
    public int valueMaxByteSize() {
        return Float.BYTES;
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
    public float getFloat(int valueIndex) {
        return valueBuffer.getFloat((long) valueIndex * Float.BYTES);
    }

    @Override
    public ElementType elementType() {
        return ElementType.FLOAT;
    }
}
