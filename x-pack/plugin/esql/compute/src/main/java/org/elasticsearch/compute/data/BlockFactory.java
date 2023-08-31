/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.compute.data.Block.MvOrdering;

import java.util.BitSet;

/** A factory for creating Blocks and Vectors. */
public class BlockFactory {

    private static final BlockFactory DEFAULT = new BlockFactory();

    private BlockFactory() { }

    /** Returns an instance of a block factory. */
    public static BlockFactory getDefault() {
        return DEFAULT;
    }

    /** Returns an instance of a block factory. */
    public static BlockFactory getInstance() {
        return new BlockFactory();
    }

    public BooleanBlock.Builder newBooleanBlockBuilder(int estimatedSize) {
        return new BooleanBlockBuilder(estimatedSize);
    }

    public BooleanBlock newBooleanArrayBlock(boolean[] values, int positionCount, int[] firstValueIndexes, BitSet nulls, MvOrdering mvOrdering) {
        return new BooleanArrayBlock(values, positionCount, firstValueIndexes, nulls, mvOrdering);
    }

    public BooleanVector.Builder newBooleanVectorBuilder(int estimatedSize) {
        return new BooleanVectorBuilder(estimatedSize);
    }

    public BooleanVector newBooleanArrayVector(boolean[] values, int positionCount) {
        return new BooleanArrayVector(values, positionCount);
    }

    public BooleanVector newConstantBooleanVector(boolean value, int positions) {
        return new ConstantBooleanVector(value, positions);
    }

    public IntBlock.Builder newIntBlockBuilder(int estimatedSize) {
        return new IntBlockBuilder(estimatedSize);
    }

    public IntBlock newIntArrayBlock(int[] values, int positionCount, int[] firstValueIndexes, BitSet nulls, MvOrdering mvOrdering) {
        return new IntArrayBlock(values, positionCount, firstValueIndexes, nulls, mvOrdering);
    }

    public IntVector.Builder newIntVectorBuilder(int estimatedSize) {
        return new IntVectorBuilder(estimatedSize);
    }

    public IntVector newIntArrayVector(int[] values, int positionCount) {
        return new IntArrayVector(values, positionCount);
    }

    public IntVector newConstantIntVector(int value, int positions) {
        return new ConstantIntVector(value, positions);
    }

    public LongBlock.Builder newLongBlockBuilder(int estimatedSize) {
        return new LongBlockBuilder(estimatedSize);
    }

    public LongBlock newLongArrayBlock(long[] values, int positionCount, int[] firstValueIndexes, BitSet nulls, MvOrdering mvOrdering) {
        return new LongArrayBlock(values, positionCount, firstValueIndexes, nulls, mvOrdering);
    }

    public LongVector.Builder newLongVectorBuilder(int estimatedSize) {
        return new LongVectorBuilder(estimatedSize);
    }

    public LongVector newLongArrayVector(long[] values, int positionCount) {
        return new LongArrayVector(values, positionCount);
    }

    public LongVector newConstantLongVector(long value, int positions) {
        return new ConstantLongVector(value, positions);
    }

    public DoubleBlock.Builder newDoubleBlockBuilder(int estimatedSize) {
        return new DoubleBlockBuilder(estimatedSize);
    }

    public DoubleBlock newDoubleArrayBlock(double[] values, int positionCount,
                                           int[] firstValueIndexes, BitSet nulls, MvOrdering mvOrdering) {
        return new DoubleArrayBlock(values, positionCount, firstValueIndexes, nulls, mvOrdering);
    }

    public DoubleVector.Builder newDoubleVectorBuilder(int estimatedSize) {
        return new DoubleVectorBuilder(estimatedSize);
    }

    public DoubleVector newDoubleArrayVector(double[] values, int positionCount) {
        return new DoubleArrayVector(values, positionCount);
    }

    public DoubleVector newConstantDoubleVector(double value, int positions) {
        return new ConstantDoubleVector(value, positions);
    }

    public BytesRefBlock.Builder newBytesRefBlockBuilder(int estimatedSize) {
        return new BytesRefBlockBuilder(estimatedSize);
    }

    public BytesRefBlock newBytesRefArrayBlock(BytesRefArray values, int positionCount, int[] firstValueIndexes, BitSet nulls, MvOrdering mvOrdering) {
        return new BytesRefArrayBlock(values, positionCount, firstValueIndexes, nulls, mvOrdering);
    }

    public BytesRefVector.Builder newBytesRefVectorBuilder(int estimatedSize) {
        return new BytesRefVectorBuilder(estimatedSize);
    }

    public BytesRefVector newBytesRefArrayVector(BytesRefArray values, int positionCount) {
        return new BytesRefArrayVector(values, positionCount);
    }

    public BytesRefVector newConstantBytesRefVector(BytesRef value, int positions) {
        return new ConstantBytesRefVector(value, positions);
    }

    /** A builder the for {@link DocBlock}. */
    public DocBlock.Builder newDocBlockBuilder(int estimatedSize) {
        return new DocBlock.Builder(estimatedSize, this);
    }
}
