/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.compute.test.TestBlockFactory;

import java.util.BitSet;

import static org.elasticsearch.test.ESTestCase.randomIntBetween;

public class BlockTypeRandomizer {
    private BlockTypeRandomizer() {}

    /**
     * Returns a block with the same contents, but with a randomized type (Constant, vector, big-array...).
     * <p>
     *     The new block uses a non-breaking block builder, and doesn't increment the circuit breaking.
     *     This is done to avoid randomly using more memory in tests that expect a deterministic memory usage.
     * </p>
     */
    public static IntBlock randomizeBlockType(IntBlock block) {
        // Just to track the randomization
        int classCount = 4;

        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();

        //
        // ConstantNullBlock. It requires all positions to be null
        //
        if (randomIntBetween(0, --classCount) == 0 && block.areAllValuesNull()) {
            if (block instanceof ConstantNullBlock) {
                return block;
            }
            return new ConstantNullBlock(block.getPositionCount(), blockFactory);
        }

        //
        // IntVectorBlock. It doesn't allow nulls or multivalues
        //
        if (randomIntBetween(0, --classCount) == 0 && block.doesHaveMultivaluedFields() == false && block.mayHaveNulls() == false) {
            if (block instanceof IntVectorBlock) {
                return block;
            }

            int[] values = new int[block.getPositionCount()];
            for (int i = 0; i < values.length; i++) {
                values[i] = block.getInt(i);
            }

            return new IntVectorBlock(new IntArrayVector(values, block.getPositionCount(), blockFactory));
        }

        // Both IntArrayBlock and IntBigArrayBlock need a nullsBitSet and a firstValueIndexes int[]
        int[] firstValueIndexes = new int[block.getPositionCount() + 1];
        BitSet nullsMask = new BitSet(block.getPositionCount());
        for (int i = 0; i < block.getPositionCount(); i++) {
            firstValueIndexes[i] = block.getFirstValueIndex(i);

            if (block.isNull(i)) {
                nullsMask.set(i);
            }
        }
        int totalValues = block.getFirstValueIndex(block.getPositionCount() - 1) + block.getValueCount(block.getPositionCount() - 1);
        firstValueIndexes[firstValueIndexes.length - 1] = totalValues;

        //
        // IntArrayBlock
        //
        if (randomIntBetween(0, --classCount) == 0) {
            if (block instanceof IntArrayBlock) {
                return block;
            }

            int[] values = new int[totalValues];
            for (int i = 0; i < values.length; i++) {
                values[i] = block.getInt(i);
            }

            return new IntArrayBlock(values, block.getPositionCount(), firstValueIndexes, nullsMask, block.mvOrdering(), blockFactory);
        }
        assert classCount == 1;

        //
        // IntBigArrayBlock
        //
        if (block instanceof IntBigArrayBlock) {
            return block;
        }

        var intArray = blockFactory.bigArrays().newIntArray(totalValues);
        for (int i = 0; i < block.getPositionCount(); i++) {
            intArray.set(i, block.getInt(i));
        }

        return new IntBigArrayBlock(intArray, block.getPositionCount(), firstValueIndexes, nullsMask, block.mvOrdering(), blockFactory);
    }
}
