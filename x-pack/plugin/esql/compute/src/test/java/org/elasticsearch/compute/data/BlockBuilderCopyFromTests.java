/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.elasticsearch.compute.data.BlockValueAsserter.assertBlockValues;
import static org.elasticsearch.compute.test.BlockTestUtils.valuesAtPositions;

public class BlockBuilderCopyFromTests extends ESTestCase {
    @ParametersFactory
    public static List<Object[]> params() {
        List<Object[]> params = new ArrayList<>();
        for (ElementType e : ElementType.values()) {
            if (e == ElementType.UNKNOWN
                || e == ElementType.NULL
                || e == ElementType.DOC
                || e == ElementType.COMPOSITE
                || e == ElementType.AGGREGATE_METRIC_DOUBLE) {
                continue;
            }
            for (boolean nullAllowed : new boolean[] { false, true }) {
                for (int[] valuesPerPosition : new int[][] { new int[] { 1, 1 }, new int[] { 1, 10 } }) {  // TODO 0
                    params.add(new Object[] { e, nullAllowed, valuesPerPosition[0], valuesPerPosition[1] });
                }
            }
        }
        return params;
    }

    private final ElementType elementType;
    private final boolean nullAllowed;
    private final int minValuesPerPosition;
    private final int maxValuesPerPosition;

    public BlockBuilderCopyFromTests(
        @Name("elementType") ElementType elementType,
        @Name("nullAllowed") boolean nullAllowed,
        @Name("minValuesPerPosition") int minValuesPerPosition,
        @Name("maxValuesPerPosition") int maxValuesPerPosition
    ) {
        this.elementType = elementType;
        this.nullAllowed = nullAllowed;
        this.minValuesPerPosition = minValuesPerPosition;
        this.maxValuesPerPosition = maxValuesPerPosition;
    }

    public void testSmall() {
        assertSmall(randomBlock());
    }

    public void testEvens() {
        assertEvens(randomBlock());
    }

    public void testSmallFiltered() {
        assertSmall(randomFilteredBlock());
    }

    public void testEvensFiltered() {
        assertEvens(randomFilteredBlock());
    }

    public void testSmallAllNull() {
        assertSmall(TestBlockFactory.getNonBreakingInstance().newConstantNullBlock(10));
    }

    public void testEvensAllNull() {
        assertEvens(TestBlockFactory.getNonBreakingInstance().newConstantNullBlock(10));
    }

    private void assertSmall(Block block) {
        int smallSize = Math.min(block.getPositionCount(), 10);
        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
        Block.Builder builder = elementType.newBlockBuilder(smallSize, blockFactory);
        builder.copyFrom(block, 0, smallSize);
        assertBlockValues(builder.build(), valuesAtPositions(block, 0, smallSize));
    }

    private void assertEvens(Block block) {
        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
        Block.Builder builder = elementType.newBlockBuilder(block.getPositionCount() / 2, blockFactory);
        List<List<Object>> expected = new ArrayList<>();
        for (int i = 0; i < block.getPositionCount(); i += 2) {
            switch (elementType) {
                case BOOLEAN -> ((BooleanBlockBuilder) builder).copyFrom((BooleanBlock) block, i);
                case BYTES_REF -> ((BytesRefBlockBuilder) builder).copyFrom((BytesRefBlock) block, i, new BytesRef());
                case DOUBLE -> ((DoubleBlockBuilder) builder).copyFrom((DoubleBlock) block, i);
                case FLOAT -> ((FloatBlockBuilder) builder).copyFrom((FloatBlock) block, i);
                case INT -> ((IntBlockBuilder) builder).copyFrom((IntBlock) block, i);
                case LONG -> ((LongBlockBuilder) builder).copyFrom((LongBlock) block, i);
                default -> throw new IllegalArgumentException("unsupported type: " + elementType);
            }

            expected.add(valuesAtPositions(block, i, i + 1).get(0));
        }
        assertBlockValues(builder.build(), expected);
    }

    private Block randomBlock() {
        int positionCount = randomIntBetween(1, 16 * 1024);
        return RandomBlock.randomBlock(elementType, positionCount, nullAllowed, minValuesPerPosition, maxValuesPerPosition, 0, 0).block();
    }

    private Block randomFilteredBlock() {
        int keepers = between(0, 4);
        Block orig = randomBlock();
        return orig.filter(IntStream.range(0, orig.getPositionCount()).filter(i -> i % 5 == keepers).toArray());
    }
}
