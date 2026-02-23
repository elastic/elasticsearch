/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.operator.blocksource.AbstractBlockSourceOperator;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.util.BitSet;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class MMROperatorTests extends OperatorTestCase {

    public static double NULL_BLOCK_CHANCE = 0.05;

    static List<float[]> TEST_VECTORS = List.of(
        new float[] { 0.4f, 0.2f, 0.4f, 0.4f },
        new float[] { 0.4f, 0.2f, 0.3f, 0.3f },
        new float[] { 0.4f, 0.1f, 0.3f, 0.3f },
        new float[] { 0.1f, 0.9f, 0.5f, 0.9f },
        new float[] { 0.1f, 0.9f, 0.5f, 0.9f },
        new float[] { 0.05f, 0.05f, 0.05f, 0.05f }
    );

    private int testLimitValue = 5;

    @Before
    public void beforeTest() {
        testLimitValue = randomIntBetween(3, 8);
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new AbstractBlockSourceOperator(blockFactory, 8192) {

            @Override
            protected int remaining() {
                return size - currentPosition;
            }

            @Override
            protected Page createPage(int positionOffset, int length) {
                length = Integer.min(length, remaining());
                var blocks = new Block[1];

                if (randomDouble() < NULL_BLOCK_CHANCE) {
                    blocks[0] = blockFactory.newConstantNullBlock(length);
                    return new Page(blocks);
                }

                float[] vectors = new float[length * 4];
                int[] vectorPositions = new int[length + 1];
                int vectorIndex = 0;
                try {
                    for (int i = 1; i <= length; i++) {
                        var thisVector = TEST_VECTORS.get(randomIntBetween(0, TEST_VECTORS.size() - 1));
                        for (int v = 0; v < 4; v++) {
                            vectors[vectorIndex + v] = thisVector[v];
                        }
                        vectorPositions[i - 1] = vectorIndex;
                        vectorIndex += 4;
                    }
                    vectorPositions[length] = vectorIndex;

                    blocks[0] = blockFactory.newFloatArrayBlock(vectors, length, vectorPositions, new BitSet(), Block.MvOrdering.UNORDERED);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {}

                finish();
                return new Page(blocks);
            }
        };
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        assertEquals(1, input.size());

        int expectedLimit = Math.min(input.getFirst().getPositionCount(), testLimitValue);

        var firstResultPage = results.getFirst();
        assertEquals(expectedLimit, firstResultPage.getPositionCount());

        assertEquals(1, firstResultPage.getBlockCount());

        assertThat(firstResultPage.getBlock(0).elementType(), equalTo(ElementType.FLOAT));
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new MMROperator.Factory("vector_field", 0, testLimitValue, null, null);
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        String description = "MMROperator[diversificationField="
            + "vector_field"
            + " (channel="
            + 0
            + "), limit="
            + testLimitValue
            + ", queryVector="
            + "null"
            + ", lambda="
            + "null"
            + "]";
        return equalTo(description);
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return expectedDescriptionOfSimple();
    }
}
