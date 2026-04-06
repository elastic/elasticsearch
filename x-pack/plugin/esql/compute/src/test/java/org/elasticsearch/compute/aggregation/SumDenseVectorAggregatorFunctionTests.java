/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.TestWarningsSource;
import org.elasticsearch.compute.test.operator.blocksource.DenseVectorFloatBlockSourceOperator;
import org.junit.Before;

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.closeTo;

public class SumDenseVectorAggregatorFunctionTests extends AggregatorFunctionTestCase {

    private int vectorDimensions;

    @Before
    public void setup() {
        vectorDimensions = randomIntBetween(1, 32);
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new DenseVectorFloatBlockSourceOperator(
            blockFactory,
            LongStream.range(0, size).mapToObj(l -> randomVector(vectorDimensions))
        );
    }

    private static float[] randomVector(int dimensions) {
        float[] vector = new float[dimensions];
        for (int i = 0; i < dimensions; i++) {
            vector[i] = randomFloat();
        }
        return vector;
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new SumDenseVectorAggregatorFunctionSupplier(TestWarningsSource.INSTANCE);
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "sum of dense_vectors";
    }

    @Override
    protected boolean supportsMultiValues() {
        return false;
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, Block result) {
        float[] expectedSum = new float[vectorDimensions];
        for (Page page : input) {
            FloatBlock block = page.getBlock(0);
            for (int p = 0; p < block.getPositionCount(); p++) {
                if (block.isNull(p)) {
                    continue;
                }
                int dims = block.getValueCount(p);
                int start = block.getFirstValueIndex(p);
                for (int i = 0; i < dims; i++) {
                    expectedSum[i] += block.getFloat(start + i);
                }
            }
        }

        FloatBlock resultBlock = (FloatBlock) result;
        assertFalse(resultBlock.isNull(0));
        int valueCount = resultBlock.getValueCount(0);
        assertEquals(vectorDimensions, valueCount);
        int start = resultBlock.getFirstValueIndex(0);
        for (int i = 0; i < vectorDimensions; i++) {
            // Use a relative tolerance since float summation order changes across partitions
            double tolerance = Math.abs(expectedSum[i]) * 1e-3f;
            assertThat("Dimension " + i + " mismatch", (double) resultBlock.getFloat(start + i), closeTo(expectedSum[i], tolerance));
        }
    }
}
