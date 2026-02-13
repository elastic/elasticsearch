/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.operator.blocksource.LongDenseVectorFloatTupleBlockSourceOperator;
import org.elasticsearch.core.Tuple;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.closeTo;

public class SumDenseVectorGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {

    private int vectorDimensions;

    @Before
    public void setup() {
        vectorDimensions = randomIntBetween(1, 32);
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int end) {
        return new LongDenseVectorFloatTupleBlockSourceOperator(
            blockFactory,
            LongStream.range(0, end).mapToObj(l -> Tuple.tuple(randomLongBetween(0, 4), randomVector(vectorDimensions)))
        );
    }

    private float[] randomVector(int dimensions) {
        float[] vector = new float[dimensions];
        for (int i = 0; i < dimensions; i++) {
            vector[i] = randomFloat();
        }
        return vector;
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new SumDenseVectorAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "sum of dense_vectors";
    }

    @Override
    protected boolean supportsMultiValues() {
        // Dense vector aggregation doesn't support multi-valued groups
        // Each vector position belongs to exactly one group
        return false;
    }

    protected void appendNullGroupValue(ElementType elementType, Block.Builder builder, int blockId) {
        // Dense vectors are multi-valued float positions; the default single randomFloat()
        // would create a 1-dimensional vector that conflicts with the expected dimensions.
        // Instead, append a properly-dimensioned random vector.
        FloatBlock.Builder floatBuilder = (FloatBlock.Builder) builder;
        floatBuilder.beginPositionEntry();
        for (int i = 0; i < vectorDimensions; i++) {
            floatBuilder.appendFloat(randomFloat());
        }
        floatBuilder.endPositionEntry();
    }

    @Override
    protected void assertSimpleGroup(List<Page> input, Block result, int position, Long group) {
        List<float[]> vectors = allVectors(input, group);
        FloatBlock resultBlock = (FloatBlock) result;

        if (vectors.isEmpty()) {
            assertTrue(resultBlock.isNull(position));
            return;
        }

        // Compute expected sum
        float[] expectedSum = new float[vectorDimensions];
        for (float[] vector : vectors) {
            for (int i = 0; i < vector.length; i++) {
                expectedSum[i] += vector[i];
            }
        }

        // Assert result
        assertFalse(resultBlock.isNull(position));
        int valueCount = resultBlock.getValueCount(position);
        assertEquals(vectorDimensions, valueCount);

        int start = resultBlock.getFirstValueIndex(position);
        for (int i = 0; i < vectorDimensions; i++) {
            assertThat(
                "Dimension " + i + " mismatch",
                (double) resultBlock.getFloat(start + i),
                closeTo(expectedSum[i], Math.max(1.0, Math.abs(expectedSum[i]) * 0.001))
            );
        }
    }

    /**
     * Extracts all vectors for a given group from the input pages.
     */
    private List<float[]> allVectors(List<Page> input, Long group) {
        List<float[]> result = new ArrayList<>();
        for (Page page : input) {
            LongBlock groups = page.getBlock(0);
            FloatBlock values = page.getBlock(1);

            for (int p = 0; p < page.getPositionCount(); p++) {
                if (group == null) {
                    // Looking for positions with null group keys
                    if (groups.isNull(p) == false) {
                        continue;
                    }
                } else {
                    if (groups.isNull(p)) {
                        continue;
                    }
                    long g = groups.getLong(groups.getFirstValueIndex(p));
                    if (g != group) {
                        continue;
                    }
                }
                if (values.isNull(p)) {
                    continue;
                }

                int valueCount = values.getValueCount(p);
                float[] vector = new float[valueCount];
                int start = values.getFirstValueIndex(p);
                for (int i = 0; i < valueCount; i++) {
                    vector[i] = values.getFloat(start + i);
                }
                result.add(vector);
            }
        }
        return result;
    }
}
