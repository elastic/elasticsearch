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
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
import org.elasticsearch.compute.test.TestWarningsSource;
import org.elasticsearch.compute.test.operator.blocksource.DenseVectorFloatBlockSourceOperator;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

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
        double[] expectedSum = new double[vectorDimensions];
        double[] absSum = new double[vectorDimensions];
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
                    absSum[i] += Math.abs(block.getFloat(start + i));
                }
            }
        }

        FloatBlock resultBlock = (FloatBlock) result;
        assertFalse(resultBlock.isNull(0));
        int valueCount = resultBlock.getValueCount(0);
        assertEquals(vectorDimensions, valueCount);
        int start = resultBlock.getFirstValueIndex(0);
        for (int i = 0; i < vectorDimensions; i++) {
            // Each partial sum is rounded to float once per aggregation stage, independent of the number of rows.
            double tolerance = absSum[i] * 1e-6;
            assertThat("Dimension " + i + " mismatch", (double) resultBlock.getFloat(start + i), closeTo(expectedSum[i], tolerance));
        }
    }

    public void testSumBeyondFloatIntegerPrecision() {
        // 2^24 + 1 is not a float, so a float running sum would stay at 2^24.
        int ones = between(100, 1000);
        Stream<float[]> vectors = Stream.concat(
            Stream.of(new float[] { 16777216f }),
            Stream.generate(() -> new float[] { 1f }).limit(ones)
        );
        DriverContext driverContext = driverContext();
        List<Page> results = new ArrayList<>();
        try (
            Driver d = TestDriverFactory.create(
                driverContext,
                new DenseVectorFloatBlockSourceOperator(driverContext.blockFactory(), vectors),
                List.of(simple().get(driverContext)),
                new TestResultPageSinkOperator(results::add)
            )
        ) {
            new TestDriverRunner().run(d);
        }
        assertThat(results.get(0).<FloatBlock>getBlock(0).getFloat(0), equalTo((float) (16777216d + ones)));
        assertDriverContext(driverContext);
    }
}
