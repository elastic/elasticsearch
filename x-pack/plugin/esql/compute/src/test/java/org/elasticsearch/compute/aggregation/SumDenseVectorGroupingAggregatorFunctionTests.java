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
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
import org.elasticsearch.compute.test.TestWarningsSource;
import org.elasticsearch.compute.test.operator.blocksource.LongDenseVectorFloatTupleBlockSourceOperator;
import org.elasticsearch.core.Tuple;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase.randomVector;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

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
        // Dense vector aggregation doesn't support multi-valued groups
        // Each vector position belongs to exactly one group
        return false;
    }

    @Override
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
        double[] expectedSum = new double[vectorDimensions];
        double[] absSum = new double[vectorDimensions];
        for (float[] vector : vectors) {
            for (int i = 0; i < vector.length; i++) {
                expectedSum[i] += vector[i];
                absSum[i] += Math.abs(vector[i]);
            }
        }

        // Assert result
        assertFalse(resultBlock.isNull(position));
        int valueCount = resultBlock.getValueCount(position);
        assertEquals(vectorDimensions, valueCount);

        int start = resultBlock.getFirstValueIndex(position);
        for (int i = 0; i < vectorDimensions; i++) {
            // Each partial sum is rounded to float once per aggregation stage, independent of the number of rows.
            double tolerance = absSum[i] * 1e-6;
            assertThat("Dimension " + i + " mismatch", (double) resultBlock.getFloat(start + i), closeTo(expectedSum[i], tolerance));
        }
    }

    public void testSumBeyondFloatIntegerPrecision() {
        // 2^24 + 1 is not a float, so a float running sum would stay at 2^24.
        int ones = between(100, 1000);
        Stream<Tuple<Long, float[]>> input = Stream.concat(
            Stream.of(Tuple.tuple(0L, new float[] { 16777216f })),
            Stream.generate(() -> Tuple.tuple(0L, new float[] { 1f })).limit(ones)
        );
        DriverContext driverContext = driverContext();
        List<Page> results = new ArrayList<>();
        try (
            Driver d = TestDriverFactory.create(
                driverContext,
                new LongDenseVectorFloatTupleBlockSourceOperator(driverContext.blockFactory(), input),
                List.of(simple().get(driverContext)),
                new TestResultPageSinkOperator(results::add)
            )
        ) {
            new TestDriverRunner().run(d);
        }
        assertThat(results.get(0).<FloatBlock>getBlock(1).getFloat(0), equalTo((float) (16777216d + ones)));
        assertDriverContext(driverContext);
    }

}
