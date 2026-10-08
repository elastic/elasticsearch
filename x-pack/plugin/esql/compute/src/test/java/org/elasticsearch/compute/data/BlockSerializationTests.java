/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.aggregation.SumLongAggregatorFunction;
import org.elasticsearch.compute.aggregation.SumLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.compute.test.TestWarningsSource;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class BlockSerializationTests extends SerializationTestCase {

    public void testEmptyAggregateMetricDoubleBlock() throws IOException {
        assertEmptyBlock(blockFactory.newAggregateMetricDoubleBlockBuilder(0).build());
        try (AggregateMetricDoubleBlock toFilter = blockFactory.newAggregateMetricDoubleBlockBuilder(0).appendNull().build()) {
            assertEmptyBlock(toFilter.filter(false));
        }
    }

    private void assertEmptyBlock(Block origBlock) throws IOException {
        assertThat(origBlock.getPositionCount(), is(0));
        try (origBlock; Block deserBlock = serializeDeserializeBlock(origBlock)) {
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(origBlock, unused -> deserBlock);
        }
    }

    public void testFilterAggregateMetricDoubleBlock() throws IOException {
        {
            var builder = blockFactory.newAggregateMetricDoubleBlockBuilder(0);
            builder.min().appendDouble(randomDouble());
            builder.max().appendDouble(randomDouble());
            builder.sum().appendDouble(randomDouble());
            builder.count().appendInt(randomInt());
            builder.min().appendDouble(randomDouble());
            builder.max().appendDouble(randomDouble());
            builder.sum().appendDouble(randomDouble());
            builder.count().appendInt(randomInt());
            try (AggregateMetricDoubleBlock toFilter = builder.build()) {
                assertFilterBlock(toFilter.filter(false, randomIntBetween(0, 1)));
            }
        }

        {
            var builder = blockFactory.newAggregateMetricDoubleBlockBuilder(0);
            builder.min().appendDouble(randomDouble());
            builder.max().appendDouble(randomDouble());
            builder.sum().appendDouble(randomDouble());
            builder.count().appendInt(randomInt());
            builder.appendNull();
            try (AggregateMetricDoubleBlock toFilter = builder.build()) {
                assertFilterBlock(toFilter.filter(false, randomIntBetween(0, 1)));
            }
        }
    }

    private void assertFilterBlock(Block origBlock) throws IOException {
        assertThat(origBlock.getPositionCount(), is(1));
        try (origBlock; Block deserBlock = serializeDeserializeBlock(origBlock)) {
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(origBlock, unused -> deserBlock);
            assertThat(deserBlock.getPositionCount(), is(1));
        }
    }

    public void testConstantNullBlock() throws IOException {
        try (Block origBlock = blockFactory.newConstantNullBlock(randomIntBetween(1, 8192))) {
            try (Block deserBlock = serializeDeserializeBlock(origBlock)) {
                EqualsHashCodeTestUtils.checkEqualsAndHashCode(origBlock, unused -> deserBlock);
            }
        }
    }

    // TODO: more types, grouping, etc...
    public void testSimulateAggs() {
        DriverContext driverCtx = driverContext();
        Page page = new Page(blockFactory.newLongArrayVector(new long[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }, 10).asBlock());
        var function = new SumLongAggregatorFunctionSupplier(TestWarningsSource.INSTANCE).aggregator(driverCtx, List.of(0));
        try (BooleanVector noMasking = driverContext().blockFactory().newConstantBooleanVector(true, page.getPositionCount())) {
            function.addRawInput(page, noMasking);
        }
        Block[] blocks = new Block[function.intermediateBlockCount()];
        try {
            function.evaluateIntermediate(blocks, 0, driverCtx);

            Block[] deserBlocks = Arrays.stream(blocks).map(this::uncheckedSerializeDeserializeBlock).toArray(Block[]::new);
            try {
                IntStream.range(0, blocks.length)
                    .forEach(i -> EqualsHashCodeTestUtils.checkEqualsAndHashCode(blocks[i], unused -> deserBlocks[i]));

                var inputChannels = IntStream.range(0, SumLongAggregatorFunction.intermediateStateDesc().size()).boxed().toList();
                try (
                    var finalAggregator = new SumLongAggregatorFunctionSupplier(TestWarningsSource.INSTANCE).aggregator(
                        driverCtx,
                        inputChannels
                    )
                ) {
                    finalAggregator.addIntermediateInput(new Page(deserBlocks));
                    Block[] finalBlocks = new Block[1];
                    finalAggregator.evaluateFinal(finalBlocks, 0, driverCtx);
                    try (var finalBlock = (LongBlock) finalBlocks[0]) {
                        assertThat(finalBlock.getLong(0), is(55L));
                    }
                }
            } finally {
                Releasables.close(deserBlocks);
            }
        } finally {
            Releasables.close(blocks);
            page.releaseBlocks();
        }
    }

    public void testCompositeBlock() throws Exception {
        final int numBlocks = randomIntBetween(1, 10);
        final int positionCount = randomIntBetween(1, 1000);
        final Block[] blocks = new Block[numBlocks];
        for (int b = 0; b < numBlocks; b++) {
            ElementType elementType = randomFrom(ElementType.LONG, ElementType.DOUBLE, ElementType.BOOLEAN, ElementType.NULL);
            blocks[b] = RandomBlock.randomBlock(blockFactory, elementType, positionCount, true, 0, between(1, 2), 0, between(1, 2)).block();
        }
        try (CompositeBlock origBlock = new CompositeBlock(blocks)) {
            assertThat(origBlock.getBlockCount(), equalTo(numBlocks));
            for (int b = 0; b < numBlocks; b++) {
                assertThat(origBlock.getBlock(b), equalTo(blocks[b]));
            }
            try (
                CompositeBlock deserBlock = serializeDeserializeBlockWithVersion(
                    origBlock,
                    TransportVersionUtils.randomVersionSupporting(Block.ESQL_AGGREGATE_METRIC_DOUBLE_BLOCK)
                )
            ) {
                assertThat(deserBlock.getBlockCount(), equalTo(numBlocks));
                for (int b = 0; b < numBlocks; b++) {
                    assertThat(deserBlock.getBlock(b), equalTo(origBlock.getBlock(b)));
                }
                EqualsHashCodeTestUtils.checkEqualsAndHashCode(deserBlock, unused -> deserBlock);
            }
        }
    }

    public void testAggregateMetricDouble() throws IOException {
        final int positionCount = randomIntBetween(1, 1000);
        DoubleBlock minBlock = (DoubleBlock) RandomBlock.randomBlock(
            blockFactory,
            randomFrom(ElementType.DOUBLE, ElementType.NULL),
            positionCount,
            true,
            0,
            1,
            0,
            0
        ).block();

        DoubleBlock maxBlock = (DoubleBlock) RandomBlock.randomBlock(
            blockFactory,
            randomFrom(ElementType.DOUBLE, ElementType.NULL),
            positionCount,
            true,
            0,
            1,
            0,
            0
        ).block();

        DoubleBlock suBlock = (DoubleBlock) RandomBlock.randomBlock(
            blockFactory,
            randomFrom(ElementType.DOUBLE, ElementType.NULL),
            positionCount,
            true,
            0,
            1,
            0,
            0
        ).block();

        IntBlock countBlock = (IntBlock) RandomBlock.randomBlock(
            blockFactory,
            randomFrom(ElementType.INT, ElementType.NULL),
            positionCount,
            true,
            0,
            1,
            0,
            0
        ).block();

        try (var origBlock = new AggregateMetricDoubleArrayBlock(minBlock, maxBlock, suBlock, countBlock)) {
            try (
                AggregateMetricDoubleBlock deserBlock = serializeDeserializeBlockWithVersion(
                    origBlock,
                    TransportVersionUtils.randomVersionSupporting(Block.ESQL_AGGREGATE_METRIC_DOUBLE_BLOCK)
                )
            ) {
                assertThat(deserBlock, equalTo(origBlock));
                EqualsHashCodeTestUtils.checkEqualsAndHashCode(deserBlock, unused -> deserBlock);
            }
        }
    }

    static BytesRef randomBytesRef() {
        return new BytesRef(randomAlphaOfLengthBetween(0, 10));
    }

    /**
     * A {@link BigArrays} that won't throw {@link CircuitBreakingException}.
     * <p>
     *     Rather than using the {@link NoneCircuitBreakerService} we use a
     *     very large limit so tests can call {@link CircuitBreaker#getUsed()}.
     * </p>
     */
    protected final BigArrays nonBreakingBigArrays() {
        return new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofBytes(Integer.MAX_VALUE)).withCircuitBreaking();
    }

    /**
     * A {@link DriverContext} with a nonBreakingBigArrays.
     */
    protected DriverContext driverContext() { // TODO make this final and return a breaking block factory
        return new DriverContext(nonBreakingBigArrays(), TestBlockFactory.getNonBreakingInstance(), null);
    }
}
