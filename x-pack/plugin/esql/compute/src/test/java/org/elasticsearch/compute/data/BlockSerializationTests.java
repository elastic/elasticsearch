/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.aggregation.SumLongAggregatorFunction;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.compute.test.TestBlockFactory;
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

    public void testConstantIntBlock() throws IOException {
        assertConstantBlockImpl(blockFactory.newConstantIntBlockWith(randomInt(), randomIntBetween(1, 8192)));
    }

    public void testConstantLongBlockLong() throws IOException {
        assertConstantBlockImpl(blockFactory.newConstantLongBlockWith(randomLong(), randomIntBetween(1, 8192)));
    }

    public void testConstantFloatBlock() throws IOException {
        assertConstantBlockImpl(blockFactory.newConstantFloatBlockWith(randomFloat(), randomIntBetween(1, 8192)));
    }

    public void testConstantDoubleBlock() throws IOException {
        assertConstantBlockImpl(blockFactory.newConstantDoubleBlockWith(randomDouble(), randomIntBetween(1, 8192)));
    }

    public void testConstantBytesRefBlock() throws IOException {
        Block block = blockFactory.newConstantBytesRefBlockWith(
            new BytesRef(((Integer) randomInt()).toString()),
            randomIntBetween(1, 8192)
        );
        assertConstantBlockImpl(block);
    }

    private void assertConstantBlockImpl(Block origBlock) throws IOException {
        assertThat(origBlock.asVector().isConstant(), is(true));
        try (origBlock; Block deserBlock = serializeDeserializeBlock(origBlock)) {
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(origBlock, unused -> deserBlock);
            assertThat(deserBlock.asVector().isConstant(), is(true));
        }
    }

    public void testEmptyIntBlock() throws IOException {
        assertEmptyBlock(blockFactory.newIntBlockBuilder(0).build());
        try (IntBlock toFilter = blockFactory.newIntBlockBuilder(0).appendNull().build()) {
            assertEmptyBlock(toFilter.filter());
        }
        assertEmptyBlock(blockFactory.newIntVectorBuilder(0).build().asBlock());
        try (IntVector toFilter = blockFactory.newIntVectorBuilder(0).appendInt(randomInt()).build()) {
            assertEmptyBlock(toFilter.filter().asBlock());
        }
    }

    public void testEmptyLongBlock() throws IOException {
        assertEmptyBlock(blockFactory.newLongBlockBuilder(0).build());
        try (LongBlock toFilter = blockFactory.newLongBlockBuilder(0).appendNull().build()) {
            assertEmptyBlock(toFilter.filter());
        }
        assertEmptyBlock(blockFactory.newLongVectorBuilder(0).build().asBlock());
        try (LongVector toFilter = blockFactory.newLongVectorBuilder(0).appendLong(randomLong()).build()) {
            assertEmptyBlock(toFilter.filter().asBlock());
        }
    }

    public void testEmptyFloatBlock() throws IOException {
        assertEmptyBlock(blockFactory.newFloatBlockBuilder(0).build());
        try (FloatBlock toFilter = blockFactory.newFloatBlockBuilder(0).appendNull().build()) {
            assertEmptyBlock(toFilter.filter());
        }
        assertEmptyBlock(blockFactory.newFloatVectorBuilder(0).build().asBlock());
        try (FloatVector toFilter = blockFactory.newFloatVectorBuilder(0).appendFloat(randomFloat()).build()) {
            assertEmptyBlock(toFilter.filter().asBlock());
        }
    }

    public void testEmptyDoubleBlock() throws IOException {
        assertEmptyBlock(blockFactory.newDoubleBlockBuilder(0).build());
        try (DoubleBlock toFilter = blockFactory.newDoubleBlockBuilder(0).appendNull().build()) {
            assertEmptyBlock(toFilter.filter());
        }
        assertEmptyBlock(blockFactory.newDoubleVectorBuilder(0).build().asBlock());
        try (DoubleVector toFilter = blockFactory.newDoubleVectorBuilder(0).appendDouble(randomDouble()).build()) {
            assertEmptyBlock(toFilter.filter().asBlock());
        }
    }

    public void testEmptyBytesRefBlock() throws IOException {
        assertEmptyBlock(blockFactory.newBytesRefBlockBuilder(0).build());
        try (BytesRefBlock toFilter = blockFactory.newBytesRefBlockBuilder(0).appendNull().build()) {
            assertEmptyBlock(toFilter.filter());
        }
        assertEmptyBlock(blockFactory.newBytesRefVectorBuilder(0).build().asBlock());
        try (BytesRefVector toFilter = blockFactory.newBytesRefVectorBuilder(0).appendBytesRef(randomBytesRef()).build()) {
            assertEmptyBlock(toFilter.filter().asBlock());
        }
    }

    private void assertEmptyBlock(Block origBlock) throws IOException {
        assertThat(origBlock.getPositionCount(), is(0));
        try (origBlock; Block deserBlock = serializeDeserializeBlock(origBlock)) {
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(origBlock, unused -> deserBlock);
        }
    }

    public void testFilterIntBlock() throws IOException {
        try (IntBlock toFilter = blockFactory.newIntBlockBuilder(0).appendInt(1).appendInt(2).build()) {
            assertFilterBlock(toFilter.filter(1));
        }
        try (IntBlock toFilter = blockFactory.newIntBlockBuilder(1).appendInt(randomInt()).appendNull().build()) {
            assertFilterBlock(toFilter.filter(0));
        }
        try (IntVector toFilter = blockFactory.newIntVectorBuilder(1).appendInt(randomInt()).build()) {
            assertFilterBlock(toFilter.filter(0).asBlock());
        }
        try (IntVector toFilter = blockFactory.newIntVectorBuilder(1).appendInt(randomInt()).appendInt(randomInt()).build()) {
            assertFilterBlock(toFilter.filter(0).asBlock());
        }
    }

    public void testFilterLongBlock() throws IOException {
        try (LongBlock toFilter = blockFactory.newLongBlockBuilder(0).appendLong(1).appendLong(2).build()) {
            assertFilterBlock(toFilter.filter(1));
        }
        try (LongBlock toFilter = blockFactory.newLongBlockBuilder(1).appendLong(randomLong()).appendNull().build()) {
            assertFilterBlock(toFilter.filter(0));
        }
        try (LongVector toFilter = blockFactory.newLongVectorBuilder(1).appendLong(randomLong()).build()) {
            assertFilterBlock(toFilter.filter(0).asBlock());
        }
        try (LongVector toFilter = blockFactory.newLongVectorBuilder(1).appendLong(randomLong()).appendLong(randomLong()).build()) {
            assertFilterBlock(toFilter.filter(0).asBlock());
        }
    }

    public void testFilterFloatBlock() throws IOException {
        try (FloatBlock toFilter = blockFactory.newFloatBlockBuilder(0).appendFloat(1).appendFloat(2).build()) {
            assertFilterBlock(toFilter.filter(1));
        }
        try (FloatBlock toFilter = blockFactory.newFloatBlockBuilder(1).appendFloat(randomFloat()).appendNull().build()) {
            assertFilterBlock(toFilter.filter(0));
        }
        try (FloatVector toFilter = blockFactory.newFloatVectorBuilder(1).appendFloat(randomFloat()).build()) {
            assertFilterBlock(toFilter.filter(0).asBlock());

        }
        try (FloatVector toFilter = blockFactory.newFloatVectorBuilder(1).appendFloat(randomFloat()).appendFloat(randomFloat()).build()) {
            assertFilterBlock(toFilter.filter(0).asBlock());
        }
    }

    public void testFilterDoubleBlock() throws IOException {
        try (DoubleBlock toFilter = blockFactory.newDoubleBlockBuilder(0).appendDouble(1).appendDouble(2).build()) {
            assertFilterBlock(toFilter.filter(1));
        }
        try (DoubleBlock toFilter = blockFactory.newDoubleBlockBuilder(1).appendDouble(randomDouble()).appendNull().build()) {
            assertFilterBlock(toFilter.filter(0));
        }
        try (DoubleVector toFilter = blockFactory.newDoubleVectorBuilder(1).appendDouble(randomDouble()).build()) {
            assertFilterBlock(toFilter.filter(0).asBlock());

        }
        try (
            DoubleVector toFilter = blockFactory.newDoubleVectorBuilder(1).appendDouble(randomDouble()).appendDouble(randomDouble()).build()
        ) {
            assertFilterBlock(toFilter.filter(0).asBlock());
        }
    }

    public void testFilterBytesRefBlock() throws IOException {
        try (
            BytesRefBlock toFilter = blockFactory.newBytesRefBlockBuilder(0)
                .appendBytesRef(randomBytesRef())
                .appendBytesRef(randomBytesRef())
                .build()
        ) {
            assertFilterBlock(toFilter.filter(randomIntBetween(0, 1)));
        }

        try (BytesRefBlock toFilter = blockFactory.newBytesRefBlockBuilder(0).appendBytesRef(randomBytesRef()).appendNull().build()) {
            assertFilterBlock(toFilter.filter(randomIntBetween(0, 1)));
        }

        try (BytesRefVector toFilter = blockFactory.newBytesRefVectorBuilder(0).appendBytesRef(randomBytesRef()).build()) {
            assertFilterBlock(toFilter.asBlock().filter(0));
        }
        try (
            BytesRefVector toFilter = blockFactory.newBytesRefVectorBuilder(0)
                .appendBytesRef(randomBytesRef())
                .appendBytesRef(randomBytesRef())
                .build()
        ) {
            assertFilterBlock(toFilter.asBlock().filter(randomIntBetween(0, 1)));
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
        var function = SumLongAggregatorFunction.create(driverCtx, List.of(0));
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
                try (var finalAggregator = SumLongAggregatorFunction.create(driverCtx, inputChannels)) {
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

    public void testOrdinalVector() throws Exception {
        int numValues = randomIntBetween(1, 1000);
        BlockFactory blockFactory = driverContext().blockFactory();
        BytesRef scratch = new BytesRef();
        try (
            BytesRefVector.Builder regular = blockFactory.newBytesRefVectorBuilder(between(1, numValues * 3));
            BytesRefHash hash = new BytesRefHash(1, blockFactory.bigArrays());
            IntVector.Builder ordinals = blockFactory.newIntVectorBuilder(between(1, numValues * 3));
            BytesRefVector.Builder dictionary = blockFactory.newBytesRefVectorBuilder(between(1, numValues * 3));
        ) {
            BytesRef v = new BytesRef("value-" + randomIntBetween(1, 20));
            int ord = Math.toIntExact(hash.add(v));
            ord = ord < 0 ? -1 - ord : ord;
            ordinals.appendInt(ord);
            regular.appendBytesRef(v);
            for (long l = 0; l < hash.size(); l++) {
                dictionary.appendBytesRef(hash.get(l, scratch));
            }
            try (BytesRefVector v1 = regular.build(); BytesRefVector v2 = new OrdinalBytesRefVector(ordinals.build(), dictionary.build())) {
                BytesRefVector.equals(v1, v2);
                for (BytesRefVector vector : List.of(v1, v2)) {
                    try (BytesRefBlock deserBlock = serializeDeserializeBlock(vector.asBlock())) {
                        EqualsHashCodeTestUtils.checkEqualsAndHashCode(deserBlock, unused -> deserBlock);
                    }
                }
                for (int p = 0; p < v1.getPositionCount(); p++) {
                    try (BytesRefVector f1 = v1.filter(p); BytesRefVector f2 = v2.filter(p)) {
                        BytesRefVector.equals(f1, f2);
                        for (BytesRefVector vector : List.of(f1, f2)) {
                            try (BytesRefBlock deserBlock = serializeDeserializeBlock(vector.asBlock())) {
                                EqualsHashCodeTestUtils.checkEqualsAndHashCode(deserBlock, unused -> deserBlock);
                            }
                        }
                    }
                }
            }
        }
    }

    public void testOrdinalBlock() throws Exception {
        int numValues = randomIntBetween(1, 1000);
        BlockFactory blockFactory = driverContext().blockFactory();
        BytesRef scratch = new BytesRef();
        try (
            BytesRefBlock.Builder regular = blockFactory.newBytesRefBlockBuilder(between(1, numValues * 3));
            BytesRefHash hash = new BytesRefHash(1, blockFactory.bigArrays());
            IntBlock.Builder ordinals = blockFactory.newIntBlockBuilder(between(1, numValues * 3));
            BytesRefVector.Builder dictionary = blockFactory.newBytesRefVectorBuilder(between(1, numValues * 3));
        ) {
            int valueCount = randomIntBetween(0, 3);
            if (valueCount == 0) {
                regular.appendNull();
                ordinals.appendNull();
            }
            if (valueCount > 1) {
                regular.beginPositionEntry();
                ordinals.beginPositionEntry();
            }
            for (int v = 0; v < valueCount; v++) {
                BytesRef bytes = new BytesRef("value-" + randomIntBetween(1, 20));
                int ord = Math.toIntExact(hash.add(bytes));
                ord = ord < 0 ? -1 - ord : ord;
                ordinals.appendInt(ord);
                regular.appendBytesRef(bytes);
            }
            if (valueCount > 1) {
                regular.endPositionEntry();
                ordinals.endPositionEntry();
            }
            for (long l = 0; l < hash.size(); l++) {
                dictionary.appendBytesRef(hash.get(l, scratch));
            }
            try (BytesRefBlock b1 = regular.build(); BytesRefBlock b2 = new OrdinalBytesRefBlock(ordinals.build(), dictionary.build())) {
                BytesRefBlock.equals(b1, b2);
                for (BytesRefBlock block : List.of(b1, b2)) {
                    try (BytesRefBlock deserBlock = serializeDeserializeBlock(block)) {
                        EqualsHashCodeTestUtils.checkEqualsAndHashCode(deserBlock, unused -> deserBlock);
                    }
                }
                for (int p = 0; p < b1.getPositionCount(); p++) {
                    try (BytesRefBlock f1 = b1.filter(p); BytesRefBlock f2 = b2.filter(p)) {
                        BytesRefBlock.equals(f1, f2);
                        for (BytesRefBlock block : List.of(f1, f2)) {
                            try (BytesRefBlock deserBlock = serializeDeserializeBlock(block)) {
                                EqualsHashCodeTestUtils.checkEqualsAndHashCode(deserBlock, unused -> deserBlock);
                            }
                        }
                    }
                }
                try (BytesRefBlock e1 = b1.expand(); BytesRefBlock e2 = b2.expand()) {
                    BytesRefBlock.equals(e1, e2);
                    for (BytesRefBlock block : List.of(e1, e2)) {
                        try (BytesRefBlock deserBlock = serializeDeserializeBlock(block)) {
                            EqualsHashCodeTestUtils.checkEqualsAndHashCode(deserBlock, unused -> deserBlock);
                        }
                    }
                }
            }
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
                    TransportVersionUtils.randomVersionBetween(
                        random(),
                        TransportVersions.AGGREGATE_METRIC_DOUBLE_BLOCK,
                        TransportVersion.current()
                    )
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
        return new DriverContext(nonBreakingBigArrays(), TestBlockFactory.getNonBreakingInstance());
    }
}
