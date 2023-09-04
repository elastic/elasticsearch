/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.SumLongAggregatorFunction;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.is;

public class BlockSerializationTests extends SerializationTestCase {

    public void testConstantIntBlock() throws IOException {
        assertConstantBlockImpl(IntBlock.newConstantBlockWith(randomInt(), randomIntBetween(1, 8192)));
    }

    public void testConstantLongBlockLong() throws IOException {
        assertConstantBlockImpl(LongBlock.newConstantBlockWith(randomLong(), randomIntBetween(1, 8192)));
    }

    public void testConstantDoubleBlock() throws IOException {
        assertConstantBlockImpl(DoubleBlock.newConstantBlockWith(randomDouble(), randomIntBetween(1, 8192)));
    }

    public void testConstantBytesRefBlock() throws IOException {
        Block block = BytesRefBlock.newConstantBlockWith(new BytesRef(((Integer) randomInt()).toString()), randomIntBetween(1, 8192));
        assertConstantBlockImpl(block);
    }

    private void assertConstantBlockImpl(Block origBlock) throws IOException {
        assertThat(origBlock.asVector().isConstant(), is(true));
        Block deserBlock = serializeDeserializeBlock(origBlock);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(origBlock, unused -> deserBlock);
        assertThat(deserBlock.asVector().isConstant(), is(true));
    }

    public void testEmptyIntBlock() {
        assertEmptyBlock(IntBlock.newBlockBuilder(0).build());
        assertEmptyBlock(IntBlock.newBlockBuilder(0).appendNull().build().filter());
        assertEmptyBlock(IntVector.newVectorBuilder(0).build().asBlock());
        assertEmptyBlock(IntVector.newVectorBuilder(0).appendInt(randomInt()).build().filter().asBlock());
    }

    public void testEmptyLongBlock() {
        assertEmptyBlock(LongBlock.newBlockBuilder(0).build());
        assertEmptyBlock(LongBlock.newBlockBuilder(0).appendNull().build().filter());
        assertEmptyBlock(LongVector.newVectorBuilder(0).build().asBlock());
        assertEmptyBlock(LongVector.newVectorBuilder(0).appendLong(randomLong()).build().filter().asBlock());
    }

    public void testEmptyDoubleBlock() {
        assertEmptyBlock(DoubleBlock.newBlockBuilder(0).build());
        assertEmptyBlock(DoubleBlock.newBlockBuilder(0).appendNull().build().filter());
        assertEmptyBlock(DoubleVector.newVectorBuilder(0).build().asBlock());
        assertEmptyBlock(DoubleVector.newVectorBuilder(0).appendDouble(randomDouble()).build().filter().asBlock());
    }

    public void testEmptyBytesRefBlock() {
        assertEmptyBlock(BytesRefBlock.newBlockBuilder(0).build());
        assertEmptyBlock(BytesRefBlock.newBlockBuilder(0).appendNull().build().filter());
        assertEmptyBlock(BytesRefVector.newVectorBuilder(0).build().asBlock());
        assertEmptyBlock(BytesRefVector.newVectorBuilder(0).appendBytesRef(randomBytesRef()).build().filter().asBlock());
    }

    private void assertEmptyBlock(Block origBlock) {
        assertThat(origBlock.getPositionCount(), is(0));
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(origBlock, block -> serializeDeserializeBlock(block));
    }

    public void testFilterIntBlock() throws IOException {
        assertFilterBlock(IntBlock.newBlockBuilder(0).appendInt(1).appendInt(2).build().filter(1));
        assertFilterBlock(IntBlock.newBlockBuilder(1).appendInt(randomInt()).appendNull().build().filter(0));
        assertFilterBlock(IntVector.newVectorBuilder(1).appendInt(randomInt()).build().filter(0).asBlock());
        assertFilterBlock(IntVector.newVectorBuilder(1).appendInt(randomInt()).appendInt(randomInt()).build().filter(0).asBlock());
    }

    public void testFilterLongBlock() throws IOException {
        assertFilterBlock(LongBlock.newBlockBuilder(0).appendLong(1).appendLong(2).build().filter(1));
        assertFilterBlock(LongBlock.newBlockBuilder(1).appendLong(randomLong()).appendNull().build().filter(0));
        assertFilterBlock(LongVector.newVectorBuilder(1).appendLong(randomLong()).build().filter(0).asBlock());
        assertFilterBlock(LongVector.newVectorBuilder(1).appendLong(randomLong()).appendLong(randomLong()).build().filter(0).asBlock());
    }

    public void testFilterDoubleBlock() throws IOException {
        assertFilterBlock(DoubleBlock.newBlockBuilder(0).appendDouble(1).appendDouble(2).build().filter(1));
        assertFilterBlock(DoubleBlock.newBlockBuilder(1).appendDouble(randomDouble()).appendNull().build().filter(0));
        assertFilterBlock(DoubleVector.newVectorBuilder(1).appendDouble(randomDouble()).build().filter(0).asBlock());
        assertFilterBlock(
            DoubleVector.newVectorBuilder(1).appendDouble(randomDouble()).appendDouble(randomDouble()).build().filter(0).asBlock()
        );
    }

    public void testFilterBytesRefBlock() throws IOException {
        assertFilterBlock(
            BytesRefBlock.newBlockBuilder(0)
                .appendBytesRef(randomBytesRef())
                .appendBytesRef(randomBytesRef())
                .build()
                .filter(randomIntBetween(0, 1))
        );
        assertFilterBlock(
            BytesRefBlock.newBlockBuilder(0).appendBytesRef(randomBytesRef()).appendNull().build().filter(randomIntBetween(0, 1))
        );
        assertFilterBlock(BytesRefVector.newVectorBuilder(0).appendBytesRef(randomBytesRef()).build().asBlock().filter(0));
        assertFilterBlock(
            BytesRefVector.newVectorBuilder(0)
                .appendBytesRef(randomBytesRef())
                .appendBytesRef(randomBytesRef())
                .build()
                .asBlock()
                .filter(randomIntBetween(0, 1))
        );
    }

    private void assertFilterBlock(Block origBlock) throws IOException {
        assertThat(origBlock.getPositionCount(), is(1));
        Block deserBlock = serializeDeserializeBlock(origBlock);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(origBlock, unused -> deserBlock);
        assertThat(deserBlock.getPositionCount(), is(1));
    }

    public void testConstantNullBlock() throws IOException {
        Block origBlock = new ConstantNullBlock(randomIntBetween(1, 8192));
        Block deserBlock = serializeDeserializeBlock(origBlock);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(origBlock, unused -> deserBlock);
    }

    // TODO: more types, grouping, etc...
    public void testAggregatorStateBlock() throws IOException {
        Page page = new Page(new LongArrayVector(new long[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }, 10).asBlock());
        var bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        var params = new Object[] {};
        var function = SumLongAggregatorFunction.create(List.of(0));
        function.addRawInput(page);
        Block[] blocks = new Block[function.intermediateBlockCount()];
        function.evaluateIntermediate(blocks, 0);

        Block[] deserBlocks = Arrays.stream(blocks).map(this::uncheckedSerializeDeserializeBlock).toArray(Block[]::new);
        IntStream.range(0, blocks.length).forEach(i -> EqualsHashCodeTestUtils.checkEqualsAndHashCode(blocks[i], unused -> deserBlocks[i]));

        var inputChannels = IntStream.range(0, SumLongAggregatorFunction.intermediateStateDesc().size()).boxed().toList();
        var finalAggregator = SumLongAggregatorFunction.create(inputChannels);
        finalAggregator.addIntermediateInput(new Page(deserBlocks));
        Block[] finalBlocks = new Block[1];
        finalAggregator.evaluateFinal(finalBlocks, 0);
        var finalBlock = (LongBlock) finalBlocks[0];
        assertThat(finalBlock.getLong(0), is(55L));
    }

    static BytesRef randomBytesRef() {
        return new BytesRef(randomAlphaOfLengthBetween(0, 10));
    }
}
