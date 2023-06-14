/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.AvgLongAggregatorFunction;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.io.IOException;

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
    }

    public void testEmptyLongBlock() {
        assertEmptyBlock(LongBlock.newBlockBuilder(0).build());
    }

    public void testEmptyDoubleBlock() {
        assertEmptyBlock(DoubleBlock.newBlockBuilder(0).build());
    }

    public void testEmptyBytesRefBlock() {
        assertEmptyBlock(BytesRefBlock.newBlockBuilder(0).build());
    }

    private void assertEmptyBlock(Block origBlock) {
        assertThat(origBlock.getPositionCount(), is(0));
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(origBlock, block -> serializeDeserializeBlock(block));
    }

    public void testFilterIntBlock() throws IOException {
        assertFilterBlock(IntBlock.newBlockBuilder(0).appendInt(1).appendInt(2).build().filter(1));
    }

    public void testFilterLongBlock() throws IOException {
        assertFilterBlock(LongBlock.newBlockBuilder(0).appendLong(1).appendLong(2).build().filter(1));
    }

    public void testFilterDoubleBlock() throws IOException {
        assertFilterBlock(DoubleBlock.newBlockBuilder(0).appendDouble(1).appendDouble(2).build().filter(1));
    }

    public void testFilterBytesRefBlock() throws IOException {
        BytesRefBlock block = BytesRefBlock.newBlockBuilder(0)
            .appendBytesRef(new BytesRef("1"))
            .appendBytesRef(new BytesRef("2"))
            .build()
            .filter(1);
        assertFilterBlock(block);
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
        var function = AvgLongAggregatorFunction.create(0);
        function.addRawInput(page);
        Block origBlock = function.evaluateIntermediate();

        Block deserBlock = serializeDeserializeBlock(origBlock);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(origBlock, unused -> deserBlock);

        var finalAggregator = AvgLongAggregatorFunction.create(-1);
        finalAggregator.addIntermediateInput(deserBlock);
        DoubleBlock finalBlock = (DoubleBlock) finalAggregator.evaluateFinal();
        assertThat(finalBlock.getDouble(0), is(5.5));
    }
}
