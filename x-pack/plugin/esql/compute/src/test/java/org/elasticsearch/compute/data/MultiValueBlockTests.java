/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class MultiValueBlockTests extends SerializationTestCase {

    public void testIntBlockTrivial1() {
        var blockBuilder = IntBlock.newBlockBuilder(4);
        blockBuilder.appendInt(10);
        blockBuilder.beginPositionEntry();
        blockBuilder.appendInt(21);
        blockBuilder.appendInt(22);
        blockBuilder.appendInt(23);
        IntBlock block = blockBuilder.build();

        // expect two positions
        assertThat(block.getPositionCount(), is(2));

        // expect four values
        assertThat(block.getTotalValueCount(), is(4));

        // assert first position
        assertThat(block.getValueCount(0), is(1));
        assertThat(block.getFirstValueIndex(0), is(0));
        assertThat(block.getInt(block.getFirstValueIndex(0)), is(10));

        // assert second position
        assertThat(block.getValueCount(1), is(3));
        assertThat(block.getFirstValueIndex(1), is(1));
        int expectedValue = 21;
        for (int i = 0; i < block.getValueCount(1); i++) {
            assertThat(block.getInt(block.getFirstValueIndex(1) + i), is(expectedValue));
            expectedValue++;
        }

        // cannot get a Vector view
        assertNull(block.asVector());
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(block, b -> serializeDeserializeBlock(b));
    }

    public void testIntBlockTrivial() {
        var blockBuilder = IntBlock.newBlockBuilder(10);
        blockBuilder.appendInt(1);
        blockBuilder.beginPositionEntry();
        blockBuilder.appendInt(21);
        blockBuilder.appendInt(22);
        blockBuilder.appendInt(23);
        blockBuilder.endPositionEntry();
        blockBuilder.beginPositionEntry();
        blockBuilder.appendInt(31);
        blockBuilder.appendInt(32);
        blockBuilder.endPositionEntry();
        blockBuilder.beginPositionEntry();
        blockBuilder.appendInt(41);
        blockBuilder.endPositionEntry();
        IntBlock block = blockBuilder.build();

        assertThat(block.getPositionCount(), is(4));
        assertThat(block.getFirstValueIndex(0), is(0));
        assertThat(block.getValueCount(0), is(1));
        assertThat(block.getInt(block.getFirstValueIndex(0)), is(1));
        assertNull(block.asVector());
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(block, b -> serializeDeserializeBlock(b));
    }

    public void testEmpty() {
        for (int initialSize : new int[] { 0, 10, 100, randomInt(512) }) {
            IntBlock intBlock = IntBlock.newBlockBuilder(initialSize).build();
            assertThat(intBlock.getPositionCount(), is(0));
            assertThat(intBlock.asVector(), is(notNullValue()));
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(intBlock, block -> serializeDeserializeBlock(block));

            LongBlock longBlock = LongBlock.newBlockBuilder(initialSize).build();
            assertThat(longBlock.getPositionCount(), is(0));
            assertThat(longBlock.asVector(), is(notNullValue()));
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(longBlock, block -> serializeDeserializeBlock(block));

            DoubleBlock doubleBlock = DoubleBlock.newBlockBuilder(initialSize).build();
            assertThat(doubleBlock.getPositionCount(), is(0));
            assertThat(doubleBlock.asVector(), is(notNullValue()));
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(doubleBlock, block -> serializeDeserializeBlock(block));

            BytesRefBlock bytesRefBlock = BytesRefBlock.newBlockBuilder(initialSize).build();
            assertThat(bytesRefBlock.getPositionCount(), is(0));
            assertThat(bytesRefBlock.asVector(), is(notNullValue()));
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(bytesRefBlock, block -> serializeDeserializeBlock(block));
        }
    }

    public void testNullOnly() throws IOException {
        for (int initialSize : new int[] { 0, 10, 100, randomInt(512) }) {
            IntBlock intBlock = IntBlock.newBlockBuilder(initialSize).appendNull().build();
            assertThat(intBlock.getPositionCount(), is(1));
            assertThat(intBlock.getValueCount(0), is(0));
            assertNull(intBlock.asVector());
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(intBlock, block -> serializeDeserializeBlock(block));

            LongBlock longBlock = LongBlock.newBlockBuilder(initialSize).appendNull().build();
            assertThat(longBlock.getPositionCount(), is(1));
            assertThat(longBlock.getValueCount(0), is(0));
            assertNull(longBlock.asVector());
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(longBlock, block -> serializeDeserializeBlock(block));

            DoubleBlock doubleBlock = DoubleBlock.newBlockBuilder(initialSize).appendNull().build();
            assertThat(doubleBlock.getPositionCount(), is(1));
            assertThat(doubleBlock.getValueCount(0), is(0));
            assertNull(doubleBlock.asVector());
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(doubleBlock, block -> serializeDeserializeBlock(block));

            BytesRefBlock bytesRefBlock = BytesRefBlock.newBlockBuilder(initialSize).appendNull().build();
            assertThat(bytesRefBlock.getPositionCount(), is(1));
            assertThat(bytesRefBlock.getValueCount(0), is(0));
            assertNull(bytesRefBlock.asVector());
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(bytesRefBlock, block -> serializeDeserializeBlock(block));
        }
    }

    public void testNullsFollowedByValues() {
        List<List<Object>> blockValues = List.of(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(1),
            List.of(2)
        );

        Block intBlock = TestBlockBuilder.blockFromValues(blockValues, ElementType.INT);
        assertThat(intBlock.elementType(), is(equalTo(ElementType.INT)));
        BlockValueAsserter.assertBlockValues(intBlock, blockValues);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(intBlock, block -> serializeDeserializeBlock(block));

        Block longBlock = TestBlockBuilder.blockFromValues(blockValues, ElementType.LONG);
        assertThat(longBlock.elementType(), is(equalTo(ElementType.LONG)));
        BlockValueAsserter.assertBlockValues(longBlock, blockValues);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(intBlock, block -> serializeDeserializeBlock(block));

        Block doubleBlock = TestBlockBuilder.blockFromValues(blockValues, ElementType.DOUBLE);
        assertThat(doubleBlock.elementType(), is(equalTo(ElementType.DOUBLE)));
        BlockValueAsserter.assertBlockValues(doubleBlock, blockValues);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(intBlock, block -> serializeDeserializeBlock(block));

        Block bytesRefBlock = TestBlockBuilder.blockFromValues(blockValues, ElementType.BYTES_REF);
        assertThat(bytesRefBlock.elementType(), is(equalTo(ElementType.BYTES_REF)));
        BlockValueAsserter.assertBlockValues(bytesRefBlock, blockValues);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(intBlock, block -> serializeDeserializeBlock(block));
    }

    public void testMultiValuesAndNullsSmall() {
        List<List<Object>> blockValues = List.of(
            List.of(100),
            List.of(),
            List.of(20, 21, 22),
            List.of(),
            List.of(),
            List.of(50),
            List.of(61, 62, 63)
        );

        Block intBlock = TestBlockBuilder.blockFromValues(blockValues, ElementType.INT);
        assertThat(intBlock.elementType(), is(equalTo(ElementType.INT)));
        BlockValueAsserter.assertBlockValues(intBlock, blockValues);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(intBlock, block -> serializeDeserializeBlock(block));

        Block longBlock = TestBlockBuilder.blockFromValues(blockValues, ElementType.LONG);
        assertThat(longBlock.elementType(), is(equalTo(ElementType.LONG)));
        BlockValueAsserter.assertBlockValues(longBlock, blockValues);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(intBlock, block -> serializeDeserializeBlock(block));

        Block doubleBlock = TestBlockBuilder.blockFromValues(blockValues, ElementType.DOUBLE);
        assertThat(doubleBlock.elementType(), is(equalTo(ElementType.DOUBLE)));
        BlockValueAsserter.assertBlockValues(doubleBlock, blockValues);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(intBlock, block -> serializeDeserializeBlock(block));

        Block bytesRefBlock = TestBlockBuilder.blockFromValues(blockValues, ElementType.BYTES_REF);
        assertThat(bytesRefBlock.elementType(), is(equalTo(ElementType.BYTES_REF)));
        BlockValueAsserter.assertBlockValues(bytesRefBlock, blockValues);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(intBlock, block -> serializeDeserializeBlock(block));
    }

    public void testMultiValuesAndNulls() {
        List<List<Object>> blockValues = new ArrayList<>();
        int positions = randomInt(512);
        for (int i = 0; i < positions; i++) {
            boolean isNull = randomBoolean();
            if (isNull) {
                blockValues.add(List.of()); // empty / null
            } else {
                int rowValueCount = randomInt(16);
                List<Object> row = new ArrayList<>();
                randomInts(rowValueCount).forEach(row::add);
                blockValues.add(row);
            }
        }

        Block intBlock = TestBlockBuilder.blockFromValues(blockValues, ElementType.INT);
        assertThat(intBlock.elementType(), is(equalTo(ElementType.INT)));
        BlockValueAsserter.assertBlockValues(intBlock, blockValues);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(intBlock, block -> serializeDeserializeBlock(block));

        Block longBlock = TestBlockBuilder.blockFromValues(blockValues, ElementType.LONG);
        assertThat(longBlock.elementType(), is(equalTo(ElementType.LONG)));
        BlockValueAsserter.assertBlockValues(longBlock, blockValues);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(intBlock, block -> serializeDeserializeBlock(block));

        Block doubleBlock = TestBlockBuilder.blockFromValues(blockValues, ElementType.DOUBLE);
        assertThat(doubleBlock.elementType(), is(equalTo(ElementType.DOUBLE)));
        BlockValueAsserter.assertBlockValues(doubleBlock, blockValues);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(intBlock, block -> serializeDeserializeBlock(block));

        Block bytesRefBlock = TestBlockBuilder.blockFromValues(blockValues, ElementType.BYTES_REF);
        assertThat(bytesRefBlock.elementType(), is(equalTo(ElementType.BYTES_REF)));
        BlockValueAsserter.assertBlockValues(bytesRefBlock, blockValues);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(intBlock, block -> serializeDeserializeBlock(block));
    }
}
