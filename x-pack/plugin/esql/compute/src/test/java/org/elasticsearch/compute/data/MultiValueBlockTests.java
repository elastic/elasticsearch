/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.compute.test.TestBlockBuilder;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.compute.data.BasicBlockTests.assertKeepMask;
import static org.elasticsearch.compute.data.BasicBlockTests.assertKeepMaskEmpty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class MultiValueBlockTests extends SerializationTestCase {

    public void testIntBlockTrivial1() {
        var blockBuilder = blockFactory.newIntBlockBuilder(4);
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
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(block, this::serializeDeserializeBlock, null, Releasable::close);
        assertKeepMask(block);
        block.close();
    }

    public void testIntBlockTrivial() {
        var blockBuilder = blockFactory.newIntBlockBuilder(10);
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
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(block, this::serializeDeserializeBlock, null, Releasable::close);
        assertKeepMask(block);
        block.close();
    }

    public void testEmpty() {
        for (int initialSize : new int[] { 0, 10, 100, randomInt(512) }) {
            IntBlock intBlock = blockFactory.newIntBlockBuilder(initialSize).build();
            assertThat(intBlock.getPositionCount(), is(0));
            assertThat(intBlock.asVector(), is(notNullValue()));
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(intBlock, this::serializeDeserializeBlock, null, Releasable::close);
            assertKeepMaskEmpty(intBlock);
            intBlock.close();

            LongBlock longBlock = blockFactory.newLongBlockBuilder(initialSize).build();
            assertThat(longBlock.getPositionCount(), is(0));
            assertThat(longBlock.asVector(), is(notNullValue()));
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(longBlock, this::serializeDeserializeBlock, null, Releasable::close);
            assertKeepMaskEmpty(longBlock);
            longBlock.close();

            DoubleBlock doubleBlock = blockFactory.newDoubleBlockBuilder(initialSize).build();
            assertThat(doubleBlock.getPositionCount(), is(0));
            assertThat(doubleBlock.asVector(), is(notNullValue()));
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(doubleBlock, this::serializeDeserializeBlock, null, Releasable::close);
            assertKeepMaskEmpty(doubleBlock);
            doubleBlock.close();

            FloatBlock floatBlock = blockFactory.newFloatBlockBuilder(initialSize).build();
            assertThat(floatBlock.getPositionCount(), is(0));
            assertThat(floatBlock.asVector(), is(notNullValue()));
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(floatBlock, this::serializeDeserializeBlock, null, Releasable::close);
            assertKeepMaskEmpty(floatBlock);
            floatBlock.close();

            BytesRefBlock bytesRefBlock = blockFactory.newBytesRefBlockBuilder(initialSize).build();
            assertThat(bytesRefBlock.getPositionCount(), is(0));
            assertThat(bytesRefBlock.asVector(), is(notNullValue()));
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(bytesRefBlock, this::serializeDeserializeBlock, null, Releasable::close);
            assertKeepMaskEmpty(bytesRefBlock);
            bytesRefBlock.close();
        }
    }

    public void testNullOnly() throws IOException {
        for (int initialSize : new int[] { 0, 10, 100, randomInt(512) }) {
            IntBlock intBlock = blockFactory.newIntBlockBuilder(initialSize).appendNull().build();
            assertThat(intBlock.getPositionCount(), is(1));
            assertThat(intBlock.getValueCount(0), is(0));
            assertNull(intBlock.asVector());
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(intBlock, this::serializeDeserializeBlock, null, Releasable::close);
            assertKeepMask(intBlock);
            intBlock.close();

            LongBlock longBlock = blockFactory.newLongBlockBuilder(initialSize).appendNull().build();
            assertThat(longBlock.getPositionCount(), is(1));
            assertThat(longBlock.getValueCount(0), is(0));
            assertNull(longBlock.asVector());
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(longBlock, this::serializeDeserializeBlock, null, Releasable::close);
            assertKeepMask(longBlock);
            longBlock.close();

            DoubleBlock doubleBlock = blockFactory.newDoubleBlockBuilder(initialSize).appendNull().build();
            assertThat(doubleBlock.getPositionCount(), is(1));
            assertThat(doubleBlock.getValueCount(0), is(0));
            assertNull(doubleBlock.asVector());
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(doubleBlock, this::serializeDeserializeBlock, null, Releasable::close);
            assertKeepMask(doubleBlock);
            doubleBlock.close();

            FloatBlock floatBlock = blockFactory.newFloatBlockBuilder(initialSize).appendNull().build();
            assertThat(floatBlock.getPositionCount(), is(1));
            assertThat(floatBlock.getValueCount(0), is(0));
            assertNull(floatBlock.asVector());
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(floatBlock, this::serializeDeserializeBlock, null, Releasable::close);
            assertKeepMask(floatBlock);
            floatBlock.close();

            BytesRefBlock bytesRefBlock = blockFactory.newBytesRefBlockBuilder(initialSize).appendNull().build();
            assertThat(bytesRefBlock.getPositionCount(), is(1));
            assertThat(bytesRefBlock.getValueCount(0), is(0));
            assertNull(bytesRefBlock.asVector());
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(bytesRefBlock, this::serializeDeserializeBlock, null, Releasable::close);
            assertKeepMask(bytesRefBlock);
            bytesRefBlock.close();
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
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(intBlock, this::serializeDeserializeBlock, null, Releasable::close);
        assertKeepMask(intBlock);

        Block longBlock = TestBlockBuilder.blockFromValues(blockValues, ElementType.LONG);
        assertThat(longBlock.elementType(), is(equalTo(ElementType.LONG)));
        BlockValueAsserter.assertBlockValues(longBlock, blockValues);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(longBlock, this::serializeDeserializeBlock, null, Releasable::close);
        assertKeepMask(longBlock);

        Block doubleBlock = TestBlockBuilder.blockFromValues(blockValues, ElementType.DOUBLE);
        assertThat(doubleBlock.elementType(), is(equalTo(ElementType.DOUBLE)));
        BlockValueAsserter.assertBlockValues(doubleBlock, blockValues);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(doubleBlock, this::serializeDeserializeBlock, null, Releasable::close);
        assertKeepMask(doubleBlock);

        Block floatBlock = TestBlockBuilder.blockFromValues(blockValues, ElementType.DOUBLE);
        assertThat(floatBlock.elementType(), is(equalTo(ElementType.DOUBLE)));
        BlockValueAsserter.assertBlockValues(floatBlock, blockValues);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(floatBlock, this::serializeDeserializeBlock, null, Releasable::close);
        assertKeepMask(floatBlock);

        Block bytesRefBlock = TestBlockBuilder.blockFromValues(blockValues, ElementType.BYTES_REF);
        assertThat(bytesRefBlock.elementType(), is(equalTo(ElementType.BYTES_REF)));
        BlockValueAsserter.assertBlockValues(bytesRefBlock, blockValues);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(bytesRefBlock, this::serializeDeserializeBlock, null, Releasable::close);
        assertKeepMask(bytesRefBlock);
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
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(intBlock, this::serializeDeserializeBlock, null, Releasable::close);
        assertKeepMask(intBlock);

        Block longBlock = TestBlockBuilder.blockFromValues(blockValues, ElementType.LONG);
        assertThat(longBlock.elementType(), is(equalTo(ElementType.LONG)));
        BlockValueAsserter.assertBlockValues(longBlock, blockValues);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(longBlock, this::serializeDeserializeBlock, null, Releasable::close);
        assertKeepMask(longBlock);

        Block doubleBlock = TestBlockBuilder.blockFromValues(blockValues, ElementType.DOUBLE);
        assertThat(doubleBlock.elementType(), is(equalTo(ElementType.DOUBLE)));
        BlockValueAsserter.assertBlockValues(doubleBlock, blockValues);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(doubleBlock, this::serializeDeserializeBlock, null, Releasable::close);
        assertKeepMask(doubleBlock);

        Block floatBlock = TestBlockBuilder.blockFromValues(blockValues, ElementType.FLOAT);
        assertThat(floatBlock.elementType(), is(equalTo(ElementType.FLOAT)));
        BlockValueAsserter.assertBlockValues(floatBlock, blockValues);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(floatBlock, this::serializeDeserializeBlock, null, Releasable::close);
        assertKeepMask(floatBlock);

        Block bytesRefBlock = TestBlockBuilder.blockFromValues(blockValues, ElementType.BYTES_REF);
        assertThat(bytesRefBlock.elementType(), is(equalTo(ElementType.BYTES_REF)));
        BlockValueAsserter.assertBlockValues(bytesRefBlock, blockValues);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(bytesRefBlock, this::serializeDeserializeBlock, null, Releasable::close);
        assertKeepMask(bytesRefBlock);
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
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(intBlock, this::serializeDeserializeBlock, null, Releasable::close);
        assertKeepMask(intBlock);

        Block longBlock = TestBlockBuilder.blockFromValues(blockValues, ElementType.LONG);
        assertThat(longBlock.elementType(), is(equalTo(ElementType.LONG)));
        BlockValueAsserter.assertBlockValues(longBlock, blockValues);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(longBlock, this::serializeDeserializeBlock, null, Releasable::close);
        assertKeepMask(longBlock);

        Block doubleBlock = TestBlockBuilder.blockFromValues(blockValues, ElementType.DOUBLE);
        assertThat(doubleBlock.elementType(), is(equalTo(ElementType.DOUBLE)));
        BlockValueAsserter.assertBlockValues(doubleBlock, blockValues);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(doubleBlock, this::serializeDeserializeBlock, null, Releasable::close);
        assertKeepMask(doubleBlock);

        Block floatBlock = TestBlockBuilder.blockFromValues(blockValues, ElementType.FLOAT);
        assertThat(floatBlock.elementType(), is(equalTo(ElementType.FLOAT)));
        BlockValueAsserter.assertBlockValues(floatBlock, blockValues);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(floatBlock, this::serializeDeserializeBlock, null, Releasable::close);
        assertKeepMask(floatBlock);

        Block bytesRefBlock = TestBlockBuilder.blockFromValues(blockValues, ElementType.BYTES_REF);
        assertThat(bytesRefBlock.elementType(), is(equalTo(ElementType.BYTES_REF)));
        BlockValueAsserter.assertBlockValues(bytesRefBlock, blockValues);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(bytesRefBlock, this::serializeDeserializeBlock, null, Releasable::close);
        assertKeepMask(bytesRefBlock);
    }

    // Tests that the use of Block builder beginPositionEntry (or not) with just a single value,
    // and no nulls, builds a block backed by a vector.
    public void testSingleNonNullValues() throws IOException {
        List<Object> blockValues = new ArrayList<>();
        int positions = randomInt(512);
        for (int i = 0; i < positions; i++) {
            blockValues.add(randomInt());
        }

        var blocks = List.of(
            TestBlockBuilder.blockFromSingleValues(blockValues, ElementType.BOOLEAN),
            TestBlockBuilder.blockFromValues(blockValues.stream().map(List::of).toList(), ElementType.BOOLEAN),
            TestBlockBuilder.blockFromSingleValues(blockValues, ElementType.INT),
            TestBlockBuilder.blockFromValues(blockValues.stream().map(List::of).toList(), ElementType.INT),
            TestBlockBuilder.blockFromSingleValues(blockValues, ElementType.LONG),
            TestBlockBuilder.blockFromValues(blockValues.stream().map(List::of).toList(), ElementType.LONG),
            TestBlockBuilder.blockFromSingleValues(blockValues, ElementType.DOUBLE),
            TestBlockBuilder.blockFromValues(blockValues.stream().map(List::of).toList(), ElementType.DOUBLE),
            TestBlockBuilder.blockFromSingleValues(blockValues, ElementType.FLOAT),
            TestBlockBuilder.blockFromValues(blockValues.stream().map(List::of).toList(), ElementType.FLOAT),
            TestBlockBuilder.blockFromSingleValues(blockValues, ElementType.BYTES_REF),
            TestBlockBuilder.blockFromValues(blockValues.stream().map(List::of).toList(), ElementType.BYTES_REF)
        );
        try {
            for (Block block : blocks) {
                assertThat(block.asVector(), is(notNullValue()));
                BlockValueAsserter.assertBlockValues(block, blockValues.stream().map(List::of).toList());
                EqualsHashCodeTestUtils.checkEqualsAndHashCode(block, this::serializeDeserializeBlock, null, Releasable::close);
                assertKeepMask(block);
            }
        } finally {
            Releasables.close(blocks);
        }
    }

    // A default max iteration times, just to avoid an infinite loop.
    static final int TIMES = 10_000;

    // Tests that the use of Block builder beginPositionEntry (or not) with just a single value,
    // with nulls, builds a block not backed by a vector.
    public void testSingleWithNullValues() {
        List<Object> blockValues = new ArrayList<>();
        boolean atLeastOneNull = false;
        int positions = randomIntBetween(1, 512);  // we must have at least one null entry
        int times = 0;
        while (atLeastOneNull == false && times < TIMES) {
            times++;
            for (int i = 0; i < positions; i++) {
                boolean isNull = randomBoolean();
                if (isNull) {
                    atLeastOneNull = true;
                    blockValues.add(null); // empty / null
                } else {
                    blockValues.add(randomInt());
                }
            }
        }
        assert atLeastOneNull : "Failed to create a values block with at least one null in " + times + " times";

        var blocks = List.of(
            TestBlockBuilder.blockFromSingleValues(blockValues, ElementType.BOOLEAN),
            TestBlockBuilder.blockFromValues(blockValues.stream().map(MultiValueBlockTests::mapToList).toList(), ElementType.BOOLEAN),
            TestBlockBuilder.blockFromSingleValues(blockValues, ElementType.INT),
            TestBlockBuilder.blockFromValues(blockValues.stream().map(MultiValueBlockTests::mapToList).toList(), ElementType.INT),
            TestBlockBuilder.blockFromSingleValues(blockValues, ElementType.LONG),
            TestBlockBuilder.blockFromValues(blockValues.stream().map(MultiValueBlockTests::mapToList).toList(), ElementType.LONG),
            TestBlockBuilder.blockFromSingleValues(blockValues, ElementType.DOUBLE),
            TestBlockBuilder.blockFromValues(blockValues.stream().map(MultiValueBlockTests::mapToList).toList(), ElementType.DOUBLE),
            TestBlockBuilder.blockFromSingleValues(blockValues, ElementType.FLOAT),
            TestBlockBuilder.blockFromValues(blockValues.stream().map(MultiValueBlockTests::mapToList).toList(), ElementType.FLOAT),
            TestBlockBuilder.blockFromSingleValues(blockValues, ElementType.BYTES_REF),
            TestBlockBuilder.blockFromValues(blockValues.stream().map(MultiValueBlockTests::mapToList).toList(), ElementType.BYTES_REF)
        );
        for (Block block : blocks) {
            assertThat(block.asVector(), is(nullValue()));
            BlockValueAsserter.assertBlockValues(block, blockValues.stream().map(MultiValueBlockTests::mapToList).toList());
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(block, this::serializeDeserializeBlock, null, Releasable::close);
            assertKeepMask(block);
        }
    }

    // Returns a list containing the given obj, or an empty list if obj is null
    static List<Object> mapToList(Object obj) {
        if (obj == null) {
            return List.of();
        } else {
            return List.of(obj);
        }
    }
}
