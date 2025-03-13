/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.io.IOException;

import static org.elasticsearch.compute.data.BasicBlockTests.assertKeepMask;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class BigArrayBlockBuilderTests extends SerializationTestCase {

    static ByteSizeValue estimateArraySize(long elementSize, long numElements) {
        long bytes = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + RamUsageEstimator.alignObjectSize(elementSize * numElements);
        return ByteSizeValue.ofBytes(bytes);
    }

    public void testLongVector() throws IOException {
        int maxPrimitiveElements = randomIntBetween(100, 1000);
        var maxPrimitiveSize = estimateArraySize(Long.BYTES, maxPrimitiveElements);
        blockFactory = new BlockFactory(blockFactory.breaker(), blockFactory.bigArrays(), maxPrimitiveSize);
        int numElements = between(2, maxPrimitiveElements / 2);
        try (var builder = blockFactory.newLongBlockBuilder(between(1, maxPrimitiveElements / 2))) {
            long[] elements = new long[numElements];
            for (int i = 0; i < numElements; i++) {
                elements[i] = randomLong();
                builder.appendLong(elements[i]);
            }
            try (LongBlock block = builder.build()) {
                assertThat(block, instanceOf(LongVectorBlock.class));
                assertThat(block.asVector(), instanceOf(LongArrayVector.class));
                assertThat(block.getPositionCount(), equalTo(numElements));
                for (int i = 0; i < numElements; i++) {
                    assertThat(block.getLong(i), equalTo(elements[i]));
                }
                assertKeepMask(block);
                try (LongBlock copy = serializeDeserializeBlock(block)) {
                    assertThat(copy, instanceOf(LongVectorBlock.class));
                    assertThat(block.asVector(), instanceOf(LongArrayVector.class));
                    assertThat(copy.getPositionCount(), equalTo(numElements));
                    for (int i = 0; i < numElements; i++) {
                        assertThat(copy.getLong(i), equalTo(elements[i]));
                    }
                }
            }
        }
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
        numElements = between(maxPrimitiveElements + 10, maxPrimitiveElements * 2);
        try (var builder = blockFactory.newLongBlockBuilder(between(1, maxPrimitiveElements * 2))) {
            long[] elements = new long[numElements];
            for (int i = 0; i < numElements; i++) {
                elements[i] = randomLong();
                builder.appendLong(elements[i]);
            }
            try (LongBlock block = builder.build()) {
                assertThat(block, instanceOf(LongVectorBlock.class));
                assertThat(block.asVector(), instanceOf(LongBigArrayVector.class));
                assertThat(block.getPositionCount(), equalTo(numElements));
                for (int i = 0; i < numElements; i++) {
                    assertThat(block.getLong(i), equalTo(elements[i]));
                }
                assertKeepMask(block);
                try (LongBlock copy = serializeDeserializeBlock(block)) {
                    assertThat(copy, instanceOf(LongVectorBlock.class));
                    assertThat(block.asVector(), instanceOf(LongBigArrayVector.class));
                    assertThat(copy.getPositionCount(), equalTo(numElements));
                    for (int i = 0; i < numElements; i++) {
                        assertThat(copy.getLong(i), equalTo(elements[i]));
                    }
                }
            }
        }
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
    }

    public void testLongBlock() throws IOException {
        int maxPrimitiveElements = randomIntBetween(1000, 5000);
        var maxPrimitiveSize = estimateArraySize(Long.BYTES, maxPrimitiveElements);
        blockFactory = new BlockFactory(blockFactory.breaker(), blockFactory.bigArrays(), maxPrimitiveSize);
        int numElements = between(2, maxPrimitiveElements / 2);
        try (var builder = blockFactory.newLongBlockBuilder(between(1, maxPrimitiveElements / 2))) {
            long[] elements = new long[numElements];
            builder.beginPositionEntry();
            for (int i = 0; i < numElements; i++) {
                elements[i] = randomLong();
                builder.appendLong(elements[i]);
            }
            builder.endPositionEntry();
            try (LongBlock block = builder.build()) {
                assertThat(block, instanceOf(LongArrayBlock.class));
                assertNull(block.asVector());
                assertThat(block.getPositionCount(), equalTo(1));
                for (int i = 0; i < numElements; i++) {
                    assertThat(block.getLong(i), equalTo(elements[i]));
                }
                assertKeepMask(block);
                try (LongBlock copy = serializeDeserializeBlock(block)) {
                    assertThat(copy, instanceOf(LongArrayBlock.class));
                    assertNull(copy.asVector());
                    assertThat(copy.getPositionCount(), equalTo(1));
                    for (int i = 0; i < numElements; i++) {
                        assertThat(copy.getLong(i), equalTo(elements[i]));
                    }
                }
            }
        }
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
        numElements = between(maxPrimitiveElements + 10, maxPrimitiveElements * 2);
        try (var builder = blockFactory.newLongBlockBuilder(between(1, maxPrimitiveElements * 2))) {
            long[] elements = new long[numElements];
            builder.beginPositionEntry();
            for (int i = 0; i < numElements; i++) {
                elements[i] = randomLong();
                builder.appendLong(elements[i]);
            }
            builder.endPositionEntry();
            try (LongBlock block = builder.build()) {
                assertThat(block, instanceOf(LongBigArrayBlock.class));
                assertNull(block.asVector());
                assertThat(block.getPositionCount(), equalTo(1));
                assertThat(block.getTotalValueCount(), equalTo(numElements));
                for (int i = 0; i < numElements; i++) {
                    assertThat(block.getLong(i), equalTo(elements[i]));
                }
                assertKeepMask(block);
                try (LongBlock copy = serializeDeserializeBlock(block)) {
                    assertThat(copy, instanceOf(LongBigArrayBlock.class));
                    assertNull(block.asVector());
                    assertThat(copy.getPositionCount(), equalTo(1));
                    assertThat(copy.getTotalValueCount(), equalTo(numElements));
                    for (int i = 0; i < numElements; i++) {
                        assertThat(copy.getLong(i), equalTo(elements[i]));
                    }
                }
            }
        }
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
    }

    public void testBooleanVector() throws IOException {
        int maxPrimitiveElements = randomIntBetween(100, 1000);
        var maxPrimitiveSize = estimateArraySize(Byte.BYTES, maxPrimitiveElements);
        blockFactory = new BlockFactory(blockFactory.breaker(), blockFactory.bigArrays(), maxPrimitiveSize);
        int numElements = between(2, maxPrimitiveElements / 2);
        try (var builder = blockFactory.newBooleanBlockBuilder(between(1, maxPrimitiveElements / 2))) {
            boolean[] elements = new boolean[numElements];
            for (int i = 0; i < numElements; i++) {
                elements[i] = randomBoolean();
                builder.appendBoolean(elements[i]);
            }
            try (var block = builder.build()) {
                assertThat(block, instanceOf(BooleanVectorBlock.class));
                assertThat(block.asVector(), instanceOf(BooleanArrayVector.class));
                assertThat(block.getPositionCount(), equalTo(numElements));
                for (int i = 0; i < numElements; i++) {
                    assertThat(block.getBoolean(i), equalTo(elements[i]));
                }
                assertKeepMask(block);
                try (ToMask mask = block.toMask()) {
                    assertThat(mask.hadMultivaluedFields(), equalTo(false));
                    for (int p = 0; p < elements.length; p++) {
                        assertThat(mask.mask().getBoolean(p), equalTo(elements[p]));
                    }
                }
                try (var copy = serializeDeserializeBlock(block)) {
                    assertThat(copy, instanceOf(BooleanVectorBlock.class));
                    assertThat(block.asVector(), instanceOf(BooleanArrayVector.class));
                    assertThat(copy.getPositionCount(), equalTo(numElements));
                    for (int i = 0; i < numElements; i++) {
                        assertThat(copy.getBoolean(i), equalTo(elements[i]));
                    }
                }
            }
        }
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
        numElements = between(maxPrimitiveElements + 10, maxPrimitiveElements * 2);
        try (var builder = blockFactory.newBooleanBlockBuilder(between(1, maxPrimitiveElements * 2))) {
            boolean[] elements = new boolean[numElements];
            for (int i = 0; i < numElements; i++) {
                elements[i] = randomBoolean();
                builder.appendBoolean(elements[i]);
            }
            try (var block = builder.build()) {
                assertThat(block, instanceOf(BooleanVectorBlock.class));
                assertThat(block.asVector(), instanceOf(BooleanBigArrayVector.class));
                assertThat(block.getPositionCount(), equalTo(numElements));
                for (int i = 0; i < numElements; i++) {
                    assertThat(block.getBoolean(i), equalTo(elements[i]));
                }
                assertKeepMask(block);
                try (var copy = serializeDeserializeBlock(block)) {
                    assertThat(copy, instanceOf(BooleanVectorBlock.class));
                    assertThat(block.asVector(), instanceOf(BooleanBigArrayVector.class));
                    assertThat(copy.getPositionCount(), equalTo(numElements));
                    for (int i = 0; i < numElements; i++) {
                        assertThat(copy.getBoolean(i), equalTo(elements[i]));
                    }
                }
            }
        }
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
    }

    public void testBooleanBlock() throws IOException {
        int maxPrimitiveElements = randomIntBetween(1000, 5000);
        var maxPrimitiveSize = estimateArraySize(Byte.BYTES, maxPrimitiveElements);
        blockFactory = new BlockFactory(blockFactory.breaker(), blockFactory.bigArrays(), maxPrimitiveSize);
        int numElements = between(2, maxPrimitiveElements / 2);
        try (var builder = blockFactory.newBooleanBlockBuilder(between(1, maxPrimitiveElements / 2))) {
            boolean[] elements = new boolean[numElements];
            builder.beginPositionEntry();
            for (int i = 0; i < numElements; i++) {
                elements[i] = randomBoolean();
                builder.appendBoolean(elements[i]);
            }
            builder.endPositionEntry();
            try (var block = builder.build()) {
                assertThat(block, instanceOf(BooleanArrayBlock.class));
                assertNull(block.asVector());
                assertThat(block.getPositionCount(), equalTo(1));
                for (int i = 0; i < numElements; i++) {
                    assertThat(block.getBoolean(i), equalTo(elements[i]));
                }
                assertKeepMask(block);
                try (ToMask mask = block.toMask()) {
                    assertThat(mask.hadMultivaluedFields(), equalTo(true));
                    for (int p = 0; p < elements.length; p++) {
                        assertThat(mask.mask().getBoolean(p), equalTo(false));
                    }
                }
                try (var copy = serializeDeserializeBlock(block)) {
                    assertThat(copy, instanceOf(BooleanArrayBlock.class));
                    assertNull(copy.asVector());
                    assertThat(copy.getPositionCount(), equalTo(1));
                    for (int i = 0; i < numElements; i++) {
                        assertThat(copy.getBoolean(i), equalTo(elements[i]));
                    }
                }
            }
        }
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
        numElements = between(maxPrimitiveElements + 10, maxPrimitiveElements * 2);
        try (var builder = blockFactory.newBooleanBlockBuilder(between(1, maxPrimitiveElements * 2))) {
            boolean[] elements = new boolean[numElements];
            builder.beginPositionEntry();
            for (int i = 0; i < numElements; i++) {
                elements[i] = randomBoolean();
                builder.appendBoolean(elements[i]);
            }
            builder.endPositionEntry();
            try (var block = builder.build()) {
                assertThat(block, instanceOf(BooleanBigArrayBlock.class));
                assertNull(block.asVector());
                assertThat(block.getPositionCount(), equalTo(1));
                assertThat(block.getTotalValueCount(), equalTo(numElements));
                for (int i = 0; i < numElements; i++) {
                    assertThat(block.getBoolean(i), equalTo(elements[i]));
                }
                assertKeepMask(block);
                try (ToMask mask = block.toMask()) {
                    assertThat(mask.hadMultivaluedFields(), equalTo(true));
                    for (int p = 0; p < elements.length; p++) {
                        assertThat(mask.mask().getBoolean(p), equalTo(false));
                    }
                }
                try (var copy = serializeDeserializeBlock(block)) {
                    assertThat(copy, instanceOf(BooleanBigArrayBlock.class));
                    assertNull(block.asVector());
                    assertThat(copy.getPositionCount(), equalTo(1));
                    assertThat(copy.getTotalValueCount(), equalTo(numElements));
                    for (int i = 0; i < numElements; i++) {
                        assertThat(copy.getBoolean(i), equalTo(elements[i]));
                    }
                }
            }
        }
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
    }

    /**
     * Tests a block with one value being multivalued and the rest are single valued.
     */
    public void testBooleanBlockOneMv() {
        int mvCount = between(2, 10);
        int positionCount = randomIntBetween(1000, 5000);
        blockFactory = new BlockFactory(blockFactory.breaker(), blockFactory.bigArrays(), ByteSizeValue.ofBytes(1));
        try (var builder = blockFactory.newBooleanBlockBuilder(between(1, mvCount + positionCount))) {
            boolean[] elements = new boolean[positionCount + mvCount];
            builder.beginPositionEntry();
            for (int i = 0; i < mvCount; i++) {
                elements[i] = randomBoolean();
                builder.appendBoolean(elements[i]);
            }
            builder.endPositionEntry();
            for (int p = 1; p < positionCount; p++) {
                elements[mvCount + p] = randomBoolean();
                builder.appendBoolean(elements[mvCount + p]);
            }
            try (var block = builder.build()) {
                assertThat(block, instanceOf(BooleanBigArrayBlock.class));
                assertNull(block.asVector());
                assertThat(block.getPositionCount(), equalTo(positionCount));
                assertThat(block.getValueCount(0), equalTo(mvCount));
                for (int i = 0; i < mvCount; i++) {
                    assertThat(block.getBoolean(block.getFirstValueIndex(0) + i), equalTo(elements[i]));
                }
                for (int p = 1; p < positionCount; p++) {
                    assertThat(block.getValueCount(p), equalTo(1));
                    assertThat(block.getBoolean(block.getFirstValueIndex(p)), equalTo(elements[mvCount + p]));
                }
                assertKeepMask(block);
                try (ToMask mask = block.toMask()) {
                    /*
                     * NOTE: this test is customized to the layout above where we don't make
                     * any fields with 0 values.
                     */
                    assertThat(mask.hadMultivaluedFields(), equalTo(true));
                    assertThat(mask.mask().getBoolean(0), equalTo(false));
                    for (int p = 1; p < positionCount; p++) {
                        assertThat(mask.mask().getBoolean(p), equalTo(elements[mvCount + p]));
                    }
                }
            }
        }
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
    }
}
