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
}
