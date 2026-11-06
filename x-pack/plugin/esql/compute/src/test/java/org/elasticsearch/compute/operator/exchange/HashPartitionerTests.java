/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.ComputeTestCase;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class HashPartitionerTests extends ComputeTestCase {

    public void testEqualIntAndLongValuesHashToSamePartition() {
        BlockFactory blockFactory = blockFactory();
        HashPartitioner intPartitioner = new HashPartitioner(new int[] { 0 }, new ElementType[] { ElementType.INT }, 97);
        HashPartitioner longPartitioner = new HashPartitioner(new int[] { 0 }, new ElementType[] { ElementType.LONG }, 97);
        try (Page intPage = intPage(blockFactory, 42); Page longPage = longPage(blockFactory, 42L)) {
            assertThat(partition(intPartitioner, intPage)[0], equalTo(partition(longPartitioner, longPage)[0]));
        }
    }

    public void testNullPositionsAndConstantNullsUseTheSameHash() {
        BlockFactory blockFactory = blockFactory();
        HashPartitioner partitioner = new HashPartitioner(new int[] { 0 }, new ElementType[] { ElementType.LONG }, 13);
        try (Page nullable = nullableLongPage(blockFactory, null, 7L); Page nulls = new Page(blockFactory.newConstantNullBlock(2))) {
            int[] nullablePartitions = partition(partitioner, nullable);
            int[] constantNullPartitions = partition(partitioner, nulls);
            assertThat(nullablePartitions[0], equalTo(constantNullPartitions[0]));
            assertThat(nullablePartitions[0], equalTo(constantNullPartitions[1]));
            assertThat(
                HashPartitioner.hashValue(nullable.getBlock(0), 0, new BytesRef()),
                equalTo(HashPartitioner.hashValue(nulls.getBlock(0), 1, new BytesRef()))
            );
        }
    }

    public void testPartitionResultsAreInRangeAndDeterministic() {
        BlockFactory blockFactory = blockFactory();
        HashPartitioner first = new HashPartitioner(new int[] { 0 }, new ElementType[] { ElementType.LONG }, 8);
        HashPartitioner second = new HashPartitioner(new int[] { 0 }, new ElementType[] { ElementType.LONG }, 8);
        long[] values = new long[1000];
        for (int i = 0; i < values.length; i++) {
            values[i] = randomLong();
        }
        try (Page page = longPage(blockFactory, values)) {
            int[] firstPartitions = partition(first, page);
            int[] secondPartitions = partition(second, page);
            assertArrayEquals(firstPartitions, secondPartitions);
            for (int partition : firstPartitions) {
                assertThat(partition, allOf(greaterThanOrEqualTo(0), lessThan(8)));
            }
        }
    }

    public void testLongValuesSpreadAcrossPartitions() {
        final int positionCount = 100_000;
        final int partitionCount = 8;
        BlockFactory blockFactory = blockFactory();
        HashPartitioner partitioner = new HashPartitioner(new int[] { 0 }, new ElementType[] { ElementType.LONG }, partitionCount);
        long[] values = new long[positionCount];
        for (int i = 0; i < values.length; i++) {
            values[i] = randomLong();
        }
        try (Page page = longPage(blockFactory, values)) {
            int[] partitions = partition(partitioner, page);
            int[] counts = new int[partitionCount];
            for (int partition : partitions) {
                counts[partition]++;
            }
            for (int count : counts) {
                assertThat(count, allOf(greaterThanOrEqualTo(10_000), lessThanOrEqualTo(15_000)));
            }
        }
    }

    public void testMultipleKeyChannelsContributeToPartition() {
        BlockFactory blockFactory = blockFactory();
        HashPartitioner partitioner = new HashPartitioner(
            new int[] { 0, 1 },
            new ElementType[] { ElementType.LONG, ElementType.BYTES_REF },
            1024
        );
        try (
            Page sameKeys = longAndBytesPage(blockFactory, new long[] { 1, 1 }, new String[] { "alpha", "alpha" });
            Page differentSecondKey = longAndBytesPage(blockFactory, new long[] { 1, 1 }, new String[] { "alpha", "beta" })
        ) {
            int[] samePartitions = partition(partitioner, sameKeys);
            int[] differentPartitions = partition(partitioner, differentSecondKey);
            assertThat(samePartitions[0], equalTo(samePartitions[1]));
            assertNotEquals(samePartitions[0], differentPartitions[1]);
        }
    }

    public void testUnsupportedTypeIsRejected() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new HashPartitioner(new int[] { 0 }, new ElementType[] { ElementType.FLOAT }, 4)
        );
        assertThat(exception.getMessage(), containsString("unsupported shuffle key type [FLOAT]"));
    }

    private static int[] partition(HashPartitioner partitioner, Page page) {
        int[] partitions = new int[page.getPositionCount()];
        partitioner.partition(page, partitions);
        return partitions;
    }

    private static Page intPage(BlockFactory blockFactory, int value) {
        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(1)) {
            return new Page(builder.appendInt(value).build());
        }
    }

    private static Page longPage(BlockFactory blockFactory, long... values) {
        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(values.length)) {
            for (long value : values) {
                builder.appendLong(value);
            }
            return new Page(builder.build());
        }
    }

    private static Page nullableLongPage(BlockFactory blockFactory, Long... values) {
        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(values.length)) {
            for (Long value : values) {
                if (value == null) {
                    builder.appendNull();
                } else {
                    builder.appendLong(value);
                }
            }
            return new Page(builder.build());
        }
    }

    private static Page longAndBytesPage(BlockFactory blockFactory, long[] numbers, String[] strings) {
        try (
            LongBlock.Builder numberBuilder = blockFactory.newLongBlockBuilder(numbers.length);
            var stringBuilder = blockFactory.newBytesRefBlockBuilder(strings.length)
        ) {
            for (long number : numbers) {
                numberBuilder.appendLong(number);
            }
            for (String string : strings) {
                stringBuilder.appendBytesRef(new BytesRef(string));
            }
            return new Page(numberBuilder.build(), stringBuilder.build());
        }
    }
}
