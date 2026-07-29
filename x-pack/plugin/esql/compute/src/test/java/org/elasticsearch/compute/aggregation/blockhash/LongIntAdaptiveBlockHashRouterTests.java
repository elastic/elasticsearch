/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.swisshash.LongLongSwissHash;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Exercises {@link BlockHash.Router} for {@link LongIntAdaptiveBlockHash}: confirms
 * {@code partitionHashOfRow} agrees with {@link LongLongSwissHash#hash}, the fast-path
 * {@code fillPartitions} assigns the correct partition for dense (null-free) blocks, and the
 * fallback routes null rows to {@code nullPartition}.
 */
public class LongIntAdaptiveBlockHashRouterTests extends BlockHashTestCase {

    private BlockHash longIntBlockHash() {
        return BlockHash.build(
            List.of(new BlockHash.GroupSpec(0, ElementType.LONG), new BlockHash.GroupSpec(1, ElementType.INT)),
            blockFactory,
            16 * 1024,
            false
        );
    }

    public void testRouterNonNullWhenSwissHashAvailable() {
        assumeTrue("SwissHash not available on this JVM", HashImplFactory.SWISS_HASH_AVAILABLE);
        try (BlockHash hash = longIntBlockHash()) {
            assertThat(hash.router(), notNullValue());
        }
    }

    public void testPartitionHashMatchesLongLongSwissHash() {
        assumeTrue("SwissHash not available on this JVM", HashImplFactory.SWISS_HASH_AVAILABLE);
        long[] k1s = { 5L, -3L, 0L, Long.MAX_VALUE };
        int[] k2s = { 7, -2, 0, Integer.MAX_VALUE };
        try (
            BlockHash hash = longIntBlockHash();
            LongBlock longBlock = blockFactory.newLongArrayVector(k1s, k1s.length).asBlock();
            IntBlock intBlock = blockFactory.newIntArrayVector(k2s, k2s.length).asBlock()
        ) {
            BlockHash.Router router = hash.router();
            assertThat(router, notNullValue());
            Page page = new Page(longBlock, intBlock);
            for (int i = 0; i < k1s.length; i++) {
                assertThat(
                    "partitionHashOfRow must equal LongLongSwissHash.hash(k1, k2) for row " + i,
                    router.partitionHashOfRow(page, i),
                    equalTo((int) LongLongSwissHash.hash(k1s[i], k2s[i]))
                );
            }
        }
    }

    public void testFillPartitionsFastPathDenseBlocks() {
        assumeTrue("SwissHash not available on this JVM", HashImplFactory.SWISS_HASH_AVAILABLE);
        long[] k1s = { 1L, 42L, -7L, 0L, Long.MIN_VALUE };
        int[] k2s = { 3, 0, 100, -1, Integer.MIN_VALUE };
        int partitionCount = 8;
        int nullPartition = 0;

        try (
            BlockHash hash = longIntBlockHash();
            LongBlock longBlock = blockFactory.newLongArrayVector(k1s, k1s.length).asBlock();
            IntBlock intBlock = blockFactory.newIntArrayVector(k2s, k2s.length).asBlock()
        ) {
            BlockHash.Router router = hash.router();
            assertThat(router, notNullValue());
            Page page = new Page(longBlock, intBlock);

            int[] partitionOf = new int[k1s.length];
            int[] counts = new int[partitionCount];
            router.fillPartitions(page, k1s.length, 1, partitionCount, nullPartition, partitionOf, counts);

            int totalCounted = 0;
            for (int p = 0; p < partitionCount; p++) {
                totalCounted += counts[p];
            }
            assertThat("counts must sum to row count", totalCounted, equalTo(k1s.length));

            for (int i = 0; i < k1s.length; i++) {
                int expected = Math.floorMod((int) LongLongSwissHash.hash(k1s[i], k2s[i]), partitionCount);
                assertThat("partition for row " + i + " must match hash formula", partitionOf[i], equalTo(expected));
            }
        }
    }

    public void testFillPartitionsFallbackNullRows() {
        assumeTrue("SwissHash not available on this JVM", HashImplFactory.SWISS_HASH_AVAILABLE);
        // Row 0: null long key; rows 1 and 2: non-null
        long[] nonNullK1s = { 99L, -5L };
        int[] k2s = { 10, 20, 30 };
        int partitionCount = 4;
        int nullPartition = 0;

        try (BlockHash hash = longIntBlockHash(); IntBlock intBlock = blockFactory.newIntArrayVector(k2s, k2s.length).asBlock()) {
            LongBlock longBlock;
            try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(3)) {
                builder.appendNull();
                builder.appendLong(nonNullK1s[0]);
                builder.appendLong(nonNullK1s[1]);
                longBlock = builder.build();
            }
            try {
                BlockHash.Router router = hash.router();
                assertThat(router, notNullValue());
                Page page = new Page(longBlock, intBlock);

                int[] partitionOf = new int[3];
                int[] counts = new int[partitionCount];
                router.fillPartitions(page, 3, 1, partitionCount, nullPartition, partitionOf, counts);

                assertThat("null row must land in nullPartition", partitionOf[0], equalTo(nullPartition));
                for (int i = 1; i < 3; i++) {
                    int expected = Math.floorMod((int) LongLongSwissHash.hash(nonNullK1s[i - 1], k2s[i]), partitionCount);
                    assertThat("non-null row " + i + " must match hash formula", partitionOf[i], equalTo(expected));
                }
            } finally {
                longBlock.close();
            }
        }
    }
}
