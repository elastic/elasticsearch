/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;

public class PrefixBlockHashTests extends BlockHashTestCase {

    private int[] addAndCaptureGroupIds(BlockHash blockHash, Block... values) {
        int[][] captured = new int[1][];
        blockHash.add(new Page(values), new GroupingAggregatorFunction.AddInput() {
            private void addBlock(int positionOffset, IntBlock groupIds) {
                int[] ids = new int[groupIds.getPositionCount()];
                for (int p = 0; p < ids.length; p++) {
                    ids[p] = groupIds.getInt(p);
                }
                captured[0] = ids;
            }

            @Override
            public void add(int positionOffset, IntArrayBlock groupIds) {
                addBlock(positionOffset, groupIds);
            }

            @Override
            public void add(int positionOffset, IntBigArrayBlock groupIds) {
                addBlock(positionOffset, groupIds);
            }

            @Override
            public void add(int positionOffset, IntVector groupIds) {
                addBlock(positionOffset, groupIds.asBlock());
            }

            @Override
            public void close() {
                fail("hashes should not close AddInput");
            }
        });
        return captured[0];
    }

    public void testGroupsSamePrefixAndTailTogether() {
        try (
            LongBlock prefix = blockFactory.newLongArrayVector(new long[] { 1, 1, 2, 2, 1 }, 5).asBlock();
            BytesRefBlock tail = bytesRefBlock("a", "b", "a", "b", "a");
            BlockHash hash = new PrefixBlockHash(
                0,
                ElementType.LONG,
                List.of(new BlockHash.GroupSpec(1, ElementType.BYTES_REF)),
                blockFactory
            )
        ) {
            int[] ids = addAndCaptureGroupIds(hash, prefix, tail);
            // (prefix=1, tail="a") occurs at positions 0 and 4 - must get the same group id.
            assertThat(ids[0], equalTo(ids[4]));
            // Every other combination must be distinct from each other and from group 0.
            assertThat(ids[1], not(equalTo(ids[0])));
            assertThat(ids[2], not(equalTo(ids[0])));
            assertThat(ids[3], not(equalTo(ids[0])));
            assertThat(ids[1], not(equalTo(ids[2])));
            assertThat(ids[1], not(equalTo(ids[3])));
            assertThat(ids[2], not(equalTo(ids[3])));
            assertThat(hash.numKeys(), equalTo(4));

            assertRoundTrip(hash, ids, new Object[][] { { 1L, "a" }, { 1L, "b" }, { 2L, "a" }, { 2L, "b" }, { 1L, "a" } });
        }
    }

    public void testMultipleTailColumns() {
        try (
            LongBlock prefix = blockFactory.newLongArrayVector(new long[] { 10, 10, 20 }, 3).asBlock();
            BytesRefBlock tail1 = bytesRefBlock("host-1", "host-1", "host-1");
            BytesRefBlock tail2 = bytesRefBlock("cpu0", "cpu1", "cpu0");
            BlockHash hash = new PrefixBlockHash(
                0,
                ElementType.LONG,
                List.of(new BlockHash.GroupSpec(1, ElementType.BYTES_REF), new BlockHash.GroupSpec(2, ElementType.BYTES_REF)),
                blockFactory
            )
        ) {
            int[] ids = addAndCaptureGroupIds(hash, prefix, tail1, tail2);
            assertThat(ids[0], not(equalTo(ids[1]))); // same prefix, different tail2
            assertThat(ids[0], not(equalTo(ids[2]))); // same tail, different prefix
            assertThat(hash.numKeys(), equalTo(3));

            assertRoundTrip(hash, ids, new Object[][] { { 10L, "host-1", "cpu0" }, { 10L, "host-1", "cpu1" }, { 20L, "host-1", "cpu0" } });
        }
    }

    public void testNullTailValue() {
        try (BytesRefBlock.Builder tailBuilder = blockFactory.newBytesRefBlockBuilder(2)) {
            tailBuilder.appendBytesRef(new BytesRef("x"));
            tailBuilder.appendNull();
            try (
                LongBlock prefix = blockFactory.newLongArrayVector(new long[] { 1, 1 }, 2).asBlock();
                BytesRefBlock tail = tailBuilder.build();
                BlockHash hash = new PrefixBlockHash(
                    0,
                    ElementType.LONG,
                    List.of(new BlockHash.GroupSpec(1, ElementType.BYTES_REF)),
                    blockFactory
                )
            ) {
                int[] ids = addAndCaptureGroupIds(hash, prefix, tail);
                assertThat(ids[0], not(equalTo(ids[1])));
                assertThat(hash.numKeys(), equalTo(2));
                assertRoundTrip(hash, ids, new Object[][] { { 1L, "x" }, { 1L, null } });
            }
        }
    }

    public void testMultiValuedTailThrows() {
        LongBlock prefix = blockFactory.newLongArrayVector(new long[] { 1 }, 1).asBlock();
        try (BytesRefBlock.Builder tailBuilder = blockFactory.newBytesRefBlockBuilder(1)) {
            tailBuilder.beginPositionEntry();
            tailBuilder.appendBytesRef(new BytesRef("a"));
            tailBuilder.appendBytesRef(new BytesRef("b"));
            tailBuilder.endPositionEntry();
            try (
                BytesRefBlock tail = tailBuilder.build();
                BlockHash hash = new PrefixBlockHash(
                    0,
                    ElementType.LONG,
                    List.of(new BlockHash.GroupSpec(1, ElementType.BYTES_REF)),
                    blockFactory
                )
            ) {
                expectThrows(UnsupportedOperationException.class, () -> addAndCaptureGroupIds(hash, prefix, tail));
            } finally {
                Releasables.closeExpectNoException(prefix);
            }
        }
    }

    /**
     * Reproduces the actual measured shape from the PromQL wide-clause investigation: a series
     * (10-label tail tuple) recurring across several time buckets (the prefix), unchanged.
     * Verifies the dictionary design gives a real, measured memory win over the generic
     * flat-composite-key approach ({@link PackedValuesBlockHash}) - not just a theoretical one.
     */
    public void testMemoryUsageVsPackedValuesBlockHash() {
        final int distinctTails = 500;
        final int prefixesPerTail = 6;
        final int tailColumnCount = 10;
        final int labelPaddingLength = 30; // realistic label length, similar to the real hostmetrics dimensions measured

        final int rowCount = distinctTails * prefixesPerTail;
        long[] prefixValues = new long[rowCount];
        String[][] tailValues = new String[tailColumnCount][rowCount];
        int row = 0;
        for (int t = 0; t < distinctTails; t++) {
            for (int b = 0; b < prefixesPerTail; b++) {
                prefixValues[row] = b;
                for (int d = 0; d < tailColumnCount; d++) {
                    tailValues[d][row] = String.format(Locale.ROOT, "tuple-%05d-dim-%02d-%s", t, d, "x".repeat(labelPaddingLength));
                }
                row++;
            }
        }

        List<BlockHash.GroupSpec> tailSpecs = new ArrayList<>();
        for (int d = 0; d < tailColumnCount; d++) {
            tailSpecs.add(new BlockHash.GroupSpec(1 + d, ElementType.BYTES_REF));
        }
        List<BlockHash.GroupSpec> packedSpecs = new ArrayList<>();
        packedSpecs.add(new BlockHash.GroupSpec(0, ElementType.LONG));
        packedSpecs.addAll(tailSpecs);

        Block[] blocks = new Block[1 + tailColumnCount];
        blocks[0] = blockFactory.newLongArrayVector(prefixValues, rowCount).asBlock();
        for (int d = 0; d < tailColumnCount; d++) {
            blocks[1 + d] = bytesRefBlock(tailValues[d]);
        }

        long packedBytes;
        try (PackedValuesBlockHash packed = new PackedValuesBlockHash(packedSpecs, blockFactory, 1024)) {
            addAndCaptureGroupIds(packed, blocks);
            packedBytes = packed.bytesRefHash.ramBytesUsed();
        }

        long dictionaryBytes;
        try (PrefixBlockHash dict = new PrefixBlockHash(0, ElementType.LONG, tailSpecs, blockFactory)) {
            addAndCaptureGroupIds(dict, blocks);
            dictionaryBytes = dict.tailDictionary.ramBytesUsed() + dict.outerHash.ramBytesUsed();
        }

        Releasables.closeExpectNoException(blocks);

        logger.info(
            "PackedValuesBlockHash={} bytes, PrefixBlockHash={} bytes, ratio={}",
            packedBytes,
            dictionaryBytes,
            (double) packedBytes / dictionaryBytes
        );
        // Expect roughly a prefixesPerTail-fold reduction (6x here); assert a conservative 3x
        // floor so this doesn't flake on hash-table load-factor/rounding differences.
        assertThat((double) packedBytes / dictionaryBytes, greaterThan(3.0));
    }

    /**
     * Feeds each row's group id back into {@link BlockHash#getKeys} and checks the recovered
     * (prefix, tail1, tail2, ...) tuple matches the original row exactly - i.e. the dictionary
     * ordinal/outer-hash round trip is lossless.
     */
    private void assertRoundTrip(BlockHash hash, int[] groupIdsPerRow, Object[][] expectedPerRow) {
        try (IntVector selected = blockFactory.newIntRangeVector(0, hash.numKeys())) {
            Block[] keys = hash.getKeys(selected);
            try {
                for (int row = 0; row < groupIdsPerRow.length; row++) {
                    int groupId = groupIdsPerRow[row];
                    Object[] expected = expectedPerRow[row];
                    assertThat("prefix for row " + row, ((LongBlock) keys[0]).getLong(groupId), equalTo(expected[0]));
                    for (int d = 0; d < expected.length - 1; d++) {
                        Object expectedTail = expected[d + 1];
                        BytesRefBlock tailBlock = (BytesRefBlock) keys[1 + d];
                        if (expectedTail == null) {
                            assertThat("tail " + d + " for row " + row, tailBlock.isNull(groupId), equalTo(true));
                        } else {
                            assertThat(
                                "tail " + d + " for row " + row,
                                tailBlock.getBytesRef(groupId, new BytesRef()),
                                equalTo(new BytesRef((String) expectedTail))
                            );
                        }
                    }
                }
            } finally {
                Releasables.closeExpectNoException(keys);
            }
        }
    }

    private BytesRefBlock bytesRefBlock(String... values) {
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(values.length)) {
            for (String v : values) {
                builder.appendBytesRef(new BytesRef(v));
            }
            return builder.build();
        }
    }
}
