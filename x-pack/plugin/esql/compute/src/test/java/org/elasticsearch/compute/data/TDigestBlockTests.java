/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.equalTo;

public class TDigestBlockTests extends ComputeTestCase {

    public void testPopulatedBlockSerialization() throws IOException {
        TDigestBlock block = randomBlockWithNulls();
        Block deserializedBlock = serializationRoundTrip(block);
        assertThat(deserializedBlock, equalTo(block));
        Releasables.close(block, deserializedBlock);
    }

    public void testNullSerialization() throws IOException {
        int elementCount = randomIntBetween(1, 100);

        Block block = new TDigestArrayBlock(
            (BytesRefBlock) blockFactory().newConstantNullBlock(elementCount),
            (DoubleBlock) blockFactory().newConstantNullBlock(elementCount),
            (DoubleBlock) blockFactory().newConstantNullBlock(elementCount),
            (DoubleBlock) blockFactory().newConstantNullBlock(elementCount),
            (LongBlock) blockFactory().newConstantNullBlock(elementCount),
            elementCount,
            null
        );

        Block deserializedBlock = serializationRoundTrip(block);
        assertThat(deserializedBlock, equalTo(block));
        Releasables.close(block, deserializedBlock);
    }

    private Block serializationRoundTrip(Block block) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        Block.writeTypedBlock(block, out);
        try (BlockStreamInput input = new BlockStreamInput(out.bytes().streamInput(), blockFactory())) {
            return Block.readTypedBlock(input);
        }
    }

    public void testComponentAccess() {
        TDigestBlock block;
        if (randomBoolean()) {
            block = randomBlockWithNulls();
        } else {
            block = (TDigestBlock) blockFactory().newConstantNullBlock(randomIntBetween(1, 100));
        }
        for (HistogramBlock.Component component : HistogramBlock.Component.values()) {
            Block componentBlock = block.buildHistogramComponentBlock(component);
            assertThat(componentBlock.getPositionCount(), equalTo(block.getPositionCount()));
            for (int i = 0; i < block.getPositionCount(); i++) {
                if (block.isNull(i)) {
                    assertThat(componentBlock.isNull(i), equalTo(true));
                } else {
                    TDigestHolder histo = block.getTDigestHolder(i, new TDigestHolder());
                    switch (component) {
                        case MIN -> {
                            double expectedMin = histo.getMin();
                            if (Double.isNaN(expectedMin)) {
                                assertThat(componentBlock.isNull(i), equalTo(true));
                            } else {
                                assertThat(componentBlock.getValueCount(i), equalTo(1));
                                int valueIndex = componentBlock.getFirstValueIndex(i);
                                assertThat(((DoubleBlock) componentBlock).getDouble(valueIndex), equalTo(expectedMin));
                            }
                        }
                        case MAX -> {
                            double expectedMax = histo.getMax();
                            if (Double.isNaN(expectedMax)) {
                                assertThat(componentBlock.isNull(i), equalTo(true));
                            } else {
                                assertThat(componentBlock.getValueCount(i), equalTo(1));
                                int valueIndex = componentBlock.getFirstValueIndex(i);
                                assertThat(((DoubleBlock) componentBlock).getDouble(valueIndex), equalTo(expectedMax));
                            }
                        }
                        case SUM -> {
                            if (histo.size() == 0) {
                                assertThat(componentBlock.isNull(i), equalTo(true));
                            } else {
                                assertThat(componentBlock.getValueCount(i), equalTo(1));
                                int valueIndex = componentBlock.getFirstValueIndex(i);
                                assertThat(((DoubleBlock) componentBlock).getDouble(valueIndex), equalTo(histo.getSum()));
                            }
                        }
                        case COUNT -> {
                            assertThat(componentBlock.getValueCount(i), equalTo(1));
                            int valueIndex = componentBlock.getFirstValueIndex(i);
                            assertThat(((DoubleBlock) componentBlock).getDouble(valueIndex), equalTo((double) histo.size()));
                        }
                    }
                }
            }
            Releasables.close(componentBlock);
        }
        Releasables.close(block);
    }

    public void testEmptyBlockEquality() {
        List<Block> blocks = List.of(
            blockFactory().newConstantNullBlock(0),
            blockFactory().newTDigestBlockBuilder(0).build(),
            filterAndRelease(blockFactory().newTDigestBlockBuilder(0).appendNull().build()),
            filterAndRelease(blockFactory().newTDigestBlockBuilder(0).appendTDigest(BlockTestUtils.randomTDigest()).build())
        );
        for (Block a : blocks) {
            for (Block b : blocks) {
                assertThat(a, equalTo(b));
                assertThat(a.hashCode(), equalTo(b.hashCode()));
            }
        }
        Releasables.close(blocks);
    }

    public void testNullValuesEquality() {
        List<Block> blocks = List.of(
            blockFactory().newConstantNullBlock(2),
            blockFactory().newTDigestBlockBuilder(0).appendNull().appendNull().build()
        );
        for (Block a : blocks) {
            for (Block b : blocks) {
                assertThat(a, equalTo(b));
                assertThat(a.hashCode(), equalTo(b.hashCode()));
            }
        }
        Releasables.close(blocks);
    }

    public void testFilteredBlockEquality() {
        TDigestHolder digest1 = BlockTestUtils.randomTDigest();
        TDigestHolder digest2 = BlockTestUtils.randomTDigest();
        Block block1 = blockFactory().newTDigestBlockBuilder(0)
            .appendTDigest(digest1)
            .appendTDigest(digest1)
            .appendTDigest(digest2)
            .build();

        Block block2 = blockFactory().newTDigestBlockBuilder(0)
            .appendTDigest(digest1)
            .appendTDigest(digest2)
            .appendTDigest(digest2)
            .build();

        Block block1Filtered = block1.filter(false, 1, 2);
        Block block2Filtered = block2.filter(false, 0, 1);

        assertThat(block1, not(equalTo(block2)));
        assertThat(block1, not(equalTo(block1Filtered)));
        assertThat(block1, not(equalTo(block2Filtered)));
        assertThat(block2, not(equalTo(block1)));
        assertThat(block2, not(equalTo(block1Filtered)));
        assertThat(block2, not(equalTo(block2Filtered)));

        assertThat(block1Filtered, equalTo(block2Filtered));
        assertThat(block1Filtered.hashCode(), equalTo(block2Filtered.hashCode()));

        Releasables.close(block1, block2, block1Filtered, block2Filtered);
    }

    public void testRandomBlockEquality() {
        int positionCount = randomIntBetween(0, 10_000);
        Block tdigestBlock = RandomBlock.randomBlock(blockFactory(), ElementType.TDIGEST, positionCount, true, 1, 10, 0, 10).block();
        Block copy = BlockUtils.deepCopyOf(tdigestBlock, blockFactory());

        assertThat(tdigestBlock, equalTo(copy));
        assertThat(tdigestBlock.hashCode(), equalTo(copy.hashCode()));

        Releasables.close(tdigestBlock, copy);
    }

    public void testMultiValueBlock() {
        TDigestHolder digest1 = BlockTestUtils.randomTDigest();
        TDigestHolder digest2 = BlockTestUtils.randomTDigest();
        TDigestHolder digest3 = BlockTestUtils.randomTDigest();

        TDigestBlock.Builder builder = blockFactory().newTDigestBlockBuilder(3);
        builder.beginPositionEntry();
        builder.appendTDigest(digest1);
        builder.appendTDigest(digest2);
        builder.endPositionEntry();
        builder.appendNull();
        builder.beginPositionEntry();
        builder.appendTDigest(digest3);
        builder.appendTDigest(digest1);
        builder.appendTDigest(digest2);
        builder.endPositionEntry();
        TDigestBlock block = builder.build();

        assertThat(block.getPositionCount(), equalTo(3));
        assertThat(block.getValueCount(0), equalTo(2));
        assertThat(block.getValueCount(1), equalTo(0));
        assertThat(block.getValueCount(2), equalTo(3));
        assertThat(block.isNull(0), equalTo(false));
        assertThat(block.isNull(1), equalTo(true));
        assertThat(block.isNull(2), equalTo(false));
        assertThat(block.mayHaveMultivaluedFields(), equalTo(true));
        assertThat(block.doesHaveMultivaluedFields(), equalTo(true));

        TDigestHolder scratch = new TDigestHolder();
        int firstValueIndex0 = block.getFirstValueIndex(0);
        assertThat(block.getTDigestHolder(firstValueIndex0, scratch).size(), equalTo(digest1.size()));
        assertThat(block.getTDigestHolder(firstValueIndex0 + 1, scratch).size(), equalTo(digest2.size()));

        int firstValueIndex2 = block.getFirstValueIndex(2);
        assertThat(block.getTDigestHolder(firstValueIndex2, scratch).size(), equalTo(digest3.size()));
        assertThat(block.getTDigestHolder(firstValueIndex2 + 1, scratch).size(), equalTo(digest1.size()));
        assertThat(block.getTDigestHolder(firstValueIndex2 + 2, scratch).size(), equalTo(digest2.size()));

        Releasables.close(block);
    }

    public void testMultiValueBlockSerialization() throws IOException {
        TDigestHolder digest1 = BlockTestUtils.randomTDigest();
        TDigestHolder digest2 = BlockTestUtils.randomTDigest();

        TDigestBlock.Builder builder = blockFactory().newTDigestBlockBuilder(2);
        builder.beginPositionEntry();
        builder.appendTDigest(digest1);
        builder.appendTDigest(digest2);
        builder.endPositionEntry();
        builder.appendTDigest(digest1);
        TDigestBlock block = builder.build();

        Block deserializedBlock = serializationRoundTrip(block);
        assertThat(deserializedBlock, equalTo(block));

        Releasables.close(block, deserializedBlock);
    }

    private static Block filterAndRelease(Block toFilterAndRelease) {
        Block filtered = toFilterAndRelease.filter(false);
        toFilterAndRelease.close();
        return filtered;
    }

    private TDigestBlock randomBlockWithNulls() {
        int elementCount = randomIntBetween(0, 100);
        TDigestBlockBuilder builder = blockFactory().newTDigestBlockBuilder(elementCount);
        for (int i = 0; i < elementCount; i++) {
            if (randomBoolean()) {
                builder.appendNull();
            } else {
                builder.appendTDigest(BlockTestUtils.randomTDigest());
            }
        }
        return builder.build();
    }
}
