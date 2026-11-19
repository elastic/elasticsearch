/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.RandomBlock;

import java.util.ArrayList;
import java.util.List;

public class TimeSeriesPartitionedBlockHashTests extends PartitionedBlockHashTestCase {
    @Override
    protected List<ElementType> keyTypes() {
        return randomBoolean() ? List.of(ElementType.BYTES_REF, ElementType.LONG) : List.of(ElementType.LONG, ElementType.BYTES_REF);
    }

    @Override
    protected PartitionedBlockHash newBlockHash(List<BlockHash.GroupSpec> groups, BlockFactory blockFactory, int emitBatchSize) {
        if (groups.get(0).elementType() == ElementType.BYTES_REF) {
            return new TimeSeriesBlockHash(0, 1, false, randomBoolean(), blockFactory);
        }
        return new TimeSeriesBlockHash(1, 0, true, randomBoolean(), blockFactory);
    }

    @Override
    protected List<Page> randomPages(BlockFactory blockFactory, List<BlockHash.GroupSpec> groups) {
        final int tsidChannel = groups.get(0).elementType() == ElementType.BYTES_REF ? 0 : 1;
        final boolean fixedLength = randomBoolean();
        BytesRef[] tsids = new BytesRef[between(1, 128)];
        for (int i = 0; i < tsids.length; i++) {
            tsids[i] = new BytesRef(randomByteArrayOfLength(fixedLength ? 16 : between(1, 24)));
        }
        long[] timestamps = new long[between(1, 32)];
        for (int i = 0; i < timestamps.length; i++) {
            timestamps[i] = randomNonNegativeLong();
        }
        int numPages = between(1, 20);
        List<Page> pages = new ArrayList<>(numPages);
        for (int i = 0; i < numPages; i++) {
            int positionCount = between(1, 1_000);
            Block[] blocks = new Block[3];
            try (
                var tsidBuilder = blockFactory.newBytesRefBlockBuilder(positionCount);
                var timestampBuilder = blockFactory.newLongBlockBuilder(positionCount)
            ) {
                for (int p = 0; p < positionCount; p++) {
                    tsidBuilder.appendBytesRef(randomFrom(tsids));
                    timestampBuilder.appendLong(timestamps[randomIntBetween(0, timestamps.length - 1)]);
                }
                blocks[tsidChannel] = tsidBuilder.build();
                blocks[1 - tsidChannel] = timestampBuilder.build();
            }
            blocks[2] = RandomBlock.randomBlock(blockFactory, ElementType.INT, positionCount, false, 0, 2, 0, 0).block();
            pages.add(new Page(blocks));
        }
        return pages;
    }
}
