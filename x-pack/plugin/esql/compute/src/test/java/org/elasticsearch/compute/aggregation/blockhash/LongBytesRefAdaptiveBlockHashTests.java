/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.ComputeTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class LongBytesRefAdaptiveBlockHashTests extends ComputeTestCase {

    public void testAddAfterLimitReached() {

        BlockFactory blockFactory = blockFactory();
        Map<Key, Integer> ords = new HashMap<>();
        try (
            BlockHash hash = new LongBytesRefAdaptiveBlockHash(
                List.of(new BlockHash.GroupSpec(0, ElementType.LONG), new BlockHash.GroupSpec(1, ElementType.BYTES_REF)),
                blockFactory,
                between(1024, 16 * 1024),
                false
            )
        ) {

            int numPages = between(0, 2);
            int nextOrd = 0;
            for (int i = 0; i < numPages; i++) {
                int positionCount = randomIntBetween(1, 10);
                try (
                    var bytesBuilder = blockFactory.newBytesRefBlockBuilder(positionCount);
                    var longsBuilder = blockFactory.newLongBlockBuilder(positionCount)
                ) {
                    for (int p = 0; p < positionCount; p++) {
                        long longValue = randomIntBetween(1, 10);
                        longsBuilder.appendLong(longValue);
                        BytesRef bytesRef = new BytesRef("v-" + randomIntBetween(1, 10));
                        bytesBuilder.appendBytesRef(bytesRef);
                        Key key = new Key(longValue, bytesRef);
                        if (ords.containsKey(key) == false) {
                            ords.put(key, nextOrd++);
                        }
                    }
                    try (var bytesBlock = bytesBuilder.build(); var longsBlock = longsBuilder.build();) {
                        hash.add(new Page(longsBlock, bytesBlock), addInput(ords, false, longsBlock, bytesBlock));
                    }
                }
            }
            numPages = between(1, 5);
            for (int i = 0; i < numPages; i++) {
                int positionCount = randomIntBetween(1, 10);
                try (
                    var bytesBuilder = blockFactory.newBytesRefBlockBuilder(positionCount);
                    var longsBuilder = blockFactory.newLongBlockBuilder(positionCount)
                ) {
                    for (int p = 0; p < positionCount; p++) {
                        long longValue = randomIntBetween(1, 50);
                        longsBuilder.appendLong(longValue);
                        BytesRef bytesRef = new BytesRef("v-" + randomIntBetween(1, 50));
                        bytesBuilder.appendBytesRef(bytesRef);
                    }
                    final int numKeys = hash.numKeys();
                    try (var bytesBlock = bytesBuilder.build(); var longsBlock = longsBuilder.build();) {
                        hash.addAfterLimitReached(new Page(longsBlock, bytesBlock), addInput(ords, true, longsBlock, bytesBlock));
                        assertThat(hash.numKeys(), equalTo(numKeys));
                    }
                }
            }
        }
    }

    GroupingAggregatorFunction.AddInput addInput(
        Map<Key, Integer> expectedOrds,
        boolean afterLimit,
        LongBlock longBlock,
        BytesRefBlock bytesBlock
    ) {
        return new GroupingAggregatorFunction.AddInput() {
            @Override
            public void add(int positionOffset, IntArrayBlock groupIds) {
                assertTrue(afterLimit);
                BytesRef bytesRef = new BytesRef();
                for (int p = 0; p < groupIds.getPositionCount(); p++) {
                    int valuePosition = positionOffset + p;
                    Key key = new Key(longBlock.getLong(valuePosition), bytesBlock.getBytesRef(valuePosition, bytesRef));
                    if (expectedOrds.containsKey(key)) {
                        assertFalse(groupIds.isNull(p));
                        int groupId = groupIds.getInt(groupIds.getFirstValueIndex(p));
                        assertThat(expectedOrds.get(key), equalTo(groupId));
                    } else {
                        assertTrue(groupIds.isNull(p));
                    }
                }
            }

            @Override
            public void add(int positionOffset, IntBigArrayBlock groupIds) {
                fail("should not be called");
            }

            @Override
            public void add(int positionOffset, IntVector groupIds) {
                BytesRef bytesRef = new BytesRef();
                for (int p = 0; p < groupIds.getPositionCount(); p++) {
                    int valuePosition = positionOffset + p;
                    Key key = new Key(longBlock.getLong(valuePosition), bytesBlock.getBytesRef(valuePosition, bytesRef));
                    int groupId = groupIds.getInt(p);
                    assertThat(expectedOrds.get(key), equalTo(groupId));
                }
            }

            @Override
            public void close() {

            }
        };
    }

    record Key(long longValue, BytesRef bytes) {

    }

}
