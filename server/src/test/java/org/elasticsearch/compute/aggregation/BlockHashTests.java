/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefArrayBlock;
import org.elasticsearch.compute.data.LongArrayBlock;
import org.elasticsearch.test.ESTestCase;

public class BlockHashTests extends ESTestCase {

    public void testBasicLongHash() {
        long[] values = new long[] { 2, 1, 4, 2, 4, 1, 3, 4 };
        Block block = new LongArrayBlock(values, values.length);
        try (BlockHash longHash = BlockHash.newLongHash(BigArrays.NON_RECYCLING_INSTANCE)) {
            assertEquals(0, longHash.add(block, 0));
            assertEquals(1, longHash.add(block, 1));
            assertEquals(2, longHash.add(block, 2));
            assertEquals(-1, longHash.add(block, 3));
            assertEquals(-3, longHash.add(block, 4));
            assertEquals(-2, longHash.add(block, 5));
            assertEquals(3, longHash.add(block, 6));
            assertEquals(-3, longHash.add(block, 7));

            Block keysBlock = longHash.getKeys();
            long[] expectedKeys = new long[] { 2, 1, 4, 3 };
            assertEquals(expectedKeys.length, keysBlock.getPositionCount());
            for (int i = 0; i < expectedKeys.length; i++) {
                assertEquals(expectedKeys[i], keysBlock.getLong(i));
            }
        }
    }

    public void testBasicBytesRefHash() {
        BytesRefArrayBlock.Builder builder = BytesRefArrayBlock.builder(8);
        builder.append(new BytesRef("item-2"));
        builder.append(new BytesRef("item-1"));
        builder.append(new BytesRef("item-4"));
        builder.append(new BytesRef("item-2"));
        builder.append(new BytesRef("item-4"));
        builder.append(new BytesRef("item-1"));
        builder.append(new BytesRef("item-3"));
        builder.append(new BytesRef("item-4"));

        Block block = builder.build();
        try (BlockHash longHash = BlockHash.newBytesRefHash(BigArrays.NON_RECYCLING_INSTANCE)) {
            assertEquals(0, longHash.add(block, 0));
            assertEquals(1, longHash.add(block, 1));
            assertEquals(2, longHash.add(block, 2));
            assertEquals(-1, longHash.add(block, 3));
            assertEquals(-3, longHash.add(block, 4));
            assertEquals(-2, longHash.add(block, 5));
            assertEquals(3, longHash.add(block, 6));
            assertEquals(-3, longHash.add(block, 7));

            Block keysBlock = longHash.getKeys();
            BytesRef[] expectedKeys = new BytesRef[] {
                new BytesRef("item-2"),
                new BytesRef("item-1"),
                new BytesRef("item-4"),
                new BytesRef("item-3") };
            assertEquals(expectedKeys.length, keysBlock.getPositionCount());
            for (int i = 0; i < expectedKeys.length; i++) {
                assertEquals(expectedKeys[i], keysBlock.getBytesRef(i, new BytesRef()));
            }
        }
    }
}
