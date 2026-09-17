/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.util.LongValues;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests the block lookup of {@link AbstractTSDBDocValuesProducer.BinaryDecoder} over block doc starts that contain runs of
 * equal values, which is how the zero-doc continuation blocks of a split binary value show up.
 */
public class BinaryDecoderFindBlockTests extends ESTestCase {

    public void testFindBlockWithContinuationBlocks() {
        // blocks: [0,3) [3,4) cont cont [4,6) [6,7) cont
        long[] blockDocStarts = new long[] { 0, 3, 4, 4, 4, 6, 7, 7 };
        int numBlocks = blockDocStarts.length - 1;
        LongValues values = asLongValues(blockDocStarts);

        assertEquals(0, AbstractTSDBDocValuesProducer.BinaryDecoder.findBlock(values, 0, numBlocks, 0));
        assertEquals(0, AbstractTSDBDocValuesProducer.BinaryDecoder.findBlock(values, 2, numBlocks, 0));
        assertEquals(1, AbstractTSDBDocValuesProducer.BinaryDecoder.findBlock(values, 3, numBlocks, 0));
        assertEquals(4, AbstractTSDBDocValuesProducer.BinaryDecoder.findBlock(values, 4, numBlocks, 0));
        assertEquals(4, AbstractTSDBDocValuesProducer.BinaryDecoder.findBlock(values, 5, numBlocks, 2));
        assertEquals(5, AbstractTSDBDocValuesProducer.BinaryDecoder.findBlock(values, 6, numBlocks, 0));
        // searching from after the doc's block reports the block before the range
        assertEquals(1, AbstractTSDBDocValuesProducer.BinaryDecoder.findBlock(values, 3, numBlocks, 2));
    }

    public void testFindBlockRandom() {
        List<Long> starts = new ArrayList<>();
        long doc = 0;
        int numBlocks = randomIntBetween(1, 200);
        for (int i = 0; i < numBlocks; i++) {
            starts.add(doc);
            // blocks after the first may be continuation blocks holding no docs
            doc += i > 0 && randomBoolean() && starts.get(i - 1) != doc ? 0 : randomIntBetween(1, 5);
        }
        starts.add(doc);
        long[] blockDocStarts = starts.stream().mapToLong(Long::longValue).toArray();
        LongValues values = asLongValues(blockDocStarts);

        for (int d = 0; d < doc; d++) {
            long expected = -1;
            for (int b = 0; b < numBlocks; b++) {
                if (blockDocStarts[b] <= d && d < blockDocStarts[b + 1]) {
                    expected = b;
                }
            }
            long from = randomLongBetween(0, expected);
            assertEquals("doc " + d, expected, AbstractTSDBDocValuesProducer.BinaryDecoder.findBlock(values, d, numBlocks, from));
        }
    }

    private static LongValues asLongValues(long[] values) {
        return new LongValues() {
            @Override
            public long get(long index) {
                return values[Math.toIntExact(index)];
            }
        };
    }
}
