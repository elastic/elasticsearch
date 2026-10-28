/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

import org.elasticsearch.index.codec.tsdb.DocValuesForUtil;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.PayloadCodecStage;

import java.io.IOException;

public class BitPackCodecStageTests extends AbstractPayloadStageTestCase {

    @Override
    protected PayloadCodecStage createStage(int blockSize) {
        return new BitPackCodecStage(new DocValuesForUtil(blockSize));
    }

    public void testIdMatchesStageId() {
        assertEquals(StageId.BITPACK_PAYLOAD.id, new BitPackCodecStage(new DocValuesForUtil(128)).id());
    }

    public void testRoundTripRandomBitWidths() throws IOException {
        for (int bits = 0; bits <= 64; bits++) {
            final int blockSize = randomBlockSize();
            final long[] values = new long[blockSize];
            for (int i = 0; i < blockSize; i++) {
                values[i] = randomValueWithExactBits(bits);
            }
            assertPayloadRoundTrip(values);
        }
    }

    public void testMultiBlockRoundTripWithArrayReuse() throws IOException {
        final int blockSize = randomBlockSize();
        final int numBlocks = randomIntBetween(2, 5);
        final long[][] blocks = new long[numBlocks][blockSize];
        for (int b = 0; b < numBlocks; b++) {
            // NOTE: randomly include all-zeros blocks to verify bpv=0 path clears stale data
            final int bits = randomIntBetween(0, 64);
            for (int i = 0; i < blockSize; i++) {
                blocks[b][i] = randomValueWithExactBits(bits);
            }
        }
        assertMultiBlockPayloadRoundTrip(blocks);
    }

    public void testPartialBlockRoundTrip() throws IOException {
        for (int iter = 0; iter < 10; iter++) {
            final int blockSize = randomBlockSize();
            final int partialSize = randomIntBetween(1, blockSize - 1);
            final int bits = randomIntBetween(0, 64);
            final long[] values = new long[partialSize];
            for (int i = 0; i < partialSize; i++) {
                values[i] = randomValueWithExactBits(bits);
            }
            assertPayloadRoundTrip(values, partialSize, blockSize);
        }
    }
}
