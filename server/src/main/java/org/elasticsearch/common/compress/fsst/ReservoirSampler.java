/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.compress.fsst;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.elasticsearch.common.compress.fsst.FSST.FSST_SAMPLELINE;
import static org.elasticsearch.common.compress.fsst.FSST.FSST_SAMPLEMAXSZ;
import static org.elasticsearch.common.compress.fsst.FSST.FSST_SAMPLETARGET;

public class ReservoirSampler {
    private static final int SAMPLE_TARGET = FSST_SAMPLETARGET;
    private static final int SAMPLE_MAX = FSST_SAMPLEMAXSZ;
    private static final int SAMPLE_LINE = FSST_SAMPLELINE;
    private int numBytesInSample = 0;
    private int numChunksSeen = 0;
    private final Random random = new Random(1234);
    private List<byte[]> sample = new ArrayList<>();

    public List<byte[]> getSample() {
        return sample;
    }

    // The byte array is only valid during this call, thus bytes need to be deep copied
    public void processLine(byte[] bytes, int offset, int length) {
        if (length == 0) {
            return;
        }

        // iterate over the chunks
        int numChunks = length / SAMPLE_LINE + (length % SAMPLE_LINE == 0 ? 0 : 1);
        for (int c = 0; c < numChunks; ++c) {
            numChunksSeen++;
            int chunkOffset = c * SAMPLE_LINE;
            int chunkLen = c == numChunks - 1 ? length - chunkOffset : SAMPLE_LINE;

            if (numBytesInSample < SAMPLE_TARGET + SAMPLE_LINE) {
                // If the reservoir isn't full, just add to it.
                // This will occur on startup, but also if a recent swap caused us to go below the target.
                // Add a buffer of an additional sample line, so that one swap doesn't cause us to fall below target.
                byte[] chunkBytes = Arrays.copyOfRange(bytes, offset + chunkOffset, offset + chunkOffset + chunkLen);
                sample.add(chunkBytes);
                numBytesInSample += chunkBytes.length;
            } else {
                int p = random.nextInt(numChunksSeen);
                if (p < sample.size()) {
                    // swap for an existing value
                    byte[] toAdd = Arrays.copyOfRange(bytes, offset + chunkOffset, offset + chunkOffset + chunkLen);
                    byte[] toRemove = sample.get(p);
                    numBytesInSample -= toRemove.length;
                    numBytesInSample += toAdd.length;
                    sample.set(p, toAdd);

                    // Sample could now be too small if we swapped a small chunk for a big one.
                    // This will be rectified as the next chunk will just be added to the sample, in the if-block above

                    // But if the sample is too large (from swapping big samples for small samples),
                    // we need to discard some
                    while (numBytesInSample > SAMPLE_MAX) {
                        numBytesInSample -= sample.removeLast().length;
                    }
                }
            }
        }
    }
}
