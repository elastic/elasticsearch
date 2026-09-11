/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/**
 * Unit tests for {@link SlicedBlockRange}.
 */
public class SlicedBlockRangeTests extends ESTestCase {

    private static final int BULK_SIZE = 32;
    private static final long PER_VECTOR_BYTES = 48L;

    public void testEmptyRangeWhenStartDocBeyondEnd() throws IOException {
        // 64 vectors, docs [0..63], ask for range [100, 200) — no vectors in range
        KnnVectorValues values = sequentialVectorValues(64, 4);
        SlicedBlockRange range = SlicedBlockRange.compute(values, 100, 200, 64, BULK_SIZE, PER_VECTOR_BYTES);
        assertThat(range, equalTo(SlicedBlockRange.EMPTY));
    }

    public void testEmptyRangeWhenStartEqualsEnd() throws IOException {
        KnnVectorValues values = sequentialVectorValues(64, 4);
        SlicedBlockRange range = SlicedBlockRange.compute(values, 10, 10, 64, BULK_SIZE, PER_VECTOR_BYTES);
        assertThat(range, equalTo(SlicedBlockRange.EMPTY));
    }

    public void testSingleVectorInRange() throws IOException {
        // 64 vectors with sequential doc IDs, ask for range [5, 6) — one vector at ordinal 5
        KnnVectorValues values = sequentialVectorValues(64, 4);
        SlicedBlockRange range = SlicedBlockRange.compute(values, 5, 6, 64, BULK_SIZE, PER_VECTOR_BYTES);
        // Ordinal 5 is in block 0 (0..31). One block covers ordinals 0-31.
        assertThat(range.docBase(), equalTo(0));
        assertThat(range.vectors(), equalTo(BULK_SIZE));
        assertThat(range.skipBytes(), equalTo(0L));
    }

    public void testRangeSpanningAllVectors() throws IOException {
        // 64 vectors, ask for [0, 64) — everything
        KnnVectorValues values = sequentialVectorValues(64, 4);
        SlicedBlockRange range = SlicedBlockRange.compute(values, 0, 64, 64, BULK_SIZE, PER_VECTOR_BYTES);
        assertThat(range.docBase(), equalTo(0));
        assertThat(range.vectors(), equalTo(64));
        assertThat(range.skipBytes(), equalTo(0L));
    }

    public void testRangeStartingMidBlock() throws IOException {
        // 96 vectors (3 blocks of 32), ask for range [40, 96) — starts in block 1 (ords 32..63)
        KnnVectorValues values = sequentialVectorValues(96, 4);
        SlicedBlockRange range = SlicedBlockRange.compute(values, 40, 96, 96, BULK_SIZE, PER_VECTOR_BYTES);
        // minOrd=40, block 1. maxOrd=96 (NO_MORE_DOCS → size), endBlock = (96-1)/32 = 2 = totalBlocks → tail.
        // vectors = 96 - 1*32 = 64. docBase = 32. skipBytes = 32 * 48 = 1536.
        assertThat(range.docBase(), equalTo(32));
        assertThat(range.vectors(), equalTo(64));
        assertThat(range.skipBytes(), equalTo(32L * PER_VECTOR_BYTES));
    }

    public void testTailBlock() throws IOException {
        // 50 vectors (1 full block + 18 tail), ask for range [35, 50)
        // minOrd=35 → block 1, maxOrd=50 → endBlock = 49/32 = 1 = totalBlocks → tail path
        KnnVectorValues values = sequentialVectorValues(50, 4);
        SlicedBlockRange range = SlicedBlockRange.compute(values, 35, 50, 50, BULK_SIZE, PER_VECTOR_BYTES);
        // vectors = 50 - 1*32 = 18, docBase = 32, skipBytes = 32 * 48 = 1536
        assertThat(range.docBase(), equalTo(32));
        assertThat(range.vectors(), equalTo(18));
        assertThat(range.skipBytes(), equalTo(32L * PER_VECTOR_BYTES));
    }

    public void testClampingWhenRangeExceedsTotalVectors() throws IOException {
        // 32 vectors but totalVectors parameter is 16 (simulating a single posting list in a multi-centroid segment).
        // Range [0, 32) spans 32 ordinals but totalVectors=16 → clamping applies.
        KnnVectorValues values = sequentialVectorValues(32, 4);
        SlicedBlockRange range = SlicedBlockRange.compute(values, 0, 32, 16, BULK_SIZE, PER_VECTOR_BYTES);
        // minOrd=0, maxOrd would be 32 but clamped to min(32, 0+16)=16.
        // startBlock=0, endBlock=15/32=0, totalBlocks=16/32=0, endBlock==totalBlocks → tail path.
        // vectors = 16 - 0 = 16.
        assertThat(range.docBase(), equalTo(0));
        assertThat(range.vectors(), equalTo(16));
        assertThat(range.skipBytes(), equalTo(0L));
    }

    public void testMultipleFullBlocks() throws IOException {
        // 128 vectors (4 blocks), ask for range [32, 96) — blocks 1 and 2
        KnnVectorValues values = sequentialVectorValues(128, 4);
        SlicedBlockRange range = SlicedBlockRange.compute(values, 32, 96, 128, BULK_SIZE, PER_VECTOR_BYTES);
        // minOrd=32 → block 1, maxOrd=96 → endBlock = 95/32 = 2
        // vectors = (1 + 2 - 1) * 32 = 64, docBase = 32, skipBytes = 32 * 48
        assertThat(range.docBase(), equalTo(32));
        assertThat(range.vectors(), equalTo(64));
        assertThat(range.skipBytes(), equalTo(32L * PER_VECTOR_BYTES));
    }

    /**
     * Creates a {@link KnnVectorValues} with sequential doc IDs (ordinal i maps to doc i) and random float vectors.
     */
    private static KnnVectorValues sequentialVectorValues(int numVectors, int dimensions) throws IOException {
        List<float[]> vectors = new ArrayList<>(numVectors);
        for (int i = 0; i < numVectors; i++) {
            float[] v = new float[dimensions];
            for (int d = 0; d < dimensions; d++) {
                v[d] = random().nextFloat();
            }
            vectors.add(v);
        }
        return FloatVectorValues.fromFloats(vectors, dimensions);
    }
}
