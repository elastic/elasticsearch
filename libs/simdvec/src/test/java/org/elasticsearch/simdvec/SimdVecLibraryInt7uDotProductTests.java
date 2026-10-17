/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static org.hamcrest.Matchers.containsString;

public class SimdVecLibraryInt7uDotProductTests extends SimdVecLibraryInt7uTests {

    public SimdVecLibraryInt7uDotProductTests(int size) {
        super(SimdVecLibrary.SimilarityFunction.DOT_PRODUCT, size);
    }

    public void testInt7uBulkMasked() {
        assumeTrue(notSupportedMsg(), supported());
        doTestInt7uBulkMasked(0, false);
    }

    public void testInt7uBulkMaskedWithPitch() {
        assumeTrue(notSupportedMsg(), supported());
        // Mimics extra data at the end
        doTestInt7uBulkMasked(Float.BYTES, false);
    }

    public void testInt7uBulkMaskedHeapSegments() {
        assumeTrue(notSupportedMsg(), supported());
        doTestInt7uBulkMasked(0, true);
    }

    private void doTestInt7uBulkMasked(int extraData, boolean heapQueryAndScores) {
        final int dims = size;
        final int pitch = dims + extraData;
        final int count = randomIntBetween(1, Integer.SIZE);
        final int mask = randomBulkMask(count);

        var vectors = new byte[count][dims];
        var vectorsSegment = arena.allocate((long) count * pitch);
        for (int i = 0; i < count; i++) {
            randomBytesBetween(vectors[i], MIN_INT7_VALUE, MAX_INT7_VALUE);
            MemorySegment.copy(vectors[i], 0, vectorsSegment, ValueLayout.JAVA_BYTE, (long) i * pitch, dims);
        }
        var query = new byte[dims];
        randomBytesBetween(query, MIN_INT7_VALUE, MAX_INT7_VALUE);

        float[] expectedScores = unselectedScores(count);
        ScalarOperations.bulkMasked(function, query, vectors, mask, expectedScores);

        if (heapQueryAndScores) {
            float[] bulkScores = unselectedScores(count);
            getVectorDistance().dotProductI7uBulkMasked(
                vectorsSegment,
                MemorySegment.ofArray(query),
                dims,
                pitch,
                mask,
                count,
                MemorySegment.ofArray(bulkScores)
            );
            assertArrayEquals(expectedScores, bulkScores, 0f);
        } else {
            var nativeQuerySeg = arena.allocate(dims);
            MemorySegment.copy(query, 0, nativeQuerySeg, ValueLayout.JAVA_BYTE, 0L, dims);
            var bulkScoresSeg = unselectedScoresSegment(count);
            getVectorDistance().dotProductI7uBulkMasked(vectorsSegment, nativeQuerySeg, dims, pitch, mask, count, bulkScoresSeg);
            assertScoresEquals(expectedScores, bulkScoresSeg);
        }
    }

    public void testBulkMaskedIllegalArgs() {
        assumeTrue(notSupportedMsg(), supported());
        final int dims = size;
        final int count = 3;
        var vectorsSegment = arena.allocate((long) dims * count);
        var query = arena.allocate(dims);
        var scores = arena.allocate((long) Integer.SIZE * Float.BYTES);

        // a vector past the end of the block is selected
        Exception ex = expectThrows(
            IAE,
            () -> getVectorDistance().dotProductI7uBulkMasked(vectorsSegment, query, dims, dims, 1 << count, count, scores)
        );
        assertThat(ex.getMessage(), containsString("selects vectors beyond count"));

        // a block larger than the mask
        var largeVectorsSegment = arena.allocate((long) dims * (Integer.SIZE + 1));
        var largeScores = arena.allocate((long) (Integer.SIZE + 1) * Float.BYTES);
        ex = expectThrows(
            IOOBE,
            () -> getVectorDistance().dotProductI7uBulkMasked(largeVectorsSegment, query, dims, dims, 1, Integer.SIZE + 1, largeScores)
        );
        assertThat(ex.getMessage(), containsString("out of bounds for length"));

        // a block larger than the dataset
        ex = expectThrows(
            IOOBE,
            () -> getVectorDistance().dotProductI7uBulkMasked(vectorsSegment, query, dims, dims, 1, count + 1, scores)
        );
        assertThat(ex.getMessage(), containsString("out of bounds for length"));
    }
}
