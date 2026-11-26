/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.next;

import org.elasticsearch.index.codec.vectors.ash.AshProjectionMatrix;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

/**
 * Unit tests for the merge-time projection-matrix selection rule
 * ({@link ESNextDiskASHVectorsWriter#selectInheritedProjectionMatrix}). The rule decides which input
 * segment's learned W^T is inherited (warm-started) into the merged segment.
 */
public class ESNextDiskASHVectorsWriterTests extends ESTestCase {

    private static AshProjectionMatrix learned(int originalDim, int nDims, float fill) {
        float[] wT = new float[originalDim * nDims];
        java.util.Arrays.fill(wT, fill);
        return new AshProjectionMatrix(wT, originalDim, nDims, true);
    }

    private static AshProjectionMatrix random(int originalDim, int nDims, float fill) {
        float[] wT = new float[originalDim * nDims];
        java.util.Arrays.fill(wT, fill);
        return new AshProjectionMatrix(wT, originalDim, nDims, false);
    }

    public void testReturnsNullWhenNoCandidates() {
        assertNull(ESNextDiskASHVectorsWriter.selectInheritedProjectionMatrix(4, List.of()));
    }

    public void testPrefersLargestLearnedSegment() {
        AshProjectionMatrix small = learned(4, 2, 1.0f);
        AshProjectionMatrix large = learned(4, 2, 2.0f);
        var candidates = List.of(
            new ESNextDiskASHVectorsWriter.SizedProjectionMatrix(small, 10),
            new ESNextDiskASHVectorsWriter.SizedProjectionMatrix(large, 100)
        );
        assertSame(large.wT(), ESNextDiskASHVectorsWriter.selectInheritedProjectionMatrix(4, candidates));
    }

    public void testSkipsRandomProjections() {
        AshProjectionMatrix largeRandom = random(4, 2, 9.0f);
        AshProjectionMatrix smallLearned = learned(4, 2, 1.0f);
        var candidates = List.of(
            // The random one is bigger but must be skipped so a random rotation is not inherited.
            new ESNextDiskASHVectorsWriter.SizedProjectionMatrix(largeRandom, 1000),
            new ESNextDiskASHVectorsWriter.SizedProjectionMatrix(smallLearned, 10)
        );
        assertSame(smallLearned.wT(), ESNextDiskASHVectorsWriter.selectInheritedProjectionMatrix(4, candidates));
    }

    public void testReturnsNullWhenAllRandom() {
        var candidates = List.of(
            new ESNextDiskASHVectorsWriter.SizedProjectionMatrix(random(4, 2, 1.0f), 10),
            new ESNextDiskASHVectorsWriter.SizedProjectionMatrix(random(4, 2, 2.0f), 20)
        );
        assertNull(ESNextDiskASHVectorsWriter.selectInheritedProjectionMatrix(4, candidates));
    }

    public void testSkipsDimensionMismatch() {
        AshProjectionMatrix wrongDim = learned(8, 2, 1.0f);
        AshProjectionMatrix rightDim = learned(4, 2, 2.0f);
        var candidates = List.of(
            new ESNextDiskASHVectorsWriter.SizedProjectionMatrix(wrongDim, 1000),
            new ESNextDiskASHVectorsWriter.SizedProjectionMatrix(rightDim, 10)
        );
        assertSame(rightDim.wT(), ESNextDiskASHVectorsWriter.selectInheritedProjectionMatrix(4, candidates));
    }

    public void testReturnsNullWhenAllDimensionMismatch() {
        var candidates = List.of(new ESNextDiskASHVectorsWriter.SizedProjectionMatrix(learned(8, 2, 1.0f), 10));
        assertNull(ESNextDiskASHVectorsWriter.selectInheritedProjectionMatrix(4, candidates));
    }
}
