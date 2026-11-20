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
import org.elasticsearch.index.codec.vectors.diskbbq.IvfSegmentConfig;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * Unit tests for the merge-time projection-matrix selection rule
 * ({@link ESNextDiskASHVectorsWriter#selectInheritanceCandidate}). The rule decides which input
 * segment's learned matrix is inherited (warm-started) into the merged segment.
 */
public class ESNextDiskASHVectorsWriterTests extends ESTestCase {

    private static AshProjectionMatrix learned(int originalDim, int nDims, float fill) {
        float[] wT = new float[originalDim * nDims];
        Arrays.fill(wT, fill);
        return AshProjectionMatrix.of(wT, originalDim, nDims, true, UUID.randomUUID(), 0);
    }

    private static AshProjectionMatrix random(int originalDim, int nDims, float fill) {
        float[] wT = new float[originalDim * nDims];
        Arrays.fill(wT, fill);
        return AshProjectionMatrix.of(wT, originalDim, nDims, false, UUID.randomUUID(), 0);
    }

    /** An input segment of {@code size} vectors whose projection matrix is {@code matrix}. */
    private static ESNextDiskASHVectorsWriter.InheritanceCandidate candidate(AshProjectionMatrix matrix, int size) {
        AshProjectionMatrix.Header header = new AshProjectionMatrix.Header(
            matrix.originalDim(),
            matrix.nDims(),
            matrix.isLearned(),
            matrix.isSeeded(),
            matrix.id(),
            matrix.trainingSampleSize()
        );
        // the selection only uses the header and the size
        return new ESNextDiskASHVectorsWriter.InheritanceCandidate(header, size, null, null);
    }

    /** The id of the projection matrix the selection inherits, or {@code null} if none. */
    private static UUID selected(
        int expectedDim,
        int expectedProjectedDims,
        List<ESNextDiskASHVectorsWriter.InheritanceCandidate> candidates
    ) {
        var selected = ESNextDiskASHVectorsWriter.selectInheritanceCandidate(expectedDim, expectedProjectedDims, candidates);
        return selected == null ? null : selected.header().id();
    }

    public void testReturnsNullWhenNoCandidates() {
        assertNull(selected(4, 2, List.of()));
    }

    public void testPrefersLargestLearnedSegment() {
        AshProjectionMatrix small = learned(4, 2, 1.0f);
        AshProjectionMatrix large = learned(4, 2, 2.0f);
        var candidates = List.of(candidate(small, 10), candidate(large, 100));
        assertEquals(large.id(), selected(4, 2, candidates));
    }

    public void testSkipsRandomProjections() {
        AshProjectionMatrix largeRandom = random(4, 2, 9.0f);
        AshProjectionMatrix smallLearned = learned(4, 2, 1.0f);
        var candidates = List.of(
            // The random one is bigger but must be skipped so a random rotation is not inherited.
            candidate(largeRandom, 1000),
            candidate(smallLearned, 10)
        );
        assertEquals(smallLearned.id(), selected(4, 2, candidates));
    }

    public void testReturnsNullWhenAllRandom() {
        var candidates = List.of(candidate(random(4, 2, 1.0f), 10), candidate(random(4, 2, 2.0f), 20));
        assertNull(selected(4, 2, candidates));
    }

    public void testSkipsDimensionMismatch() {
        AshProjectionMatrix wrongDim = learned(8, 2, 1.0f);
        AshProjectionMatrix rightDim = learned(4, 2, 2.0f);
        var candidates = List.of(candidate(wrongDim, 1000), candidate(rightDim, 10));
        assertEquals(rightDim.id(), selected(4, 2, candidates));
    }

    public void testSkipsProjectedDimensionMismatch() {
        AshProjectionMatrix wrongProjectedDims = learned(4, 3, 1.0f);
        AshProjectionMatrix rightProjectedDims = learned(4, 2, 2.0f);
        var candidates = List.of(candidate(wrongProjectedDims, 1000), candidate(rightProjectedDims, 10));
        assertEquals(rightProjectedDims.id(), selected(4, 2, candidates));
    }

    public void testInheritsProjectionOnlyWhenThereIsSomethingToInherit() {
        // without reduction and without Procrustes iterations the projection is the same seeded rotation everywhere
        int dims = randomIntBetween(2, 4096);
        var fullRotation = IvfSegmentConfig.AshConfig.of(2, 4, 1.0f, Integer.MAX_VALUE);
        assertFalse(ESNextDiskASHVectorsWriter.inheritsProjection(fullRotation, dims));

        // Procrustes iterations refine even a full rotation
        var refinedRotation = new IvfSegmentConfig.AshConfig(1.0f, Integer.MAX_VALUE, 2, 4, 2, 10);
        assertTrue(ESNextDiskASHVectorsWriter.inheritsProjection(refinedRotation, dims));

        // a reduced projection is learned from the data
        var halfProjection = IvfSegmentConfig.AshConfig.of(2, 4, 0.5f, Integer.MAX_VALUE);
        assertTrue(ESNextDiskASHVectorsWriter.inheritsProjection(halfProjection, dims));
    }

    public void testReturnsNullWhenAllDimensionMismatch() {
        var candidates = List.of(candidate(learned(8, 2, 1.0f), 10));
        assertNull(selected(4, 2, candidates));
    }
}
