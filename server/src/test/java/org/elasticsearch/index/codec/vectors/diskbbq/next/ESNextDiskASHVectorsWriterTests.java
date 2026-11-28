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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Unit tests for the merge-time projection-matrix selection rules
 * ({@link ESNextDiskASHVectorsWriter#selectShareableProjectionMatrix} and
 * {@link ESNextDiskASHVectorsWriter#selectInheritedProjectionMatrix}). They decide which input segment's matrix is
 * reused as-is, or else inherited (warm-started), in the merged segment.
 */
public class ESNextDiskASHVectorsWriterTests extends ESTestCase {

    private static AshProjectionMatrix learned(int originalDim, int nDims, float fill) {
        float[] wT = new float[originalDim * nDims];
        Arrays.fill(wT, fill);
        return new AshProjectionMatrix(wT, originalDim, nDims, true, UUID.randomUUID(), 0);
    }

    private static AshProjectionMatrix random(int originalDim, int nDims, float fill) {
        float[] wT = new float[originalDim * nDims];
        Arrays.fill(wT, fill);
        return new AshProjectionMatrix(wT, originalDim, nDims, false, UUID.randomUUID(), 0);
    }

    public void testReturnsNullWhenNoCandidates() {
        assertNull(ESNextDiskASHVectorsWriter.selectInheritedProjectionMatrix(4, 2, List.of()));
    }

    public void testPrefersLargestLearnedSegment() {
        AshProjectionMatrix small = learned(4, 2, 1.0f);
        AshProjectionMatrix large = learned(4, 2, 2.0f);
        var candidates = List.of(
            new ESNextDiskASHVectorsWriter.SizedProjectionMatrix(small, 10),
            new ESNextDiskASHVectorsWriter.SizedProjectionMatrix(large, 100)
        );
        assertSame(large, ESNextDiskASHVectorsWriter.selectInheritedProjectionMatrix(4, 2, candidates));
    }

    public void testSkipsRandomProjections() {
        AshProjectionMatrix largeRandom = random(4, 2, 9.0f);
        AshProjectionMatrix smallLearned = learned(4, 2, 1.0f);
        var candidates = List.of(
            // The random one is bigger but must be skipped so a random rotation is not inherited.
            new ESNextDiskASHVectorsWriter.SizedProjectionMatrix(largeRandom, 1000),
            new ESNextDiskASHVectorsWriter.SizedProjectionMatrix(smallLearned, 10)
        );
        assertSame(smallLearned, ESNextDiskASHVectorsWriter.selectInheritedProjectionMatrix(4, 2, candidates));
    }

    public void testReturnsNullWhenAllRandom() {
        var candidates = List.of(
            new ESNextDiskASHVectorsWriter.SizedProjectionMatrix(random(4, 2, 1.0f), 10),
            new ESNextDiskASHVectorsWriter.SizedProjectionMatrix(random(4, 2, 2.0f), 20)
        );
        assertNull(ESNextDiskASHVectorsWriter.selectInheritedProjectionMatrix(4, 2, candidates));
    }

    public void testSkipsDimensionMismatch() {
        AshProjectionMatrix wrongDim = learned(8, 2, 1.0f);
        AshProjectionMatrix rightDim = learned(4, 2, 2.0f);
        var candidates = List.of(
            new ESNextDiskASHVectorsWriter.SizedProjectionMatrix(wrongDim, 1000),
            new ESNextDiskASHVectorsWriter.SizedProjectionMatrix(rightDim, 10)
        );
        assertSame(rightDim, ESNextDiskASHVectorsWriter.selectInheritedProjectionMatrix(4, 2, candidates));
    }

    public void testSkipsProjectedDimensionMismatch() {
        AshProjectionMatrix wrongProjectedDims = learned(4, 3, 1.0f);
        AshProjectionMatrix rightProjectedDims = learned(4, 2, 2.0f);
        var candidates = List.of(
            new ESNextDiskASHVectorsWriter.SizedProjectionMatrix(wrongProjectedDims, 1000),
            new ESNextDiskASHVectorsWriter.SizedProjectionMatrix(rightProjectedDims, 10)
        );
        assertSame(rightProjectedDims, ESNextDiskASHVectorsWriter.selectInheritedProjectionMatrix(4, 2, candidates));
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
        var candidates = List.of(new ESNextDiskASHVectorsWriter.SizedProjectionMatrix(learned(8, 2, 1.0f), 10));
        assertNull(ESNextDiskASHVectorsWriter.selectInheritedProjectionMatrix(4, 2, candidates));
    }

    private static AshProjectionMatrix withSample(int originalDim, int nDims, boolean learned, int trainingSampleSize) {
        return new AshProjectionMatrix(new float[originalDim * nDims], originalDim, nDims, learned, UUID.randomUUID(), trainingSampleSize);
    }

    /** The same W as {@code matrix}, as read by another segment that uses it. */
    private static AshProjectionMatrix copyOf(AshProjectionMatrix matrix) {
        return new AshProjectionMatrix(
            matrix.wT().clone(),
            matrix.originalDim(),
            matrix.nDims(),
            matrix.isLearned(),
            matrix.id(),
            matrix.trainingSampleSize()
        );
    }

    private static ESNextDiskASHVectorsWriter.SizedProjectionMatrix sized(AshProjectionMatrix matrix, int size) {
        return new ESNextDiskASHVectorsWriter.SizedProjectionMatrix(matrix, size);
    }

    public void testShareableSelectionReturnsNullWhenNoCandidates() {
        assertThat(ESNextDiskASHVectorsWriter.selectShareableProjectionMatrix(4, 2, 100, List.of()), nullValue());
    }

    public void testShareableSelectionPrefersMatrixUsedByMostVectors() {
        int required = randomIntBetween(1, 100);
        AshProjectionMatrix small = withSample(4, 2, true, required);
        AshProjectionMatrix large = withSample(4, 2, true, required + randomIntBetween(0, 100));
        var candidates = new ArrayList<>(List.of(sized(small, 10), sized(large, 100)));
        Collections.shuffle(candidates, random());
        assertThat(ESNextDiskASHVectorsWriter.selectShareableProjectionMatrix(4, 2, required, candidates), sameInstance(large));
    }

    public void testShareableSelectionSumsVectorsOfSegmentsSharingAMatrix() {
        AshProjectionMatrix shared = withSample(4, 2, true, 100);
        AshProjectionMatrix largest = withSample(4, 2, true, 100);
        var candidates = new ArrayList<>(
            // two segments sharing W hold more vectors together than the single largest segment
            List.of(sized(shared, 60), sized(copyOf(shared), 60), sized(largest, 100))
        );
        Collections.shuffle(candidates, random());
        AshProjectionMatrix selected = ESNextDiskASHVectorsWriter.selectShareableProjectionMatrix(4, 2, 100, candidates);
        assertThat(selected.id(), equalTo(shared.id()));
    }

    public void testShareableSelectionSkipsPartialSamplesAndMismatchedShapes() {
        var ineligible = List.of(
            sized(withSample(4, 2, true, 99), 1000),
            sized(withSample(4, 2, false, 100), 1000),
            sized(withSample(8, 2, true, 100), 1000),
            sized(withSample(4, 3, true, 100), 1000),
            sized(null, 1000)
        );
        assertThat(ESNextDiskASHVectorsWriter.selectShareableProjectionMatrix(4, 2, 100, ineligible), nullValue());

        AshProjectionMatrix eligible = withSample(4, 2, true, 100);
        var candidates = new ArrayList<>(ineligible);
        candidates.add(sized(eligible, 1));
        Collections.shuffle(candidates, random());
        assertThat(ESNextDiskASHVectorsWriter.selectShareableProjectionMatrix(4, 2, 100, candidates), sameInstance(eligible));
    }
}
