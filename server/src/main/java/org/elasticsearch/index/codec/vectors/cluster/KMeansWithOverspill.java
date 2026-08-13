/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

import org.elasticsearch.index.codec.vectors.diskbbq.OverspillAssignments;

import java.util.Arrays;
import java.util.List;
import java.util.PrimitiveIterator;

/**
 * A {@link KMeansResult} with additional overspill assignments
 */
public record KMeansWithOverspill<V>(KMeansResult<V> result, OverspillAssignments overspill) {

    public V[] centroids() {
        return result.centroids();
    }

    public int[] assignments() {
        return result.assignments();
    }

    /**
     * Merge multiple clustering results into a single result by concatenating centroids
     * in the provided order and reindexing assignments to the merged centroid layout.
     * Overspill assignments are offset the same way, where present.
     */
    public static <V> KMeansWithOverspill<V> merge(List<KMeansWithOverspill<V>> results, CentroidOps<V> ops) {
        int numCentroids = 0;
        int numAssignments = 0;
        for (KMeansWithOverspill<V> result : results) {
            numCentroids += result.centroids().length;
            numAssignments += result.assignments().length;
        }

        V[] centroids = ops.newCentroidArrayShallow(numCentroids);
        int[] assignments = new int[numAssignments];
        int[] spillAssignmentOffsets = new int[numAssignments];
        int[] spillCentroidOffsets = new int[numAssignments];
        OverspillAssignments[] overspills = new OverspillAssignments[numAssignments];

        int centroidOffset = 0;
        int assignmentOffset = 0;
        int spillAssignmentIdx = 0;
        for (KMeansWithOverspill<V> result : results) {
            V[] resultCentroids = result.centroids();
            int[] resultAssignments = result.assignments();
            ops.arrayCopy(resultCentroids, 0, centroids, centroidOffset, resultCentroids.length);
            for (int i = 0; i < resultAssignments.length; i++) {
                assignments[assignmentOffset + i] = resultAssignments[i] + centroidOffset;
            }

            OverspillAssignments overspill = result.overspill();
            if (overspill.size() > 0) {
                spillAssignmentOffsets[spillAssignmentIdx] = assignmentOffset;
                spillCentroidOffsets[spillAssignmentIdx] = centroidOffset;
                overspills[spillAssignmentIdx] = overspill;
                spillAssignmentIdx++;
            }

            centroidOffset += resultCentroids.length;
            assignmentOffset += resultAssignments.length;
        }

        OverspillAssignments overspill = spillAssignmentIdx == 0
            ? OverspillAssignments.NONE
            : new MergedOverspillAssignments(
                numAssignments,
                Arrays.copyOf(spillAssignmentOffsets, spillAssignmentIdx),
                Arrays.copyOf(spillCentroidOffsets, spillAssignmentIdx),
                Arrays.copyOf(overspills, spillAssignmentIdx)
            );

        return new KMeansWithOverspill<>(new KMeansResult<>(centroids, assignments), overspill);
    }

    private static class MergedOverspillAssignments implements OverspillAssignments {

        private final int[] assignmentOffsets;
        private final int[] centroidOffsets;
        private final OverspillAssignments[] assignments;
        private final int size;

        private MergedOverspillAssignments(
            int totalSize,
            int[] assignmentOffsets,
            int[] centroidOffsets,
            OverspillAssignments[] assignments
        ) {
            assert assignmentOffsets.length == assignments.length;
            assert centroidOffsets.length == assignmentOffsets.length;

            this.assignmentOffsets = assignmentOffsets;
            this.centroidOffsets = centroidOffsets;
            this.assignments = assignments;
            this.size = totalSize;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public PrimitiveIterator.OfInt getAssignmentsFor(int ordinal) {
            int index = Arrays.binarySearch(assignmentOffsets, ordinal);
            if (index == -1) {
                // ordinal is before the first tracked offset — no overspill for this position
                return EMPTY_ITERATOR;
            }
            if (index < 0) {
                index = -index - 2; // go back one to the offset < ordinal, as that's the assignment containing this ordinal
            }
            // if this ordinal is out of range for the indexed assignment, that's ok, it'll just return an empty iterator
            var iterator = assignments[index].getAssignmentsFor(ordinal - assignmentOffsets[index]);
            return new MappingIntIterator(iterator, centroidOffsets[index]);
        }
    }

    private static class MappingIntIterator implements PrimitiveIterator.OfInt {

        private final PrimitiveIterator.OfInt iterator;
        private final int resultOffset;

        private MappingIntIterator(OfInt iterator, int resultOffset) {
            this.iterator = iterator;
            this.resultOffset = resultOffset;
        }

        @Override
        public int nextInt() {
            return iterator.nextInt() + resultOffset;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }
    }
}
