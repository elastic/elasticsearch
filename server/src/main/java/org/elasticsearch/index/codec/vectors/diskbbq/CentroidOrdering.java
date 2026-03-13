/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IntroSorter;
import org.elasticsearch.index.codec.vectors.cluster.NeighborHood;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

/**
 * Reorders DiskBBQ centroids to improve memory locality at query time.
 *
 * <p>Why this exists: modern CPUs and the OS move data in cache line sized blocks. When an IVF query
 * visits multiple centroids and their posting lists, the best case is that those centroids live near
 * each other on disk and in memory so the same cache line pull serves multiple useful values. The
 * default centroid order from hierarchical clustering is better than random but still leaves
 * unrelated centroids interleaved. Since postings inherit centroid order, improving centroid layout
 * improves vector layout too.
 *
 * <p>We model the layout problem as a weighted graph placement problem (Minimum Linear Arrangement).
 * Vertices are centroids, edges connect centroids that are likely to be visited in the same search,
 * and the cost is the weighted linear distance between their assigned positions. This objective
 * favors keeping groups of related centroids close without over-optimizing a single long edge.
 *
 * <p>Edge construction: we approximate the unknown query co-visit graph with a nearest-neighbor graph
 * over centroids. The triangle inequality implies closer centroids are more likely to be similar in
 * distance to a query, so they co-occur more often. Edge weights scale by local neighbor distance
 * statistics so weights are bounded and roughly normalized across centroid densities.
 *
 * <p>Initialization: a <a href="https://en.wikipedia.org/wiki/Hilbert_curve">Hilbert space-filling curve</a>
 * provides a fast, locality-preserving ordering of centroids. Hilbert indices are computed from
 * quantized centroid coordinates (Butz algorithm), and the centroids are initially sorted by those
 * indices. This can be worse than the hierarchical traversal baseline but gives a strong starting
 * point for refinement.
 *
 * <p>Optimization: we approximate MLA via a <a href="https://en.wikipedia.org/wiki/Simulated_annealing">simulated
 * annealing</a> variant of the <a href="https://en.wikipedia.org/wiki/Kernighan%E2%80%93Lin_algorithm">Kernighan-Lin</a>
 * min-cut heuristic. For each temperature, we compute vertex cut weights once and propose swaps in
 * decreasing cut-weight order using a sigmoid acceptance rule. This reduces the per-level complexity
 * while allowing occasional uphill moves to escape local minima. We recursively apply the procedure
 * to left/right halves of the layout and finish with a local 1-opt pass to polish adjacent swaps.
 */
public final class CentroidOrdering {

    private static final int DEFAULT_K = 32;
    private static final int MIN_CENTROID_COUNT = 16;
    private static final int HILBERT_ORDER = 16;
    private static final float[] TEMPERATURES = { 8.0f, 4.0f, 2.0f, 1.0f, 0.5f, 0.25f };
    private static final int PROBES = 24;
    private static final int MIN_PARTITION_SIZE = 8;
    private static final int LOCAL_1_OPT_ROUNDS = 128;
    private static final float WEIGHT_CAP = 5.0f;
    private static final float MARGIN_SCALE = 0.08f;
    private static final long RNG_SEED = 42L;

    private CentroidOrdering() {}

    public record Result(float[][] centroids, int[] assignments, int[] overspillAssignments) {}

    public static Result reorder(int dims, float[][] centroids, int[] assignments, int[] overspillAssignments) throws IOException {
        return reorder(dims, centroids, assignments, overspillAssignments, null);
    }

    public static Result reorder(int dims, float[][] centroids, int[] assignments, int[] overspillAssignments, NeighborHood[] neighborhoods)
        throws IOException {
        if (centroids.length < MIN_CENTROID_COUNT) {
            return new Result(centroids, assignments, overspillAssignments);
        }
        int n = centroids.length;
        int k = Math.min(DEFAULT_K, n - 1);

        // Build a centroid neighborhood graph to approximate co-visit likelihood during queries.
        final NeighborHood[] resolvedNeighborhoods;
        if (neighborhoods == null || neighborhoods.length != n) {
            resolvedNeighborhoods = NeighborHood.computeNeighborhoods(centroids, k);
        } else {
            resolvedNeighborhoods = neighborhoods;
        }

        int[][] neighbors = new int[n][];
        float[][] distances = new float[n][];
        float[] avgDistances = new float[n];
        for (int i = 0; i < n; i++) {
            int[] localNeighbors = resolvedNeighborhoods[i].neighbors();
            float[] localDistances = resolvedNeighborhoods[i].distances();
            int limit = Math.min(localNeighbors.length, k);
            float totalDistance = 0.0f;
            for (int j = 0; j < limit; j++) {
                float dist = (float) Math.sqrt(localDistances[j]);
                localDistances[j] = dist;
                totalDistance += dist;
            }
            neighbors[i] = localNeighbors;
            distances[i] = localDistances;
            avgDistances[i] = limit == 0 ? 0.0f : totalDistance / limit;
        }

        // Opportunistically symmetrize neighbor lists without exceeding k.
        symmetrizeNeighbors(neighbors, distances, k);

        // Normalize edge weights by local neighbor distance scale to keep weights bounded.
        float[][] weights = new float[n][];
        float totalWeight = 0.0f;
        for (int i = 0; i < n; i++) {
            int[] localNeighbors = neighbors[i];
            float[] localDistances = distances[i];
            int limit = Math.min(localNeighbors.length, k);
            float[] localWeights = new float[limit];
            for (int j = 0; j < limit; j++) {
                int neighbor = localNeighbors[j];
                float avg = 0.5f * (avgDistances[i] + avgDistances[neighbor]);
                float dist = localDistances[j];
                float weight = avg == 0.0f ? WEIGHT_CAP : WEIGHT_CAP * avg / (avg + 4.0f * dist);
                localWeights[j] = weight;
            }
            weights[i] = localWeights;
            for (float weight : localWeights) {
                totalWeight += weight;
            }
        }

        float averageWeight = totalWeight / n;
        if (averageWeight == 0.0f) {
            return new Result(centroids, assignments, overspillAssignments);
        }
        float margin = MARGIN_SCALE * averageWeight;
        if (margin == 0.0f) {
            return new Result(centroids, assignments, overspillAssignments);
        }

        // Layout permutation: ptov maps position -> vertex, vtop maps vertex -> position.
        int[] ptov = new int[n];
        int[] vtop = new int[n];
        for (int i = 0; i < n; i++) {
            ptov[i] = i;
            vtop[i] = i;
        }

        // Hilbert-based initialization provides a fast, locality-preserving starting order.
        int[] ptovHilbert = hilbertOrder(dims, centroids);
        int[] vtopHilbert = new int[n];
        for (int i = 0; i < n; i++) {
            vtopHilbert[ptovHilbert[i]] = i;
        }

        // Annealing loop: recursive min-cut with probabilistic swaps to escape local minima.
        Random rng = new Random(RNG_SEED);
        int[] minPtov = Arrays.copyOf(ptov, n);
        int[] minVtop = Arrays.copyOf(vtop, n);
        float minCost = permutationCost(neighbors, weights, minVtop);

        int[] dcostsOrder = new int[n];
        float[] dcosts = new float[n];
        for (int probe = 0; probe < PROBES; probe++) {
            recursiveMinCutPartition(rng, 0, n, neighbors, weights, ptov, vtop, margin, dcosts, dcostsOrder);
            float cost = permutationCost(neighbors, weights, vtop);
            if (cost < minCost) {
                minCost = cost;
                System.arraycopy(ptov, 0, minPtov, 0, n);
                System.arraycopy(vtop, 0, minVtop, 0, n);
            }
            if (4 * probe < PROBES) {
                resetIdentity(ptov, vtop);
            } else if (2 * probe < PROBES) {
                System.arraycopy(ptovHilbert, 0, ptov, 0, n);
                System.arraycopy(vtopHilbert, 0, vtop, 0, n);
            } else {
                System.arraycopy(minPtov, 0, ptov, 0, n);
                System.arraycopy(minVtop, 0, vtop, 0, n);
            }
        }

        local1Opt(neighbors, weights, minPtov, minVtop);

        float[][] reorderedCentroids = new float[n][];
        for (int i = 0; i < n; i++) {
            reorderedCentroids[i] = centroids[minPtov[i]];
        }
        int[] remappedAssignments = remapAssignments(assignments, minVtop);
        int[] remappedOverspillAssignments = overspillAssignments.length == 0
            ? overspillAssignments
            : remapAssignments(overspillAssignments, minVtop);

        return new Result(reorderedCentroids, remappedAssignments, remappedOverspillAssignments);
    }

    private static void resetIdentity(int[] ptov, int[] vtop) {
        for (int i = 0; i < ptov.length; i++) {
            ptov[i] = i;
            vtop[i] = i;
        }
    }

    private static int[] remapAssignments(int[] assignments, int[] oldToNew) {
        int[] remapped = new int[assignments.length];
        for (int i = 0; i < assignments.length; i++) {
            int assignment = assignments[i];
            remapped[i] = assignment < 0 ? assignment : oldToNew[assignment];
        }
        return remapped;
    }

    private static void symmetrizeNeighbors(int[][] neighbors, float[][] distances, int k) {
        int n = neighbors.length;
        for (int i = 0; i < n; i++) {
            int[] localNeighbors = neighbors[i];
            float[] localDistances = distances[i];
            int limit = Math.min(localNeighbors.length, k);
            for (int j = 0; j < limit; j++) {
                int neighbor = localNeighbors[j];
                if (neighbor == i) {
                    continue;
                }
                if (containsNeighbor(neighbors[neighbor], i, k)) {
                    continue;
                }
                int insertAt = findReplaceCandidate(distances[neighbor], k);
                if (insertAt == -1) {
                    continue;
                }
                if (insertAt >= neighbors[neighbor].length) {
                    continue;
                }
                neighbors[neighbor][insertAt] = i;
                distances[neighbor][insertAt] = localDistances[j];
            }
        }
    }

    private static boolean containsNeighbor(int[] neighbors, int target, int k) {
        int limit = Math.min(neighbors.length, k);
        for (int i = 0; i < limit; i++) {
            if (neighbors[i] == target) {
                return true;
            }
        }
        return false;
    }

    private static int findReplaceCandidate(float[] distances, int k) {
        int limit = Math.min(distances.length, k);
        if (limit == 0) {
            return -1;
        }
        int worstIndex = 0;
        float worstDistance = distances[distances.length - 1];
        for (int i = 1; i < limit; i++) {
            if (distances[i] > worstDistance) {
                worstDistance = distances[i];
                worstIndex = i;
            }
        }
        return worstIndex;
    }

    private static float permutationCost(int[][] neighbors, float[][] weights, int[] vtop) {
        float cost = 0.0f;
        int endpoints = 0;
        for (int i = 0; i < neighbors.length; i++) {
            int pos = vtop[i];
            int[] localNeighbors = neighbors[i];
            float[] localWeights = weights[i];
            for (int j = 0; j < localWeights.length; j++) {
                int neighbor = localNeighbors[j];
                int neighborPos = vtop[neighbor];
                cost += localWeights[j] * Math.abs(pos - neighborPos);
                endpoints++;
            }
        }
        return endpoints == 0 ? 0.0f : cost / endpoints;
    }

    private static void recursiveMinCutPartition(
        Random rng,
        int a,
        int b,
        int[][] neighbors,
        float[][] weights,
        int[] ptov,
        int[] vtop,
        float margin,
        float[] dcosts,
        int[] dcostOrder
    ) {
        if (b - a <= MIN_PARTITION_SIZE) {
            return;
        }
        int mid = (a + b) / 2;
        int length = b - a;
        // Compute per-vertex cut weights for this partition boundary.
        for (int i = 0; i < length; i++) {
            dcostOrder[i] = i;
        }

        for (float temperature : TEMPERATURES) {
            for (int pos = a; pos < b; pos++) {
                int vertex = ptov[pos];
                float rcost = 0.0f;
                float lcost = 0.0f;
                int[] localNeighbors = neighbors[vertex];
                float[] localWeights = weights[vertex];
                for (int i = 0; i < localWeights.length; i++) {
                    int neighbor = localNeighbors[i];
                    if (vtop[neighbor] < mid) {
                        rcost += localWeights[i];
                    } else {
                        lcost += localWeights[i];
                    }
                }
                dcosts[pos - a] = lcost - rcost;
            }

            // Rank vertices on each side by decreasing potential cut reduction.
            intArraySorter(dcostOrder, 0, mid - a, (lhs, rhs) -> Float.compare(dcosts[rhs], dcosts[lhs]));
            intArraySorter(dcostOrder, mid - a, length, (lhs, rhs) -> Float.compare(dcosts[lhs], dcosts[rhs]));

            for (int j = 0; j < mid - a; j++) {
                int lhs = dcostOrder[j];
                int rhs = dcostOrder[mid - a + j];
                // Annealed swap acceptance using a sigmoid of the cut-weight delta.
                float acceptanceLogit = (dcosts[lhs] - dcosts[rhs] - margin) / (temperature * margin);
                if (rng.nextFloat() < sigmoid(acceptanceLogit)) {
                    int leftPos = lhs + a;
                    int rightPos = rhs + a;
                    int leftVertex = ptov[leftPos];
                    int rightVertex = ptov[rightPos];
                    ptov[leftPos] = rightVertex;
                    ptov[rightPos] = leftVertex;
                    vtop[rightVertex] = leftPos;
                    vtop[leftVertex] = rightPos;
                }
                if (acceptanceLogit < -10.0f) {
                    break;
                }
            }
        }

        // Recurse on the left/right halves of the current arrangement.
        recursiveMinCutPartition(rng, a, mid, neighbors, weights, ptov, vtop, margin, dcosts, dcostOrder);
        recursiveMinCutPartition(rng, mid, b, neighbors, weights, ptov, vtop, margin, dcosts, dcostOrder);
    }

    private static void local1Opt(int[][] neighbors, float[][] weights, int[] ptov, int[] vtop) {
        int roundsLeft = LOCAL_1_OPT_ROUNDS;
        int swaps;
        do {
            swaps = 0;
            for (int i = 1, j = 0; i < ptov.length; i++, j++) {
                float dcost = 0.0f;
                int vertexI = ptov[i];
                int vertexJ = ptov[j];
                int[] neighborsI = neighbors[vertexI];
                float[] weightsI = weights[vertexI];
                for (int n = 0; n < weightsI.length; n++) {
                    int k = vtop[neighborsI[n]];
                    if (k == j) {
                        continue;
                    }
                    dcost += weightsI[n] * (Math.abs(k - j) - Math.abs(k - i));
                }
                int[] neighborsJ = neighbors[vertexJ];
                float[] weightsJ = weights[vertexJ];
                for (int n = 0; n < weightsJ.length; n++) {
                    int k = vtop[neighborsJ[n]];
                    if (k == i) {
                        continue;
                    }
                    dcost += weightsJ[n] * (Math.abs(k - i) - Math.abs(k - j));
                }
                if (dcost < 0.0f) {
                    ptov[i] = vertexJ;
                    ptov[j] = vertexI;
                    vtop[vertexI] = j;
                    vtop[vertexJ] = i;
                    swaps++;
                }
            }
        } while (swaps > 0 && --roundsLeft > 0);
    }

    private static float sigmoid(float value) {
        return (float) (1.0 / (1.0 + Math.exp(-value)));
    }

    private static int[] hilbertOrder(int dim, float[][] points) {
        int n = points.length;
        float min = Float.POSITIVE_INFINITY;
        float max = Float.NEGATIVE_INFINITY;
        for (float[] point : points) {
            for (float v : point) {
                min = Math.min(min, v);
                max = Math.max(max, v);
            }
        }
        if (max == min) {
            int[] identity = new int[n];
            for (int i = 0; i < n; i++) {
                identity[i] = i;
            }
            return identity;
        }

        BitVector[] hilbertIndices = new BitVector[n];
        int[] coords = new int[dim];
        HilbertEncoder encoder = new HilbertEncoder(dim, CentroidOrdering.HILBERT_ORDER);
        for (int i = 0; i < n; i++) {
            float[] point = points[i];
            ESVectorUtil.quantizeVectorWithIntervals(point, coords, min, max, CentroidOrdering.HILBERT_ORDER);
            hilbertIndices[i] = encoder.encode(coords);
        }

        int[] positions = new int[n];
        for (int i = 0; i < n; i++) {
            positions[i] = i;
        }
        intArraySorter(positions, 0, n, (lhs, rhs) -> hilbertIndices[lhs].compareTo(hilbertIndices[rhs]));
        return positions;
    }

    private interface IntComparator {
        int compare(int lhs, int rhs);
    }

    private static void intArraySorter(int[] ints, int from, int length, IntComparator comparator) {
        new IntroSorter() {
            int pivotPos = -1;

            @Override
            protected void swap(int i, int j) {
                int tmp = ints[i];
                ints[i] = ints[j];
                ints[j] = tmp;
            }

            @Override
            protected void setPivot(int i) {
                pivotPos = i;
            }

            @Override
            protected int comparePivot(int i) {
                return comparator.compare(ints[pivotPos], ints[i]);
            }

            @Override
            protected int compare(int a, int b) {
                return comparator.compare(ints[a], ints[b]);
            }
        }.sort(from, length);
    }

    private static final class HilbertEncoder {
        private final int dim;
        private final int order;
        private final BitVector w;
        private final BitVector tS;
        private final BitVector p;
        private final BitVector t;

        private HilbertEncoder(int dim, int order) {
            this.dim = dim;
            this.order = order;
            this.w = new BitVector(dim);
            this.tS = new BitVector(dim);
            this.p = new BitVector(dim);
            this.t = new BitVector(dim);
        }

        private BitVector encode(int[] point) {
            BitVector hcode = new BitVector(dim * order);
            w.clear();
            tS.clear();
            int xj = 0;

            for (int i = order - 1; i >= 0; i--) {
                for (int j = 0; j < dim; j++) {
                    tS.setBit(dim - 1 - j, ((point[j] >> i) & 1) != 0);
                }

                tS.xorVector(w);
                tS.rotateRight(xj);
                calcP(tS, p, dim);

                int base = i * dim;
                for (int j = 0; j < dim; j++) {
                    if (p.getBit(j)) {
                        hcode.setBit(base + j, true);
                    }
                }

                if (i > 0) {
                    calcT(p, t);
                    t.rotateRight(xj);
                    w.xorVector(t);
                    xj += (calcJ(p, dim) - 1);
                    xj %= dim;
                }
            }
            return hcode;
        }

        private static void calcP(BitVector s, BitVector out, int dim) {
            out.clear();
            boolean last = false;
            for (int i = dim - 1; i >= 0; i--) {
                boolean pbit = s.getBit(i) ^ last;
                out.setBit(i, pbit);
                last = pbit;
            }
        }

        private static int calcJ(BitVector p, int dim) {
            boolean zeroBit = p.getBit(0);
            if (zeroBit) {
                return dim - p.nextBitSetSafe(1);
            }
            for (int i = 1; i < dim; i++) {
                if (p.getBit(i) == false) {
                    return dim - i;
                }
            }
            return dim;
        }

        private static void calcT(BitVector p, BitVector out) {
            if (p.lessThanThree()) {
                out.clear();
                return;
            }
            out.copyFrom(p);
            if (out.getBit(0)) {
                out.decrement(1);
            } else {
                out.decrement(2);
            }
            out.transformToGray();
        }
    }

    private static final class BitVector implements Comparable<BitVector> {
        private final FixedBitSet bits;
        private final long[] words;
        private final int wordCount;
        private final long lastWordMask;
        private final long[] scratch;
        private final int length;

        BitVector(int length) {
            this.length = length;
            this.bits = new FixedBitSet(length);
            this.words = bits.getBits();
            this.wordCount = words.length;
            int tail = length & 63;
            this.lastWordMask = tail == 0 ? -1L : (1L << tail) - 1L;
            this.scratch = new long[wordCount];
        }

        void setBit(int index, boolean value) {
            if (index >= length || index < 0) {
                return;
            }
            if (value) {
                bits.set(index);
            } else {
                bits.clear(index);
            }
        }

        boolean getBit(int index) {
            if (index >= length || index < 0) {
                return false;
            }
            return bits.get(index);
        }

        void xorVector(BitVector other) {
            bits.xor(other.bits);
        }

        void clear() {
            bits.clear(0, length);
        }

        void copyFrom(BitVector other) {
            System.arraycopy(other.words, 0, words, 0, wordCount);
        }

        int nextBitSetSafe(int fromIndex) {
            if (fromIndex < 0) {
                throw new IndexOutOfBoundsException("fromIndex must be non-negative; got " + fromIndex);
            }
            if (fromIndex >= length) {
                return -1;
            }
            int next = bits.nextSetBit(fromIndex);
            return next >= length ? -1 : next;
        }

        void rotateRight(int shift) {
            if (length <= 1) {
                return;
            }
            shift %= length;
            if (shift < 0) {
                shift += length;
            }
            if (shift == 0) {
                return;
            }
            Arrays.fill(scratch, 0L);
            for (int i = nextBitSetSafe(0); i >= 0;) {
                int target = Math.floorMod(i - shift, length);
                int word = target >>> 6;
                scratch[word] |= 1L << (target & 63);
                if (i >= length - 1) {
                    break;
                }
                i = nextBitSetSafe(i + 1);
            }
            System.arraycopy(scratch, 0, words, 0, wordCount);
            words[wordCount - 1] &= lastWordMask;
        }

        void transformToGray() {
            for (int i = 0; i < wordCount; i++) {
                long next = i + 1 < wordCount ? words[i + 1] : 0L;
                scratch[i] = (words[i] >>> 1) | (next << 63);
            }
            scratch[wordCount - 1] &= lastWordMask;
            for (int i = 0; i < wordCount; i++) {
                words[i] ^= scratch[i];
            }
            words[wordCount - 1] &= lastWordMask;
        }

        void decrement(int value) {
            if (value <= 0) {
                return;
            }
            for (int step = 0; step < value; step++) {
                int i = nextBitSetSafe(0);
                if (i < 0) {
                    return;
                }
                bits.clear(i);
                if (i > 0) {
                    bits.set(0, i);
                }
            }
        }

        boolean lessThanThree() {
            int firstSetBit = nextBitSetSafe(0);
            if (firstSetBit > 2) {
                return false;
            }
            int value = (getBit(0) ? 1 : 0) + (getBit(1) ? 2 : 0);
            return value < 3;
        }

        @Override
        public int compareTo(BitVector other) {
            int maxWords = Math.max(this.wordCount, other.wordCount);
            for (int i = maxWords - 1; i >= 0; i--) {
                long a = 0L;
                long b = 0L;
                if (i < this.wordCount) {
                    a = this.words[i];
                    if (i == this.wordCount - 1) {
                        a &= this.lastWordMask;
                    }
                }
                if (i < other.wordCount) {
                    b = other.words[i];
                    if (i == other.wordCount - 1) {
                        b &= other.lastWordMask;
                    }
                }
                if (a != b) {
                    return Long.compareUnsigned(a, b);
                }
            }
            return 0;
        }
    }
}
