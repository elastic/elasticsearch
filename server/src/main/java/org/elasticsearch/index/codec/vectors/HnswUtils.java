/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IntroSorter;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.packed.DirectMonotonicWriter;
import org.elasticsearch.index.codec.vectors.cluster.NeighborHood;

import java.io.IOException;
import java.util.Arrays;
import java.util.SplittableRandom;

/**
 * Utility class for building HNSW graphs from precomputed nearest-neighbor lists and for
 * serializing / deserializing the resulting graph to / from an {@link IndexOutput}.
 */
public final class HnswUtils {

    private HnswUtils() {}

    // Per-node candidate cap when building an upper level's adjacency from a brute-force kNN over that
    // level's (small) node set; pruned down to maxConn diverse links.
    private static final int LEVEL_NEIGHBOR_CANDIDATES = 64;

    // Block shift for DirectMonotonicWriter/Reader; same value used by Lucene99HnswVectorsFormat.
    private static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;

    /**
     * A multi-level HNSW adjacency, ready to serialize. Level 0 contains all nodes (so
     * {@code nodesByLevel[0]} is {@code null}, implicitly {@code 0..size-1}); upper levels list the
     * promoted node ids (sorted). {@code neighborsByLevel[level][i]} are the global-ordinal neighbors
     * of the i-th node on that level (for level 0, i is the vector ordinal).
     */
    public record MultiLevelAdjacency(
        int maxConn,
        int numLevels,
        int entryNode,
        int size,
        int[][] nodesByLevel,
        int[][][] neighborsByLevel
    ) {}

    private static int[][] buildAdjacencyFromNeighborhoods(NeighborHood[] neighborhoods, UpdateableRandomVectorScorer scorer, int maxConn)
        throws IOException {
        final int n = scorer.maxOrd();

        final int[][] adj = new int[n][];
        final int[] sizes = new int[n];
        for (int c = 0; c < n; c++) {
            adj[c] = new int[maxConn];
        }

        final int[] candidateNodes = new int[n];
        final float[] candidateScores = new float[n];
        final int[] keptNodes = new int[maxConn];
        final float[] diversityScratch = new float[maxConn]; // bulkScore output buffer

        // Step 1: directed diversity-pruned edges from kNN neighborhoods + immediate reverse edges.
        for (int c = 0; c < n; c++) {
            final NeighborHood hood = neighborhoods[c];
            final int[] rawCandidates = hood == null ? new int[0] : hood.neighbors();

            int numValid = 0;
            for (int raw : rawCandidates) {
                if (raw == c || raw < 0 || raw >= n) continue;
                candidateNodes[numValid++] = raw;
            }
            scorer.setScoringOrdinal(c);
            scorer.bulkScore(candidateNodes, candidateScores, numValid);

            // Re-sort by similarity descending (nearest first) using the scorer's metric, which may differ
            // from the metric used to build the original neighborhood.
            new NodesByScoreSorter(candidateNodes, candidateScores).sort(0, numValid);

            int keptCount = prune(scorer, numValid, maxConn, candidateNodes, candidateScores, keptNodes, diversityScratch);
            // Add both the directed edge (c → neighbor) and its reverse (neighbor → c).
            for (int k = 0; k < keptCount; k++) {
                addNeighborIfAbsent(adj, sizes, c, keptNodes[k]);
                addNeighborIfAbsent(adj, sizes, keptNodes[k], c);
            }
        }

        // Step 2: re-prune any node whose degree exceeded maxConn due to reverse edges.
        final int[] sortBuf = new int[n];
        final float[] scoreBuf = new float[n];
        final int[][] result = new int[n][];
        for (int c = 0; c < n; c++) {
            final int sz = sizes[c];
            if (sz <= maxConn) {
                result[c] = Arrays.copyOf(adj[c], sz);
            } else {
                // Bulk-score all current neighbors of c in one pass, then sort + diversity-prune.
                scorer.setScoringOrdinal(c);
                System.arraycopy(adj[c], 0, sortBuf, 0, sz);
                scorer.bulkScore(sortBuf, scoreBuf, sz);
                new NodesByScoreSorter(sortBuf, scoreBuf).sort(0, sz);
                int keptCount = prune(scorer, sz, maxConn, sortBuf, scoreBuf, keptNodes, diversityScratch);
                result[c] = Arrays.copyOf(keptNodes, keptCount);
                sizes[c] = keptCount;
            }
        }
        return result;
    }

    /**
     * Builds a multi-level HNSW adjacency over the given nodes. Level 0 reuses the (already computed)
     * neighborhoods via {@link #buildAdjacencyFromNeighborhoods}; each node is then assigned an HNSW
     * level (geometric, {@code mL = 1/ln(maxConn)}), and each upper level's adjacency is built by a
     * cheap brute-force kNN + diversity prune over just that level's (small, geometrically shrinking)
     * node set using the caller-supplied scorer. No per-node graph search is performed.
     *
     * @param neighborhoods level-0 nearest-neighbor lists (may be {@code null} only when
     *                      {@code scorer.maxOrd() <= 1})
     * @param scorer        mutable scorer; the caller owns it and may reuse it after this call
     * @param seed          seed for the (deterministic) level assignment
     */
    public static MultiLevelAdjacency buildMultiLevelFromNeighborhoods(
        NeighborHood[] neighborhoods,
        UpdateableRandomVectorScorer scorer,
        int maxConn,
        long seed
    ) throws IOException {
        final int n = scorer.maxOrd();
        if (n <= 1) {
            final int[][] level0 = new int[n][];
            Arrays.fill(level0, new int[0]);
            return new MultiLevelAdjacency(maxConn, 1, 0, n, new int[1][], new int[][][] { level0 });
        }
        final int[][] level0 = buildAdjacencyFromNeighborhoods(neighborhoods, scorer, maxConn);
        final double mL = 1.0 / Math.log(Math.max(maxConn, 2));
        final SplittableRandom random = new SplittableRandom(seed);
        final int[] levelOf = new int[n];
        int maxLevel = 0;
        for (int c = 0; c < n; c++) {
            double u;
            do {
                u = random.nextDouble();
            } while (u == 0.0);
            // Geometric level assignment per the HNSW paper: level = floor(-ln(u) / ln(maxConn)).
            // Higher levels are exponentially rarer: the expected fraction of nodes on level l+1 is
            // 1/maxConn of those on level l.
            final int level = (int) Math.floor(-Math.log(u) * mL);
            levelOf[c] = level;
            if (level > maxLevel) {
                maxLevel = level;
            }
        }
        final int numLevels = maxLevel + 1;
        final int[][] nodesByLevel = new int[numLevels][];
        final int[][][] neighborsByLevel = new int[numLevels][][];
        neighborsByLevel[0] = level0; // nodesByLevel[0] stays null (implicitly all nodes)
        // Count nodes per upper level in a single pass, then fill in one more pass.
        final int[] levelCounts = new int[numLevels];
        for (int c = 0; c < n; c++) {
            for (int l = 1; l <= levelOf[c]; l++) {
                levelCounts[l]++;
            }
        }
        for (int level = 1; level < numLevels; level++) {
            nodesByLevel[level] = new int[levelCounts[level]];
        }
        final int[] writeIdx = new int[numLevels];
        for (int c = 0; c < n; c++) {
            for (int l = 1; l <= levelOf[c]; l++) {
                nodesByLevel[l][writeIdx[l]++] = c; // ascending c => already sorted
            }
        }
        for (int level = 1; level < numLevels; level++) {
            neighborsByLevel[level] = buildLevelAdjacency(nodesByLevel[level], scorer, maxConn);
        }
        final int entryNode = numLevels > 1 ? nodesByLevel[numLevels - 1][0] : 0;
        return new MultiLevelAdjacency(maxConn, numLevels, entryNode, n, nodesByLevel, neighborsByLevel);
    }

    /**
     * Builds the adjacency among the given (small) upper-level node set via a brute-force pairwise
     * scoring + diversity prune using the caller's scorer (global ordinals throughout). The result is
     * made symmetrical and re-pruned to {@code maxConn} links per node, matching the level-0 treatment.
     */
    private static int[][] buildLevelAdjacency(int[] levelNodes, UpdateableRandomVectorScorer scorer, int maxConn) throws IOException {
        final int m = levelNodes.length;
        if (m == 1) return new int[1][0];

        final int k = Math.min(m - 1, LEVEL_NEIGHBOR_CANDIDATES);

        final int[] candidateGlobals = new int[m];
        final float[] candidateScores = new float[m];
        final int[] keptGlobal = new int[maxConn];
        final float[] diversityScratch = new float[maxConn];

        final int[][] adj = new int[m][maxConn];
        final int[] sizes = new int[m];

        // Step 1: directed diversity-pruned edges among levelNodes.
        for (int i = 0; i < m; i++) {
            scorer.setScoringOrdinal(levelNodes[i]);
            System.arraycopy(levelNodes, 0, candidateGlobals, 0, i);
            System.arraycopy(levelNodes, i + 1, candidateGlobals, i, m - i - 1);
            int cnt = m - 1;
            scorer.bulkScore(candidateGlobals, candidateScores, cnt);
            new NodesByScoreSorter(candidateGlobals, candidateScores).sort(0, cnt);
            cnt = Math.min(cnt, k);

            int keptCount = prune(scorer, cnt, maxConn, candidateGlobals, candidateScores, keptGlobal, diversityScratch);
            adj[i] = Arrays.copyOf(keptGlobal, keptCount);
            sizes[i] = keptCount;
        }

        // Step 2: symmetrize — for each directed edge i→j, add reverse j→i (global ordinals).
        // levelNodes is sorted ascending so Arrays.binarySearch gives global→local in O(log m).
        // TODO: Maybe after further testing & pushing in production, we can remove the backlinking
        // we know all the vectors given the neighborhoods already, maybe we just diversify the forward links
        for (int i = 0; i < m; i++) {
            for (int ki = 0; ki < sizes[i]; ki++) {
                final int neighborGlobal = adj[i][ki];
                final int neighborLocal = Arrays.binarySearch(levelNodes, neighborGlobal);
                addNeighborIfAbsent(adj, sizes, neighborLocal, levelNodes[i]);
            }
        }

        // Step 3: re-prune any oversize node.
        final int[] sortBuf = new int[m];
        final float[] scoreBuf = new float[m];
        final int[][] result = new int[m][];
        for (int i = 0; i < m; i++) {
            final int sz = sizes[i];
            if (sz <= maxConn) {
                result[i] = Arrays.copyOf(adj[i], sz);
            } else {
                scorer.setScoringOrdinal(levelNodes[i]);
                System.arraycopy(adj[i], 0, sortBuf, 0, sz);
                scorer.bulkScore(sortBuf, scoreBuf, sz);
                new NodesByScoreSorter(sortBuf, scoreBuf).sort(0, sz);
                int keptCount = prune(scorer, sz, maxConn, sortBuf, scoreBuf, keptGlobal, diversityScratch);
                result[i] = Arrays.copyOf(keptGlobal, keptCount);
                sizes[i] = keptCount;
            }
        }
        return result;
    }

    /**
     * Appends {@code nb} to {@code adj[c]} if it is not already present, growing the backing array as
     * needed. The linear-scan dedup is acceptable because neighbor lists are bounded by {@code maxConn}.
     * Lists are unsorted during building (held in diversity-pruned score order with reverse edges
     * appended at the end), so binary search is not applicable.
     */
    private static void addNeighborIfAbsent(int[][] adj, int[] sizes, int c, int nb) {
        final int sz = sizes[c];
        for (int k = 0; k < sz; k++) {
            if (adj[c][k] == nb) return;
        }
        if (sz == adj[c].length) {
            adj[c] = ArrayUtil.grow(adj[c]);
        }
        adj[c][sizes[c]++] = nb;
    }

    /**
     * Serializes a {@link MultiLevelAdjacency} into {@code graphOut}, writing the
     * {@link DirectMonotonicWriter} offset-table metadata to {@code metaOut}.
     * The only minor difference from Lucene's output format is that this always uses GroupVarInt for neighbor encoding.
     */
    public static void writeMultiLevelGraph(IndexOutput graphOut, IndexOutput metaOut, MultiLevelAdjacency graph) throws IOException {
        final int size = graph.size();
        final int numLevels = graph.numLevels();
        graphOut.writeVInt(graph.maxConn());
        graphOut.writeVInt(numLevels);
        graphOut.writeVInt(graph.entryNode());
        graphOut.writeVInt(size);
        int totalNodes = size;
        for (int level = 1; level < numLevels; level++) {
            final int[] nodes = graph.nodesByLevel()[level];
            totalNodes += nodes.length;
            graphOut.writeVInt(nodes.length);
            for (int i = 0; i < nodes.length; i++) {
                graphOut.writeVInt(i == 0 ? nodes[0] : nodes[i] - nodes[i - 1]);
            }
        }
        // Collect neighbor data; record per-node byte offsets (monotonically increasing).
        final ByteBuffersDataOutput neighbors = new ByteBuffersDataOutput();
        final long[] offsets = new long[totalNodes];
        int offsetIdx = 0;
        final int[] deltas = new int[graph.maxConn()];
        for (int level = 0; level < numLevels; level++) {
            final int[][] levelAdjacency = graph.neighborsByLevel()[level];
            final int levelSize = level == 0 ? size : graph.nodesByLevel()[level].length;
            for (int i = 0; i < levelSize; i++) {
                offsets[offsetIdx++] = neighbors.size();
                final int[] nb = levelAdjacency[i];
                Arrays.sort(nb);
                neighbors.writeVInt(nb.length);
                if (nb.length > 0) {
                    int prev = 0;
                    for (int j = 0; j < nb.length; j++) {
                        deltas[j] = nb[j] - prev;
                        prev = nb[j];
                    }
                    neighbors.writeGroupVInts(deltas, nb.length);
                }
            }
        }
        // Write DirectMonotonic-compressed offsets: metadata to metaOut, data to graphOut.
        final long offsetsStart = graphOut.getFilePointer();
        metaOut.writeLong(offsetsStart);
        metaOut.writeVInt(DIRECT_MONOTONIC_BLOCK_SHIFT);
        final DirectMonotonicWriter offsetWriter = DirectMonotonicWriter.getInstance(
            metaOut,
            graphOut,
            totalNodes,
            DIRECT_MONOTONIC_BLOCK_SHIFT
        );
        for (long offset : offsets) {
            offsetWriter.add(offset);
        }
        offsetWriter.finish();
        metaOut.writeLong(graphOut.getFilePointer() - offsetsStart);
        graphOut.copyBytes(neighbors.toDataInput(), neighbors.size());
    }

    private static final class NodesByScoreSorter extends IntroSorter {
        private final int[] nodes;
        private final float[] scores;
        private float pivotScore;

        NodesByScoreSorter(int[] nodes, float[] scores) {
            this.nodes = nodes;
            this.scores = scores;
        }

        @Override
        protected void swap(int i, int j) {
            int tmpNode = nodes[i];
            nodes[i] = nodes[j];
            nodes[j] = tmpNode;
            float tmpScore = scores[i];
            scores[i] = scores[j];
            scores[j] = tmpScore;
        }

        @Override
        protected int compare(int i, int j) {
            return Float.compare(scores[j], scores[i]); // descending
        }

        @Override
        protected void setPivot(int i) {
            pivotScore = scores[i];
        }

        @Override
        protected int comparePivot(int j) {
            return Float.compare(scores[j], pivotScore); // descending
        }
    }

    /**
     * Reads a graph previously written by {@link #writeMultiLevelGraph} from {@code graphIn} and
     * {@code metaIn}.
     */
    public static HnswGraph readGraph(IndexInput graphIn, IndexInput metaIn) throws IOException {
        graphIn.seek(0);
        final int maxConn = graphIn.readVInt();
        final int numLevels = graphIn.readVInt();
        final int entryNode = graphIn.readVInt();
        final int size = graphIn.readVInt();
        final int[][] nodesByLevel = new int[numLevels][];
        int totalNodes = size;
        for (int level = 1; level < numLevels; level++) {
            final int numNodes = graphIn.readVInt();
            final int[] nodeIds = new int[numNodes];
            int previous = 0;
            for (int i = 0; i < numNodes; i++) {
                previous += graphIn.readVInt();
                nodeIds[i] = previous;
            }
            nodesByLevel[level] = nodeIds;
            totalNodes += numNodes;
        }
        final long offsetsStart = metaIn.readLong();
        final int blockShift = metaIn.readVInt();
        final DirectMonotonicReader.Meta offsetsMeta = DirectMonotonicReader.loadMeta(metaIn, totalNodes, blockShift);
        final long offsetsLength = metaIn.readLong();
        final RandomAccessInput offsetData = graphIn.randomAccessSlice(offsetsStart, offsetsLength);
        final LongValues offsets = DirectMonotonicReader.getInstance(offsetsMeta, offsetData);
        final long neighborDataStart = offsetsStart + offsetsLength;
        final IndexInput neighborData = graphIn.slice("hnsw-graph-neighbors", neighborDataStart, graphIn.length() - neighborDataStart);
        return new OffHeapHnswGraph(neighborData, nodesByLevel, offsets, size, numLevels, entryNode, maxConn);
    }

    private static int prune(
        UpdateableRandomVectorScorer scorer,
        int sz,
        int maxConn,
        int[] sortBuf,
        float[] scoreBuf,
        int[] keptNodes,
        float[] diversityScratch
    ) throws IOException {
        int keptCount = 0;
        for (int ci = 0; ci < sz && keptCount < maxConn; ci++) {
            final int candidate = sortBuf[ci];
            final float simToNode = scoreBuf[ci];
            if (keptCount == 0) {
                keptNodes[keptCount++] = candidate;
                continue;
            }
            scorer.setScoringOrdinal(candidate);
            if (scorer.bulkScore(keptNodes, diversityScratch, keptCount) < simToNode) {
                keptNodes[keptCount++] = candidate;
            }
        }
        return keptCount;
    }
}
