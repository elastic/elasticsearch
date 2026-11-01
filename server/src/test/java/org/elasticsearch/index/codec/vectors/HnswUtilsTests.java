/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.hnsw.HnswGraphSearcher;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.elasticsearch.index.codec.vectors.cluster.CentroidOps;
import org.elasticsearch.index.codec.vectors.cluster.NeighborHood;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

import static org.apache.lucene.util.hnsw.HnswGraphBuilder.DEFAULT_MAX_CONN;

public class HnswUtilsTests extends ESTestCase {

    public void testGraphSerialization() throws IOException {
        int numVectors = random().nextInt(50, 300);
        int dimensions = random().nextInt(8, 32);
        float[][] vectors = new float[numVectors][dimensions];
        for (int c = 0; c < numVectors; c++) {
            for (int d = 0; d < dimensions; d++) {
                vectors[c][d] = random().nextFloat() * 2f - 1f;
            }
        }
        int m = random().nextInt(4, 24);
        int candidates = Math.min(numVectors - 1, 64);
        NeighborHood[] neighborhoods = NeighborHood.computeNeighborhoods(CentroidOps.FLOAT, vectors, candidates);
        HnswUtils.MultiLevelAdjacency built = HnswUtils.buildMultiLevelFromNeighborhoods(
            neighborhoods,
            floatScorer(vectors, VectorSimilarityFunction.EUCLIDEAN),
            m,
            random().nextLong()
        );

        int numLevels = built.numLevels();
        try (Directory dir = newDirectory()) {
            try (var graphOut = dir.createOutput("g", IOContext.DEFAULT); var metaOut = dir.createOutput("g.meta", IOContext.DEFAULT)) {
                HnswUtils.writeMultiLevelGraph(graphOut, metaOut, built);
            }
            try (var graphIn = dir.openInput("g", IOContext.DEFAULT); var metaIn = dir.openInput("g.meta", IOContext.DEFAULT)) {
                HnswGraph graph = HnswUtils.readGraph(graphIn, metaIn);

                assertEquals(numLevels, graph.numLevels());
                assertEquals(built.size(), graph.size());
                assertEquals(built.entryNode(), graph.entryNode());
                assertEquals(built.maxConn(), graph.maxConn());

                for (int level = 1; level < numLevels; level++) {
                    int[] expectedNodes = built.nodesByLevel()[level];
                    HnswGraph.NodesIterator it = graph.getNodesOnLevel(level);
                    for (int expectedNode : expectedNodes) {
                        assertTrue("upper-level node list too short at level=" + level, it.hasNext());
                        assertEquals("node mismatch at level=" + level, expectedNode, it.nextInt());
                    }
                    assertFalse("upper-level node list too long at level=" + level, it.hasNext());
                }

                for (int level = 0; level < numLevels; level++) {
                    int levelSize = level == 0 ? built.size() : built.nodesByLevel()[level].length;
                    for (int i = 0; i < levelSize; i++) {
                        int node = level == 0 ? i : built.nodesByLevel()[level][i];
                        graph.seek(level, node);
                        List<Integer> actual = new ArrayList<>();
                        for (int nb = graph.nextNeighbor(); nb != DocIdSetIterator.NO_MORE_DOCS; nb = graph.nextNeighbor()) {
                            actual.add(nb);
                        }
                        assertArrayEquals(
                            "neighbor mismatch at level=" + level + " node=" + node,
                            built.neighborsByLevel()[level][i],
                            actual.stream().mapToInt(Integer::intValue).toArray()
                        );
                    }
                }
            }
        }
    }

    public void testNeighborhoodGraphIsConnected() throws IOException {
        int dimensions = random().nextInt(8, 48);
        int numVectors = random().nextInt(150, 400);
        float[][] vectors = new float[numVectors][dimensions];
        for (int c = 0; c < numVectors; c++) {
            for (int d = 0; d < dimensions; d++) {
                vectors[c][d] = c * 100f + random().nextFloat();
            }
        }
        // shuffle ordinals so spatial order != ordinal order
        for (int i = numVectors - 1; i > 0; i--) {
            int j = random().nextInt(i + 1);
            float[] tmp = vectors[i];
            vectors[i] = vectors[j];
            vectors[j] = tmp;
        }
        int m = random().nextInt(8, 32);
        int candidates = Math.min(numVectors - 1, 64);
        NeighborHood[] neighborhoods = NeighborHood.computeNeighborhoods(CentroidOps.FLOAT, vectors, candidates);
        HnswUtils.MultiLevelAdjacency built = HnswUtils.buildMultiLevelFromNeighborhoods(
            neighborhoods,
            floatScorer(vectors, VectorSimilarityFunction.EUCLIDEAN),
            m,
            42L
        );
        assertTrue("expected at least one level for " + numVectors + " vectors", built.numLevels() >= 1);
        // BFS the level-0 adjacency: it must connect every node
        assertEquals("in-memory level-0 adjacency not fully connected", numVectors, reachable(built.neighborsByLevel()[0]));

        // round-trip through serialization and BFS the deserialized level-0 graph
        try (Directory dir = newDirectory()) {
            try (
                var graphOut = dir.createOutput("graph", IOContext.DEFAULT);
                var metaOut = dir.createOutput("graph.meta", IOContext.DEFAULT)
            ) {
                HnswUtils.writeMultiLevelGraph(graphOut, metaOut, built);
            }
            try (var graphIn = dir.openInput("graph", IOContext.DEFAULT); var metaIn = dir.openInput("graph.meta", IOContext.DEFAULT)) {
                HnswGraph graph = HnswUtils.readGraph(graphIn, metaIn);
                assertEquals(built.numLevels(), graph.numLevels());
                int[][] readAdjacency = new int[numVectors][];
                for (int node = 0; node < numVectors; node++) {
                    graph.seek(0, node);
                    List<Integer> nbrs = new ArrayList<>();
                    for (int nb = graph.nextNeighbor(); nb != DocIdSetIterator.NO_MORE_DOCS; nb = graph.nextNeighbor()) {
                        nbrs.add(nb);
                    }
                    readAdjacency[node] = nbrs.stream().mapToInt(Integer::intValue).toArray();
                }
                assertEquals("deserialized level-0 graph not fully connected", numVectors, reachable(readAdjacency));
            }
        }
    }

    public void testNeighborhoodGraphSearchFindsSelf() throws IOException {
        int dimensions = random().nextInt(16, 48);
        int numVectors = random().nextInt(150, 300);
        float[][] vectors = new float[numVectors][dimensions];
        for (int c = 0; c < numVectors; c++) {
            for (int d = 0; d < dimensions; d++) {
                vectors[c][d] = random().nextFloat() * 4f - 2f;
            }
        }
        var sim = VectorSimilarityFunction.EUCLIDEAN;
        int m = random().nextInt(DEFAULT_MAX_CONN, 32);
        int candidates = Math.min(numVectors - 1, 64);
        var neighborhoods = NeighborHood.computeNeighborhoods(CentroidOps.FLOAT, vectors, candidates);
        HnswUtils.MultiLevelAdjacency built = HnswUtils.buildMultiLevelFromNeighborhoods(neighborhoods, floatScorer(vectors, sim), m, 42L);
        try (Directory dir = newDirectory()) {
            try (var graphOut = dir.createOutput("g", IOContext.DEFAULT); var metaOut = dir.createOutput("g.meta", IOContext.DEFAULT)) {
                HnswUtils.writeMultiLevelGraph(graphOut, metaOut, built);
            }
            try (var graphIn = dir.openInput("g", IOContext.DEFAULT); var metaIn = dir.openInput("g.meta", IOContext.DEFAULT)) {
                HnswGraph graph = HnswUtils.readGraph(graphIn, metaIn);
                int found = 0;
                int queries = Math.min(numVectors, 40);
                for (int qi = 0; qi < queries; qi++) {
                    int q = random().nextInt(numVectors);
                    var scorer = floatScorer(vectors, sim);
                    scorer.setScoringOrdinal(q);
                    var collector = new TopKnnCollector(10, Integer.MAX_VALUE);
                    HnswGraphSearcher.search(scorer, collector, graph, null);
                    for (var sd : collector.topDocs().scoreDocs) {
                        if (sd.doc == q) {
                            found++;
                            break;
                        }
                    }
                }
                assertTrue("graph search found self in only " + found + "/" + queries + " queries", found >= queries * 0.9);
            }
        }
    }

    private static UpdateableRandomVectorScorer floatScorer(float[][] vectors, VectorSimilarityFunction sim) {
        return new UpdateableRandomVectorScorer() {
            private int queryOrd;

            @Override
            public void setScoringOrdinal(int node) {
                queryOrd = node;
            }

            @Override
            public float score(int node) {
                return sim.compare(vectors[queryOrd], vectors[node]);
            }

            @Override
            public int maxOrd() {
                return vectors.length;
            }
        };
    }

    private static int reachable(int[][] adjacency) {
        boolean[] visited = new boolean[adjacency.length];
        ArrayDeque<Integer> queue = new ArrayDeque<>();
        visited[0] = true;
        queue.add(0);
        int reached = 1;
        while (queue.isEmpty() == false) {
            int node = queue.poll();
            for (int neighbor : adjacency[node]) {
                if (visited[neighbor] == false) {
                    visited[neighbor] = true;
                    reached++;
                    queue.add(neighbor);
                }
            }
        }
        return reached;
    }
}
