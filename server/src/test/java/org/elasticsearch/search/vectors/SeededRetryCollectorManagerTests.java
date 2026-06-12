/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.search.knn.TopKnnCollectorManager;
import org.apache.lucene.store.Directory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Verifies that {@link SeededRetryCollectorManager} never feeds more than 16 seed entry points
 * into a single graph (leaf), regardless of how many round-0 docs are offered as seeds.
 */
public class SeededRetryCollectorManagerTests extends ESTestCase {

    private static final String FIELD = "vector";
    /** Mirror of the (package-private) cap under test so an accidental change to one is caught. */
    private static final int MAX_SEEDS_PER_GRAPH = 16;

    /**
     * A single graph offered far more seeds than the cap keeps exactly {@link #MAX_SEEDS_PER_GRAPH},
     * and keeps the lowest doc IDs (ordinals 0..15 in a single all-vectors segment).
     */
    public void testSingleGraphCapsSeedsAtMax() throws IOException {
        try (Directory dir = newDirectory()) {
            writeSingleSegment(dir, 50);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals("test relies on a single graph", 1, reader.leaves().size());
                int[] seedDocs = ascendingSeeds(50);

                List<KnnSearchStrategy.Seeded> perLeaf = captureSeedsPerLeaf(reader, seedDocs);
                assertEquals(1, perLeaf.size());
                KnnSearchStrategy.Seeded seeded = perLeaf.get(0);
                assertEquals(MAX_SEEDS_PER_GRAPH, seeded.numberOfEntryPoints());

                int[] ordinals = drain(seeded);
                assertEquals(MAX_SEEDS_PER_GRAPH, ordinals.length);
                for (int i = 0; i < MAX_SEEDS_PER_GRAPH; i++) {
                    assertEquals("expected the lowest doc IDs to be kept", i, ordinals[i]);
                }
            }
        }
    }

    public void testFewerSeedsThanCapAreAllKept() throws IOException {
        int seedCount = randomIntBetween(1, MAX_SEEDS_PER_GRAPH);
        try (Directory dir = newDirectory()) {
            writeSingleSegment(dir, 50);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                List<KnnSearchStrategy.Seeded> perLeaf = captureSeedsPerLeaf(reader, ascendingSeeds(seedCount));
                assertEquals(1, perLeaf.size());
                assertEquals(seedCount, perLeaf.get(0).numberOfEntryPoints());
            }
        }
    }

    public void testCapAppliesPerGraph() throws IOException {
        try (Directory dir = newDirectory()) {
            // Two commits with NoMergePolicy produce two separate segments (graphs) deterministically.
            try (IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
                addVectorDocs(iw, 0, 30);
                iw.commit();
                addVectorDocs(iw, 30, 30);
                iw.commit();
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals("test relies on two graphs", 2, reader.leaves().size());

                List<KnnSearchStrategy.Seeded> perLeaf = captureSeedsPerLeaf(reader, ascendingSeeds(60));
                assertEquals(2, perLeaf.size());
                for (KnnSearchStrategy.Seeded seeded : perLeaf) {
                    assertEquals(MAX_SEEDS_PER_GRAPH, seeded.numberOfEntryPoints());
                }
            }
        }
    }

    private static List<KnnSearchStrategy.Seeded> captureSeedsPerLeaf(DirectoryReader reader, int[] seedDocs) throws IOException {
        List<KnnSearchStrategy.Seeded> captured = new ArrayList<>();
        for (LeafReaderContext ctx : reader.leaves()) {
            CapturingCollectorManager capturing = new CapturingCollectorManager();
            SeededRetryCollectorManager manager = new SeededRetryCollectorManager(capturing, seedDocs, FIELD);
            manager.newCollector(Integer.MAX_VALUE, KnnSearchStrategy.Hnsw.DEFAULT, ctx);
            if (capturing.captured instanceof KnnSearchStrategy.Seeded seeded) {
                captured.add(seeded);
            }
        }
        return captured;
    }

    private static int[] drain(KnnSearchStrategy.Seeded seeded) throws IOException {
        DocIdSetIterator it = seeded.entryPoints();
        List<Integer> ordinals = new ArrayList<>();
        for (int ord = it.nextDoc(); ord != DocIdSetIterator.NO_MORE_DOCS; ord = it.nextDoc()) {
            ordinals.add(ord);
        }
        return ordinals.stream().mapToInt(Integer::intValue).toArray();
    }

    private static int[] ascendingSeeds(int count) {
        int[] seeds = new int[count];
        for (int i = 0; i < count; i++) {
            seeds[i] = i;
        }
        return seeds;
    }

    // Plain IndexWriterConfig + a single commit keeps every doc in one segment (one HNSW graph) so the
    // per-graph cap is exercised deterministically; we intentionally do not force-merge.
    private static void writeSingleSegment(Directory dir, int docCount) throws IOException {
        try (IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig())) {
            addVectorDocs(iw, 0, docCount);
            iw.commit();
        }
    }

    private static void addVectorDocs(IndexWriter iw, int fromDoc, int count) throws IOException {
        for (int i = 0; i < count; i++) {
            Document doc = new Document();
            doc.add(new KnnFloatVectorField(FIELD, new float[] { fromDoc + i }));
            iw.addDocument(doc);
        }
    }

    private static class CapturingCollectorManager implements KnnCollectorManager {
        private final KnnCollectorManager real = new TopKnnCollectorManager(10, null);
        private KnnSearchStrategy captured;

        @Override
        public KnnCollector newCollector(int visitedLimit, KnnSearchStrategy searchStrategy, LeafReaderContext context) throws IOException {
            this.captured = searchStrategy;
            return real.newCollector(visitedLimit, searchStrategy, context);
        }

        @Override
        public boolean isOptimistic() {
            return false;
        }
    }
}
