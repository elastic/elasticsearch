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
 * Verifies that {@link SeededRetryCollectorManager} maps the seed doc IDs that fall in a given leaf to
 * that leaf's vector ordinals, partitioning per leaf. The seed set is already capped and
 * proximity-selected upstream (see {@link PostFilterKnnQuery#nearestSeedsPerLeaf}), so the manager
 * applies no cap of its own and simply feeds every in-range seed as an HNSW entry point.
 */
public class SeededRetryCollectorManagerTests extends ESTestCase {

    private static final String FIELD = "vector";

    /** Every seed lands in the single graph and is mapped to its ordinal; nothing is dropped. */
    public void testMapsAllInRangeSeeds() throws IOException {
        try (Directory dir = newDirectory()) {
            writeSingleSegment(dir, 50);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals("test relies on a single graph", 1, reader.leaves().size());

                List<KnnSearchStrategy.Seeded> perLeaf = captureSeedsPerLeaf(reader, new int[][] { seedRange(0, 50) });
                assertEquals(1, perLeaf.size());
                KnnSearchStrategy.Seeded seeded = perLeaf.get(0);
                assertEquals(50, seeded.numberOfEntryPoints());

                int[] ordinals = drain(seeded);
                assertEquals(50, ordinals.length);
                for (int i = 0; i < 50; i++) {
                    assertEquals(i, ordinals[i]);
                }
            }
        }
    }

    /** Seeds spanning two graphs are partitioned per leaf and mapped to leaf-local ordinals. */
    public void testSeedsPartitionedPerLeafWithLocalOrdinals() throws IOException {
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

                List<KnnSearchStrategy.Seeded> perLeaf = captureSeedsPerLeaf(reader, new int[][] { seedRange(0, 30), seedRange(30, 60) });
                assertEquals(2, perLeaf.size());
                // Each leaf holds 30 docs; the global seeds for it map to leaf-local ordinals 0..29.
                for (KnnSearchStrategy.Seeded seeded : perLeaf) {
                    assertEquals(30, seeded.numberOfEntryPoints());
                    int[] ordinals = drain(seeded);
                    assertEquals(30, ordinals.length);
                    for (int i = 0; i < 30; i++) {
                        assertEquals(i, ordinals[i]);
                    }
                }
            }
        }
    }

    /** A leaf with no seeds in its doc-id range is searched without a seeded strategy. */
    public void testSeedsOutsideLeafRangeSkipped() throws IOException {
        try (Directory dir = newDirectory()) {
            writeSingleSegment(dir, 50);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                // All seeds are beyond the leaf's maxDoc, so none map into it.
                List<KnnSearchStrategy.Seeded> perLeaf = captureSeedsPerLeaf(reader, new int[][] { { 100, 200, 300 } });
                assertTrue("no seeded strategy expected when no seed falls in the leaf", perLeaf.isEmpty());
            }
        }
    }

    private static List<KnnSearchStrategy.Seeded> captureSeedsPerLeaf(DirectoryReader reader, int[][] seedDocsPerLeaf) throws IOException {
        List<KnnSearchStrategy.Seeded> captured = new ArrayList<>();
        for (LeafReaderContext ctx : reader.leaves()) {
            CapturingCollectorManager capturing = new CapturingCollectorManager();
            SeededRetryCollectorManager manager = new SeededRetryCollectorManager(capturing, seedDocsPerLeaf, FIELD);
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

    /** Global doc IDs [fromInclusive, toExclusive), the seed set for one leaf. */
    private static int[] seedRange(int fromInclusive, int toExclusive) {
        int[] seeds = new int[toExclusive - fromInclusive];
        for (int i = 0; i < seeds.length; i++) {
            seeds[i] = fromInclusive + i;
        }
        return seeds;
    }

    // Plain IndexWriterConfig + a single commit keeps every doc in one segment (one HNSW graph).
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
