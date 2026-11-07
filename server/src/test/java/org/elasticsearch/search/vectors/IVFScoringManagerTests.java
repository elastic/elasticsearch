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
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.index.codec.vectors.VectorTestUtils;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidIndexFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.QuantEncoding;
import org.elasticsearch.index.codec.vectors.diskbbq.TestIvfQueryConfigResolver;
import org.elasticsearch.index.codec.vectors.diskbbq.es95.ES950DiskBBQVectorsFormat;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class IVFScoringManagerTests extends ESTestCase {

    public void testForceMergedLeafGetsOnePartPerThread() {
        assertSlices(new long[] { 72_000 }, all(1), 8, copies(new int[] { 0 }, 8));
        assertSlices(new long[] { 72_000 }, all(1), 16, copies(new int[] { 0 }, 10)); // no slice under 10% of the work
        assertSlices(new long[] { 72_000 }, all(1), 1, new int[][] { { 0 } });
    }

    public void testASmallFreshLeafJoinsAPartOfTheBigOne() {
        // the big leaf holds 7.9 slices' worth of work: rounded, it keeps every thread busy
        int[][] expected = copies(new int[] { 0 }, 8);
        expected[0] = new int[] { 1, 0 };
        assertSlices(new long[] { 71_000, 1_000 }, all(2), 8, expected);
    }

    public void testSmallLeavesAreGroupedIntoSlicesOfEqualWork() {
        long[] equal = new long[12];
        Arrays.fill(equal, 1_000);
        assertSlices(equal, all(equal.length), 8, new int[][] { { 0, 1 }, { 2, 3 }, { 4, 5 }, { 6, 7 }, { 8, 9 }, { 10, 11 } });
        long[] tiered = { 4_000, 2_000, 1_000, 500, 250, 125 };
        assertSlices(tiered, all(tiered.length), 4, new int[][] { { 0 }, { 0 }, { 1 }, { 2, 3, 4, 5 } });
    }

    public void testLeftoversJoinTheLeastLoadedSlices() {
        assertSlices(new long[] { 100, 2_500, 100, 100 }, all(4), 4, new int[][] { { 0, 1 }, { 2, 1 }, { 3, 1 }, { 1 } });
        assertSlices(new long[] { 1_000, 1_000, 50 }, all(3), 4, new int[][] { { 2, 0 }, { 0 }, { 1 }, { 1 } });
    }

    public void testNoPartHasLessThanTenPercentOfTheWork() {
        // 15% of the work is 1.5 slices' worth with 10 slices; split in two, each part would hold 7.5%
        int[][] expected = copies(new int[] { 1 }, 9);
        expected[8] = new int[] { 0 };
        assertSlices(new long[] { 1_500, 8_500 }, all(2), 16, expected);
    }

    public void testRoundingNeverMakesMoreSlicesThanThreads() {
        // three leaves hold 2.5 slices' worth each: rounded up, they would make 9 slices for 8 threads
        long[] work = { 2_500, 2_500, 2_500, 500 };
        int[][] expected = { { 3, 0 }, { 0 }, { 0 }, { 1 }, { 1 }, { 1 }, { 2 }, { 2 } };
        assertSlices(work, all(work.length), 8, expected);
    }

    public void testLeavesThatCannotBeSplitStayWhole() {
        assertSlices(new long[] { 10_000, 100 }, new boolean[] { false, true }, 4, new int[][] { { 1, 0 } });
    }

    /** Whatever the shape: at most one slice per thread and core, none under 10% of the work, every leaf searched once. */
    public void testSlicesKeepTheirGuarantees() {
        for (int iter = 0; iter < 200; iter++) {
            int leaves = randomIntBetween(1, 50);
            long[] work = new long[leaves];
            boolean[] splittable = new boolean[leaves];
            for (int i = 0; i < leaves; i++) {
                work[i] = randomBoolean() ? randomLongBetween(0, 1_000) : randomLongBetween(0, 1_000_000);
                splittable[i] = randomBoolean();
            }
            int parallelism = randomIntBetween(1, 32);
            int[][] slices = IVFScoringManager.slices(work, splittable, parallelism);
            assertTrue(slices.length + " slices for " + parallelism, slices.length <= Math.min(parallelism, 10));
            int[] parts = new int[leaves];
            for (int[] slice : slices) {
                for (int leaf : slice) {
                    parts[leaf]++;
                }
            }
            for (int i = 0; i < leaves; i++) {
                assertTrue("leaf " + i + " not searched", parts[i] >= 1);
                assertTrue("leaf " + i + " cannot be split", splittable[i] || parts[i] == 1);
            }
            long total = Arrays.stream(work).sum();
            for (int[] slice : slices) {
                double load = 0;
                for (int leaf : slice) {
                    load += (double) work[leaf] / parts[leaf];
                }
                assertTrue(load + " of " + total, slices.length == 1 || load >= 0.1 * total * (1 - 1e-9));
            }
        }
    }

    /** With slices on, a shard returns exactly what one-task-per-leaf search returns. */
    public void testSlicedSearchMatchesPerLeafSearch() throws IOException {
        int dims = randomIntBetween(8, 32);
        IndexWriterConfig config = newIndexWriterConfig().setCodec(
            TestUtil.alwaysKnnVectorsFormat(new ES950DiskBBQVectorsFormat(64, randomIntBetween(2, 8)))
        ).setMergePolicy(NoMergePolicy.INSTANCE).setMaxBufferedDocs(50_000).setRAMBufferSizeMB(256);
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, config)) {
            int segments = randomIntBetween(1, 6);
            for (int s = 0; s < segments; s++) {
                // the first segment holds most of the work, so it is split into parts
                int docs = s == 0 ? randomIntBetween(15_000, 20_000) : randomIntBetween(10, 1_500);
                for (int i = 0; i < docs; i++) {
                    Document doc = new Document();
                    doc.add(
                        new KnnFloatVectorField(
                            "v",
                            VectorTestUtils.randomNormalizedFloatVector(dims),
                            VectorSimilarityFunction.DOT_PRODUCT
                        )
                    );
                    doc.add(new StringField("tag", randomFrom("a", "b", "c"), Field.Store.NO));
                    writer.addDocument(doc);
                }
                writer.flush();
            }
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                warmUp(reader, executor, dims);
                for (int q = 0; q < 10; q++) {
                    float[] vector = VectorTestUtils.randomNormalizedFloatVector(dims);
                    Query filter = randomBoolean() ? null : new TermQuery(new Term("tag", "a"));
                    float visitRatio = randomFrom(0f, 0.05f, 0.3f, 0.5f);
                    int k = randomIntBetween(1, 50);
                    assertSlicedMatchesPerLeaf(reader, executor, vector, k, filter, visitRatio);
                }
            }
        } finally {
            terminate(executor);
        }
    }

    /**
     * A field can hold segments of other formats (e.g. flat ones written before its index options changed to bbq_disk).
     * Only DiskBBQ segments may be split: other formats ignore the shared scan, so each part would search the whole segment.
     */
    public void testSegmentsOfOtherFormatsAreNotSplit() throws IOException {
        int dims = randomIntBetween(8, 32);
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try (Directory dir = newDirectory()) {
            // plain configs: the randomized one may flush more segments than the two this test builds
            IndexWriterConfig flatConfig = new IndexWriterConfig().setCodec(TestUtil.getDefaultCodec()).setRAMBufferSizeMB(256);
            IndexWriterConfig ivfConfig = new IndexWriterConfig().setCodec(
                TestUtil.alwaysKnnVectorsFormat(new ES950DiskBBQVectorsFormat(64, 4))
            ).setMergePolicy(NoMergePolicy.INSTANCE).setRAMBufferSizeMB(256);
            for (IndexWriterConfig config : List.of(flatConfig, ivfConfig)) {
                try (IndexWriter writer = new IndexWriter(dir, config)) {
                    for (int i = 0; i < 20_000; i++) {
                        Document doc = new Document();
                        doc.add(
                            new KnnFloatVectorField(
                                "v",
                                VectorTestUtils.randomNormalizedFloatVector(dims),
                                VectorSimilarityFunction.DOT_PRODUCT
                            )
                        );
                        writer.addDocument(doc);
                    }
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals(2, reader.leaves().size());
                assertSlicedMatchesPerLeaf(reader, executor, VectorTestUtils.randomNormalizedFloatVector(dims), 10, null, 0.5f);
            }
        } finally {
            terminate(executor);
        }
    }

    /**
     * Panama's float reductions don't fix their order of additions, so interpreted and compiled code can round the last bit
     * differently. Searching first lets the bit-exact comparisons compare compiled code with compiled code.
     */
    private static void warmUp(DirectoryReader reader, ExecutorService executor, int dims) throws IOException {
        for (int q = 0; q < 30; q++) {
            float[] vector = VectorTestUtils.randomNormalizedFloatVector(dims);
            searchWith(reader, executor, randomBoolean(), query(vector, 10, null, randomFrom(0f, 0.05f, 0.5f)), 10);
        }
    }

    private static IVFKnnFloatVectorQuery query(float[] vector, int k, Query filter, float visitRatio) {
        TestIvfQueryConfigResolver resolver = new TestIvfQueryConfigResolver(
            CentroidIndexFormat.FLAT,
            QuantEncoding.ONE_BIT_4BIT_QUERY,
            false,
            1.0f
        );
        return new IVFKnnFloatVectorQuery("v", vector, k, k, filter, visitRatio, resolver);
    }

    /** Searches with the ContextIndexSearcher Elasticsearch uses; a null executor runs every slice on the caller. */
    private static TopDocs searchWith(DirectoryReader reader, ExecutorService executor, boolean slices, Query query, int k)
        throws IOException {
        IndexSearcher plain = new IndexSearcher(reader);
        ContextIndexSearcher searcher = executor == null
            ? new ContextIndexSearcher(reader, plain.getSimilarity(), plain.getQueryCache(), plain.getQueryCachingPolicy(), false)
            : new ContextIndexSearcher(
                reader,
                plain.getSimilarity(),
                plain.getQueryCache(),
                plain.getQueryCachingPolicy(),
                randomBoolean(),
                executor,
                4,
                1
            );
        searcher.setIvfSlicesEnabled(slices);
        return searcher.search(query, k);
    }

    /** Searches with slices on or off, and checks both paths return the same hits for the same work. */
    private static void assertSlicedMatchesPerLeaf(
        DirectoryReader reader,
        ExecutorService executor,
        float[] vector,
        int k,
        Query filter,
        float visitRatio
    ) throws IOException {
        IVFKnnFloatVectorQuery perLeafQuery = query(vector, k, filter, visitRatio);
        TopDocs perLeaf = searchWith(reader, executor, false, perLeafQuery, k);
        IVFKnnFloatVectorQuery slicedQuery = query(vector, k, filter, visitRatio);
        TopDocs sliced = searchWith(reader, executor, true, slicedQuery, k);
        assertEquals("vector ops", perLeafQuery.totalVectorOps(), slicedQuery.totalVectorOps());
        assertEquals(perLeaf.scoreDocs.length, sliced.scoreDocs.length);
        for (int i = 0; i < perLeaf.scoreDocs.length; i++) {
            assertEquals(perLeaf.scoreDocs[i].doc, sliced.scoreDocs[i].doc);
            assertEquals(perLeaf.scoreDocs[i].score, sliced.scoreDocs[i].score, 0f);
        }
    }

    private static void assertSlices(long[] work, boolean[] splittable, int parallelism, int[][] expected) {
        int[][] actual = IVFScoringManager.slices(work, splittable, parallelism);
        List<String> actualList = new ArrayList<>();
        for (int[] slice : actual) {
            actualList.add(Arrays.toString(slice));
        }
        List<String> expectedList = new ArrayList<>();
        for (int[] slice : expected) {
            expectedList.add(Arrays.toString(slice));
        }
        assertEquals(expectedList, actualList);
    }

    private static int[][] copies(int[] slice, int n) {
        int[][] slices = new int[n][];
        Arrays.fill(slices, slice);
        return slices;
    }

    private static boolean[] all(int n) {
        boolean[] splittable = new boolean[n];
        Arrays.fill(splittable, true);
        return splittable;
    }
}
