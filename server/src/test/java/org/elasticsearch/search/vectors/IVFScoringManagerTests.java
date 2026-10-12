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

    private static final long MAX = 1_000;

    public void testForceMergedLeafIsSplitIntoAtMostMaxParts() {
        assertSlices(new long[] { 50 * MAX }, all(1), new int[][] { { 0 }, { 0 }, { 0 }, { 0 }, { 0 } });
        assertSlices(new long[] { 3 * MAX }, all(1), new int[][] { { 0 }, { 0 }, { 0 } });
    }

    public void testSmallLeavesAreGroupedLikeLucene() {
        // grouped by decreasing work until a slice passes MAX or holds 5 leaves
        long[] work = { 300, 200, 100, 400, 50, 10, 10 };
        assertSlices(work, all(work.length), new int[][] { { 3, 0, 1, 2, 4 }, { 5, 6 } });
        long[] heavier = { 600, 500, 400 };
        assertSlices(heavier, all(heavier.length), new int[][] { { 0, 1 }, { 2 } });
        long[] tiny = new long[12];
        Arrays.fill(tiny, 10);
        assertSlices(tiny, all(tiny.length), new int[][] { { 0, 1, 2, 3, 4 }, { 5, 6, 7, 8, 9 }, { 10, 11 } });
    }

    public void testBigLeavesGetTheirOwnSlicesFirst() {
        long[] work = { 100, 2_500, 100, 100 }; // one big segment and a few small ones
        assertSlices(work, all(work.length), new int[][] { { 1 }, { 1 }, { 1 }, { 0, 2, 3 } });
    }

    public void testLeavesThatCannotBeSplitStayWhole() {
        long[] work = { 10 * MAX, 100 };
        boolean[] splittable = { false, true };
        assertSlices(work, splittable, new int[][] { { 0 }, { 1 } });
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
                // the first segment is big enough that a visit ratio of 0.3+ splits it (work > MAX_WORK_PER_SLICE)
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
     * A segment holding nothing near the query stops after the first round, and re-entry decisions don't depend on threads:
     * the same hits and work with and without an executor.
     */
    public void testReentryStopsLeavesWithNothingCompetitiveAndIsDeterministic() throws IOException {
        int dims = 16;
        float[] target = VectorTestUtils.randomNormalizedFloatVector(dims);
        // a plain config: the randomized one may flush more segments than the two this test builds
        IndexWriterConfig config = new IndexWriterConfig().setCodec(TestUtil.alwaysKnnVectorsFormat(new ES950DiskBBQVectorsFormat(64, 4)))
            .setMergePolicy(NoMergePolicy.INSTANCE)
            .setRAMBufferSizeMB(256);
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, config)) {
            for (float sign : new float[] { 1f, -1f }) { // one segment around the target, one around its opposite
                for (int i = 0; i < 4_000; i++) {
                    Document doc = new Document();
                    doc.add(new KnnFloatVectorField("v", near(target, sign), VectorSimilarityFunction.DOT_PRODUCT));
                    writer.addDocument(doc);
                }
                writer.flush();
            }
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                assertEquals(2, reader.leaves().size());
                IVFKnnFloatVectorQuery full = query(target, 10, null, 0.5f);
                searchWith(reader, executor, true, false, full, 10);
                IVFKnnFloatVectorQuery reentered = query(target, 10, null, 0.5f);
                TopDocs concurrent = searchWith(reader, executor, true, true, reentered, 10);
                IVFKnnFloatVectorQuery sequential = query(target, 10, null, 0.5f);
                TopDocs alone = searchWith(reader, null, true, true, sequential, 10);

                assertTrue(reentered.totalVectorOps() + " < " + full.totalVectorOps(), reentered.totalVectorOps() < full.totalVectorOps());
                assertEquals(reentered.totalVectorOps(), sequential.totalVectorOps());
                assertEquals(concurrent.scoreDocs.length, alone.scoreDocs.length);
                for (int i = 0; i < concurrent.scoreDocs.length; i++) {
                    assertEquals(concurrent.scoreDocs[i].doc, alone.scoreDocs[i].doc);
                    assertEquals(concurrent.scoreDocs[i].score, alone.scoreDocs[i].score, 0f);
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
    private static TopDocs searchWith(DirectoryReader reader, ExecutorService executor, boolean slices, boolean reentry, Query query, int k)
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
        searcher.setIvfReentryEnabled(reentry);
        return searcher.search(query, k);
    }

    /** A unit vector close to {@code sign * target}. */
    private static float[] near(float[] target, float sign) {
        float[] noise = VectorTestUtils.randomNormalizedFloatVector(target.length);
        float[] v = new float[target.length];
        double norm = 0;
        for (int i = 0; i < v.length; i++) {
            v[i] = sign * target[i] + 0.3f * noise[i];
            norm += v[i] * v[i];
        }
        for (int i = 0; i < v.length; i++) {
            v[i] /= (float) Math.sqrt(norm);
        }
        return v;
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
        TopDocs perLeaf = searchWith(reader, executor, false, false, perLeafQuery, k);
        IVFKnnFloatVectorQuery slicedQuery = query(vector, k, filter, visitRatio);
        TopDocs sliced = searchWith(reader, executor, true, false, slicedQuery, k);
        assertEquals("vector ops", perLeafQuery.totalVectorOps(), slicedQuery.totalVectorOps());
        assertEquals(perLeaf.scoreDocs.length, sliced.scoreDocs.length);
        for (int i = 0; i < perLeaf.scoreDocs.length; i++) {
            assertEquals(perLeaf.scoreDocs[i].doc, sliced.scoreDocs[i].doc);
            assertEquals(perLeaf.scoreDocs[i].score, sliced.scoreDocs[i].score, 0f);
        }
    }

    private static void assertSlices(long[] work, boolean[] splittable, int[][] expected) {
        int[][] actual = IVFScoringManager.slices(work, splittable, MAX, 5, 5);
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

    private static boolean[] all(int n) {
        boolean[] splittable = new boolean[n];
        Arrays.fill(splittable, true);
        return splittable;
    }
}
