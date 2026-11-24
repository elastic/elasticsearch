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
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.codec.vectors.VectorTestUtils;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidIndexFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.QuantEncoding;
import org.elasticsearch.index.codec.vectors.diskbbq.TestIvfQueryConfigResolver;
import org.elasticsearch.index.codec.vectors.diskbbq.es95.ES950DiskBBQVectorsFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.vectors.AbstractIVFKnnVectorQuery.IVFCollectorManager;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.apache.lucene.index.VectorSimilarityFunction.DOT_PRODUCT;

public class IVFParallelScanTests extends ESTestCase {

    // In the shape tests below, a leaf's work is given in multiples of the least work a part may have.

    public void testForceMergedLeafGetsOnePartPerTask() {
        assertParts(new long[] { 72 }, 8, 8);
        assertParts(new long[] { 72 }, 16, 10); // no part under 10% of the work
        assertParts(new long[] { 72 }, 1, 1);
    }

    public void testASmallLeafNextToABigOneStaysWhole() {
        // the big leaf holds 7.9 tasks' worth of work: rounded, it keeps every thread busy
        assertParts(new long[] { 71, 1 }, 8, 8, 1);
    }

    /** Leaves with less than a task's work are searched whole, one task each: never fewer tasks than without a parallel scan. */
    public void testSmallLeavesAreNeitherSplitNorGrouped() {
        assertParts(new long[] { 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10 }, 8, ones(12));
        assertParts(new long[] { 40, 20, 10, 5, 2, 1 }, 4, 2, 1, 1, 1, 1, 1);
    }

    public void testNoPartHasLessThanTenPercentOfTheWork() {
        // 85% of the work is 8.5 tasks' worth with 10 tasks; split in nine, each part would hold under 10%
        assertParts(new long[] { 15, 85 }, 16, 1, 8);
    }

    public void testRoundingNeverMakesMorePartsThanTasks() {
        // three leaves hold 2.5 tasks' worth each: rounded up, they would make 9 parts for 8 tasks
        assertParts(new long[] { 25, 25, 25, 5 }, 8, 3, 3, 2, 1);
    }

    /** A leaf is split into no more parts than it has work for, however many tasks may run: a small leaf stays whole. */
    public void testNoPartHasLessThanTheMinimumWork() {
        assertParts(new long[] { 1 }, 8, 1);
        assertParts(new long[] { 2 }, 8, 2);
        assertParts(new long[] { 5 }, 8, 5);
        assertParts(new long[] { 5, 1 }, 8, 5, 1);
        long min = IVFParallelScan.MIN_PART_WORK;
        boolean[] splittable = { true };
        assertArrayEquals(new int[] { 1 }, IVFParallelScan.partsPerLeaf(new long[] { 2 * min - 1 }, splittable, 8));
        assertArrayEquals(new int[] { 2 }, IVFParallelScan.partsPerLeaf(new long[] { 2 * min }, splittable, 8));
        assertArrayEquals(new int[] { 2 }, IVFParallelScan.partsPerLeaf(new long[] { 3 * min - 1 }, splittable, 8));
    }

    public void testLeavesThatCannotBeSplitStayWhole() {
        long min = IVFParallelScan.MIN_PART_WORK;
        assertArrayEquals(
            new int[] { 1, 1 },
            IVFParallelScan.partsPerLeaf(new long[] { 100 * min, min }, new boolean[] { false, true }, 4)
        );
    }

    /**
     * Whatever the shape: every leaf is searched, a leaf that cannot be split stays whole, the split leaves make no more parts
     * than tasks may run, and no part holds under 10% of the shard's work or under the minimum work of a part.
     */
    public void testPartsKeepTheirGuarantees() {
        for (int iter = 0; iter < 200; iter++) {
            int leaves = randomIntBetween(1, 50);
            long[] work = new long[leaves];
            boolean[] splittable = new boolean[leaves];
            for (int i = 0; i < leaves; i++) {
                work[i] = randomBoolean() ? randomLongBetween(0, 1_000) : randomLongBetween(0, 1_000_000);
                splittable[i] = randomBoolean();
            }
            int parallelism = randomIntBetween(1, 32);
            int[] parts = IVFParallelScan.partsPerLeaf(work, splittable, parallelism);
            long total = Arrays.stream(work).sum();
            int partsOfSplitLeaves = 0;
            for (int i = 0; i < leaves; i++) {
                assertTrue("leaf " + i + " not searched", parts[i] >= 1);
                assertTrue("leaf " + i + " cannot be split", splittable[i] || parts[i] == 1);
                if (parts[i] > 1) {
                    partsOfSplitLeaves += parts[i];
                    double workPerPart = (double) work[i] / parts[i];
                    assertTrue(workPerPart + " of " + total, workPerPart >= 0.1 * total * (1 - 1e-9));
                    assertTrue(workPerPart + " per part", workPerPart >= IVFParallelScan.MIN_PART_WORK);
                }
            }
            assertTrue(partsOfSplitLeaves + " parts for " + parallelism, partsOfSplitLeaves <= Math.min(parallelism, 10));
        }
    }

    /** With the parallel scan on, a shard returns exactly what one-task-per-leaf search returns. */
    public void testParallelScanMatchesPerLeafSearch() throws IOException {
        int dims = randomIntBetween(8, 32);
        IndexWriterConfig config = newIndexWriterConfig().setCodec(
            TestUtil.alwaysKnnVectorsFormat(new ES950DiskBBQVectorsFormat(64, randomIntBetween(2, 8)))
        ).setMergePolicy(NoMergePolicy.INSTANCE).setMaxBufferedDocs(50_000).setRAMBufferSizeMB(256);
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, config)) {
            int segments = randomIntBetween(1, 6);
            for (int s = 0; s < segments; s++) {
                // the first segment holds most of the work, so it is split into parts when the budget is big enough
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
                    if (randomInt(99) == 0) {
                        doc.add(new StringField("rare", "yes", Field.Store.NO));
                    }
                    writer.addDocument(doc);
                }
                writer.flush();
            }
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                warmUp(reader, executor, dims);
                for (int q = 0; q < 10; q++) {
                    float[] vector = VectorTestUtils.randomNormalizedFloatVector(dims);
                    // with the rare tag, a budget often ends before enough docs were found, so the search goes on past it
                    Query filter = randomFrom(
                        Arrays.asList(null, new TermQuery(new Term("tag", "a")), new TermQuery(new Term("rare", "yes")))
                    );
                    float visitRatio = randomFrom(0f, 0.05f, 0.3f, 0.5f);
                    int k = randomIntBetween(1, 50);
                    assertParallelScanMatchesPerLeaf(reader, executor, () -> query(vector, k, filter, visitRatio), k);
                }
            }
        } finally {
            terminate(executor);
        }
    }

    /**
     * A nested field: each parent doc has several child vectors, and a search returns the best child of each of the nearest
     * parents. With the parallel scan on, a shard returns the same parents and children as without it, also when scores tie.
     */
    public void testNestedParallelScanMatchesPerLeafSearch() throws IOException {
        int dims = randomIntBetween(8, 32);
        float[] repeated = VectorTestUtils.randomNormalizedFloatVector(dims);
        IndexWriterConfig config = newIndexWriterConfig().setCodec(
            TestUtil.alwaysKnnVectorsFormat(new ES950DiskBBQVectorsFormat(64, randomIntBetween(2, 8)))
        ).setMergePolicy(NoMergePolicy.INSTANCE).setMaxBufferedDocs(50_000).setRAMBufferSizeMB(256);
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, config)) {
            int segments = randomIntBetween(1, 4);
            for (int s = 0; s < segments; s++) {
                // the first segment holds most of the work, so it is split into parts when the budget is big enough
                int parents = s == 0 ? randomIntBetween(3_000, 4_000) : randomIntBetween(5, 300);
                for (int p = 0; p < parents; p++) {
                    List<Document> block = new ArrayList<>();
                    int children = randomIntBetween(1, 8);
                    // The chunks of one doc are often alike: they then land in the same posting lists, so many children are
                    // scored for few parents, and their scores tie. Copies of one vector make parents tie with each other too.
                    float[] alike = randomInt(3) == 0 ? null
                        : randomInt(4) == 0 ? repeated
                        : VectorTestUtils.randomNormalizedFloatVector(dims);
                    for (int c = 0; c < children; c++) {
                        float[] vector = alike != null ? alike : VectorTestUtils.randomNormalizedFloatVector(dims);
                        Document child = new Document();
                        child.add(new KnnFloatVectorField("v", vector, VectorSimilarityFunction.DOT_PRODUCT));
                        if (randomInt(49) == 0) {
                            child.add(new StringField("rare", "yes", Field.Store.NO));
                        }
                        block.add(child);
                    }
                    Document parent = new Document();
                    parent.add(new StringField("docType", "_parent", Field.Store.NO));
                    block.add(parent);
                    writer.addDocuments(block);
                }
                writer.flush();
            }
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                BitSetProducer parents = new QueryBitSetProducer(new TermQuery(new Term("docType", "_parent")));
                warmUp(reader, executor, dims);
                for (int q = 0; q < 10; q++) {
                    float[] vector = randomBoolean() ? repeated : VectorTestUtils.randomNormalizedFloatVector(dims);
                    // A budget often ends before enough parents were found, so the search has to go on past it: a small budget
                    // (the leaf is then searched whole), or a big one with the rare tag (the leaf is then split).
                    float visitRatio = randomFrom(0f, 0.05f, 0.5f, 0.9f);
                    Query filter = randomBoolean() ? null : new TermQuery(new Term("rare", "yes"));
                    int k = randomIntBetween(1, 50);
                    assertParallelScanMatchesPerLeaf(
                        reader,
                        executor,
                        () -> new DiversifyingChildrenIVFKnnFloatVectorQuery("v", vector, k, k, filter, parents, visitRatio, resolver()),
                        k
                    );
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
                float[] vector = VectorTestUtils.randomNormalizedFloatVector(dims);
                assertParallelScanMatchesPerLeaf(reader, executor, () -> query(vector, 10, null, 0.5f), 10);
            }
        } finally {
            terminate(executor);
        }
    }

    /**
     * A part that starts after the other parts took all of its segment's work returns before it creates a collector. Here the
     * tasks run one after another, so the first part takes all the work and every other part starts late: the search creates
     * two collectors, one to prepare the segment and one to scan it, however many parts the segment has.
     */
    public void testLatePartsCreateNoCollector() throws IOException {
        int dims = randomIntBetween(8, 32);
        int k = 10;
        try (Directory dir = newDirectory()) {
            indexLeaves(dir, dims, 20_000);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals(1, reader.leaves().size());
                RecordingCollectorManager collectors = new RecordingCollectorManager(k);
                IVFParallelScan scan = new IVFParallelScan(
                    query(VectorTestUtils.randomNormalizedFloatVector(dims), k, null, 0.5f),
                    null,
                    0.5f,
                    List.of(new AbstractIVFKnnVectorQuery.LeafInput(reader.leaves().get(0), collectors, false)),
                    randomIntBetween(2, 8)
                );
                TopDocs[] hits = scan.search(new TaskExecutor(Runnable::run));
                assertTrue("the segment was searched", hits[0].scoreDocs.length > 0);
                assertEquals("one collector prepares the segment and one scans it", 2, collectors.sharingAScan.get());
                assertEquals(0, collectors.onTheirOwn.get());
            }
        }
    }

    /**
     * A leaf that is not split is searched as without a parallel scan: its collector does not share a scan. Only the leaf
     * holding most of the work is split, and nothing is split when the tasks cannot run in parallel.
     */
    public void testOnlySplitLeavesShareAScan() throws IOException {
        int dims = randomIntBetween(8, 32);
        int k = 10;
        try (Directory dir = newDirectory()) {
            indexLeaves(dir, dims, 20_000, 300, 300);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals(3, reader.leaves().size());
                for (int maxParallelTasks : new int[] { 4, 1 }) {
                    List<RecordingCollectorManager> collectors = new ArrayList<>();
                    List<AbstractIVFKnnVectorQuery.LeafInput> inputs = new ArrayList<>();
                    for (LeafReaderContext leaf : reader.leaves()) {
                        collectors.add(new RecordingCollectorManager(k));
                        inputs.add(new AbstractIVFKnnVectorQuery.LeafInput(leaf, collectors.get(leaf.ord), false));
                    }
                    float[] vector = VectorTestUtils.randomNormalizedFloatVector(dims);
                    TopDocs[] hits = new IVFParallelScan(query(vector, k, null, 0.5f), null, 0.5f, inputs, maxParallelTasks).search(
                        new TaskExecutor(Runnable::run)
                    );
                    for (int leaf = 0; leaf < 3; leaf++) {
                        assertTrue("leaf " + leaf + " was searched", hits[leaf].scoreDocs.length > 0);
                        boolean split = leaf == 0 && maxParallelTasks > 1;
                        assertEquals("leaf " + leaf + ", on its own", split ? 0 : 1, collectors.get(leaf).onTheirOwn.get());
                        assertEquals("leaf " + leaf + ", sharing a scan", split, collectors.get(leaf).sharingAScan.get() > 0);
                    }
                }
            }
        }
    }

    /**
     * A query type that has not opted in to splitting its leaves is searched as without a parallel scan: none of its
     * collectors shares a scan, also for a leaf that holds nearly all the work.
     */
    public void testAQueryThatCannotSplitSkipsTheParallelScan() throws IOException {
        int dims = randomIntBetween(8, 32);
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try (Directory dir = newDirectory()) {
            indexLeaves(dir, dims, 20_000, 300);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                for (boolean optsIn : new boolean[] { true, false }) {
                    List<RecordingCollectorManager> leaves = new ArrayList<>();
                    float[] vector = VectorTestUtils.randomNormalizedFloatVector(dims);
                    Query query = new IVFKnnFloatVectorQuery("v", vector, 10, 10, null, 0.5f, resolver()) {
                        @Override
                        boolean canSplitLeaves() {
                            return optsIn || super.canSplitLeaves(); // a subclass cannot split its leaves unless it says so
                        }

                        @Override
                        protected IVFCollectorManager getKnnCollectorManager(int k, SharedMinCompetitiveScore sharedScore) {
                            RecordingCollectorManager leaf = new RecordingCollectorManager(k, sharedScore);
                            leaves.add(leaf);
                            return leaf;
                        }
                    };
                    assertEquals(10, searchWith(reader, executor, true, query, 10).scoreDocs.length);
                    assertEquals(2, leaves.size());
                    int sharingAScan = leaves.get(0).sharingAScan.get() + leaves.get(1).sharingAScan.get();
                    assertEquals("opts in: " + optsIn, optsIn, sharingAScan > 0);
                }
            }
        } finally {
            terminate(executor);
        }
    }

    /**
     * A shard with no leaf worth splitting is searched as without a parallel scan. With tasks that run one after another,
     * that shows in the order its leaves are searched: their own order, where a parallel scan starts with the biggest leaf.
     */
    public void testAShardWithNothingToSplitIsSearchedAsWithoutAParallelScan() throws IOException {
        int dims = randomIntBetween(8, 32);
        // at a visit ratio of 0.5 a leaf's work is its number of vectors: the middle leaf has two parts' worth only when big
        for (int middleLeaf : new int[] { 3_000, 20_000 }) {
            try (Directory dir = newDirectory()) {
                indexLeaves(dir, dims, 300, middleLeaf, 300);
                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    List<Integer> searched = new ArrayList<>();
                    float[] vector = VectorTestUtils.randomNormalizedFloatVector(dims);
                    Query query = new IVFKnnFloatVectorQuery("v", vector, 10, 10, null, 0.5f, resolver()) {
                        @Override
                        boolean canSplitLeaves() {
                            return true;
                        }

                        @Override
                        protected IVFCollectorManager getKnnCollectorManager(int k, SharedMinCompetitiveScore sharedScore) {
                            return new IVFCollectorManager(k, sharedScore) {
                                @Override
                                public AbstractMaxScoreKnnCollector newCollector(
                                    int limit,
                                    KnnSearchStrategy strategy,
                                    LeafReaderContext ctx
                                ) throws IOException {
                                    searched.add(ctx.ord);
                                    return super.newCollector(limit, strategy, ctx);
                                }
                            };
                        }
                    };
                    assertEquals(10, searchWith(reader, EsExecutors.DIRECT_EXECUTOR_SERVICE, true, query, 10).scoreDocs.length);
                    if (middleLeaf < 2 * IVFParallelScan.MIN_PART_WORK) {
                        assertEquals(List.of(0, 1, 2), searched);
                    } else {
                        assertEquals("the split leaf is prepared first", 1, (int) searched.get(0));
                    }
                }
            }
        }
    }

    /**
     * The leaves of a shard share a score cutoff, and the parts of a split leaf take part in it like any leaf's collector.
     * Here only the split leaf can publish one: the other leaf has fewer vectors than its collector holds. The tasks run one
     * after another, so that leaf is searched last and starts from the cutoff the parts published.
     */
    public void testPartsShareTheCutoffOfTheirShard() throws IOException {
        int dims = randomIntBetween(8, 32);
        int k = 10;
        try (Directory dir = newDirectory()) {
            indexLeaves(dir, dims, 20_000, k / 2);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                SharedMinCompetitiveScore cutoff = new SharedMinCompetitiveScore();
                List<RecordingCollectorManager> leaves = new ArrayList<>();
                List<AbstractIVFKnnVectorQuery.LeafInput> inputs = new ArrayList<>();
                for (LeafReaderContext leaf : reader.leaves()) {
                    leaves.add(new RecordingCollectorManager(k, cutoff));
                    inputs.add(new AbstractIVFKnnVectorQuery.LeafInput(leaf, leaves.get(leaf.ord), false));
                }
                float[] vector = VectorTestUtils.randomNormalizedFloatVector(dims);
                new IVFParallelScan(query(vector, k, null, 0.5f), null, 0.5f, inputs, 4).search(new TaskExecutor(Runnable::run));

                assertTrue("the big leaf was split", leaves.get(0).sharingAScan.get() > 0);
                assertTrue("a part published its cutoff", cutoff.get() > AbstractMaxScoreKnnCollector.LEAST_COMPETITIVE);
                assertEquals(1, leaves.get(1).collectors.size());
                assertEquals("the small leaf started from it", cutoff.get(), leaves.get(1).collectors.get(0).getMinCompetitiveDocScore());
            }
        }
    }

    /**
     * A filter skips docs, so a filtered search goes on past its budget, until it has scored about half as many docs as the
     * same search without the filter. Here the filter keeps a quarter of the docs: a search that stopped at its budget would
     * score a quarter as many. A leaf searched whole and the part that goes on past the budget of a split leaf follow the
     * same rule, so comparing the two cannot show a search that stops too early.
     */
    public void testAFilteredSearchGoesOnPastItsBudget() throws IOException {
        int dims = randomIntBetween(8, 32);
        IndexWriterConfig config = new IndexWriterConfig().setCodec(TestUtil.alwaysKnnVectorsFormat(new ES950DiskBBQVectorsFormat(64, 4)))
            .setMergePolicy(NoMergePolicy.INSTANCE)
            .setRAMBufferSizeMB(256);
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, config)) {
                for (int i = 0; i < 18_000; i++) {
                    Document doc = new Document();
                    doc.add(new KnnFloatVectorField("v", VectorTestUtils.randomNormalizedFloatVector(dims), DOT_PRODUCT));
                    doc.add(new StringField("tag", randomFrom("a", "b", "c", "d"), Field.Store.NO));
                    writer.addDocument(doc);
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                float[] vector = VectorTestUtils.randomNormalizedFloatVector(dims);
                Query filter = new TermQuery(new Term("tag", "a"));
                float visitRatio = 0.3f; // a budget big enough for the leaf to be split
                for (boolean parallelScan : new boolean[] { false, true }) {
                    long scored = hits(reader, executor, parallelScan, query(vector, 10, null, visitRatio), 10).vectorOps();
                    long scoredWithFilter = hits(reader, executor, parallelScan, query(vector, 10, filter, visitRatio), 10).vectorOps();
                    String what = scoredWithFilter + " docs scored with the filter, " + scored + " without, parallel scan " + parallelScan;
                    assertTrue(what, scoredWithFilter >= 0.4 * scored);
                }
            }
        } finally {
            terminate(executor);
        }
    }

    // Data slices. A query for exactly one slice searches a leaf with a single scan, of that slice's posting lists, so its
    // leaves can be split like any other query's.

    public void testAFloatQueryForOneSliceMatchesPerLeafSearch() throws IOException {
        int dims = randomIntBetween(8, 32);
        Supplier<Field> field = () -> new KnnFloatVectorField("v", VectorTestUtils.randomNormalizedFloatVector(dims), DOT_PRODUCT);
        assertOneSliceMatchesPerLeafSearch(field, false, (k, filter, visitRatio, slice) -> {
            float[] vector = VectorTestUtils.randomNormalizedFloatVector(dims);
            return () -> new IVFKnnFloatSlicedVectorQuery("v", vector, k, k, filter, visitRatio, resolver(), SLICE, slice);
        });
    }

    public void testAByteQueryForOneSliceMatchesPerLeafSearch() throws IOException {
        int dims = randomIntBetween(8, 32);
        Supplier<Field> field = () -> new KnnByteVectorField("v", VectorTestUtils.randomByteVector(dims), DOT_PRODUCT);
        assertOneSliceMatchesPerLeafSearch(field, false, (k, filter, visitRatio, slice) -> {
            byte[] vector = VectorTestUtils.randomByteVector(dims);
            return () -> new IVFKnnByteSlicedVectorQuery("v", vector, k, k, filter, visitRatio, resolver(), SLICE, slice);
        });
    }

    public void testANestedQueryForOneSliceMatchesPerLeafSearch() throws IOException {
        int dims = randomIntBetween(8, 32);
        Supplier<Field> field = () -> new KnnFloatVectorField("v", VectorTestUtils.randomNormalizedFloatVector(dims), DOT_PRODUCT);
        BitSetProducer parents = new QueryBitSetProducer(new TermQuery(new Term("docType", "_parent")));
        assertOneSliceMatchesPerLeafSearch(field, true, (k, filter, visitRatio, slice) -> {
            float[] vector = VectorTestUtils.randomNormalizedFloatVector(dims);
            return () -> new DiversifyingChildrenIVFKnnFloatSlicedVectorQuery(
                "v",
                vector,
                k,
                k,
                filter,
                parents,
                visitRatio,
                resolver(),
                SLICE,
                slice
            );
        });
    }

    public void testANestedByteQueryForOneSliceMatchesPerLeafSearch() throws IOException {
        int dims = randomIntBetween(8, 32);
        Supplier<Field> field = () -> new KnnByteVectorField("v", VectorTestUtils.randomByteVector(dims), DOT_PRODUCT);
        BitSetProducer parents = new QueryBitSetProducer(new TermQuery(new Term("docType", "_parent")));
        assertOneSliceMatchesPerLeafSearch(field, true, (k, filter, visitRatio, slice) -> {
            byte[] vector = VectorTestUtils.randomByteVector(dims);
            return () -> new DiversifyingChildrenIVFKnnByteSlicedVectorQuery(
                "v",
                vector,
                k,
                k,
                filter,
                parents,
                visitRatio,
                resolver(),
                SLICE,
                slice
            );
        });
    }

    /** Makes queries for one slice. Each call takes a new random vector, and its supplier returns equal queries on it. */
    @FunctionalInterface
    private interface OneSliceQueries {
        Supplier<AbstractIVFKnnVectorQuery> next(int k, Query filter, float visitRatio, BytesRef slice);
    }

    /**
     * With the parallel scan on, a query for one slice returns exactly what it returns without it, whichever slice it asks
     * for, with and without a filter.
     */
    private static void assertOneSliceMatchesPerLeafSearch(Supplier<Field> vectorField, boolean nested, OneSliceQueries queries)
        throws IOException {
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try (Directory dir = newDirectory()) {
            // Slice 0 is most of the first leaf, so a search of it there is split when its budget is big enough. The last
            // leaf has no doc of it.
            int[][] vectorsPerSlice = { { 18_000, 600, 400 }, { 300, 500 }, { 0, 200, 300 } };
            indexSlicedLeaves(dir, vectorField, nested, randomBoolean(), vectorsPerSlice);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                // The first leaf keeps posting lists per slice: with a small budget, a search of slice 0 visits few of its vectors.
                AbstractIVFKnnVectorQuery smallSearch = queries.next(10, null, 0.05f, new BytesRef("slice0")).get();
                searchWith(reader, executor, false, smallSearch, 10);
                assertTrue(smallSearch.totalVectorOps() + " visited", smallSearch.totalVectorOps() < vectorsPerSlice[0][0] / 2);

                // a search of slice 0 with a big budget does split the first leaf: this test is about such searches
                List<AbstractIVFKnnVectorQuery.LeafInput> leaves = new ArrayList<>();
                for (LeafReaderContext leaf : reader.leaves()) {
                    leaves.add(new AbstractIVFKnnVectorQuery.LeafInput(leaf, new IVFCollectorManager(10, null), false));
                }
                AbstractIVFKnnVectorQuery bigSearch = queries.next(10, null, 0.5f, new BytesRef("slice0")).get();
                assertTrue(new IVFParallelScan(bigSearch, null, 0.5f, leaves, 4).splitsALeaf());

                for (int q = 0; q < 50; q++) {
                    // half the searches are of slice 0, and half of those have a budget big enough to split the first leaf
                    BytesRef slice = new BytesRef("slice" + (randomBoolean() ? 0 : randomIntBetween(1, 2)));
                    // with the rare tag, a budget often ends before enough docs were found, so the search goes on past it
                    Query filter = randomFrom(
                        Arrays.asList(null, new TermQuery(new Term("tag", "a")), new TermQuery(new Term("rare", "yes")))
                    );
                    int k = randomIntBetween(1, 50);
                    Supplier<AbstractIVFKnnVectorQuery> query = queries.next(k, filter, randomFrom(0f, 0.05f, 0.5f, 0.5f), slice);
                    if (q < 30) {
                        searchWith(reader, executor, randomBoolean(), query.get(), k); // warms up, see warmUp
                    } else {
                        assertParallelScanMatchesPerLeaf(reader, executor, query, k);
                    }
                }
            }
        } finally {
            terminate(executor);
        }
    }

    /** Several slices are several scans of a leaf, filling one collector in turn: only a query for one slice can split. */
    public void testOnlyAQueryForOneSliceCanSplit() {
        float[] floats = VectorTestUtils.randomNormalizedFloatVector(8);
        byte[] bytes = VectorTestUtils.randomByteVector(8);
        BitSetProducer parents = new QueryBitSetProducer(new TermQuery(new Term("docType", "_parent")));
        for (int slices = 0; slices <= 3; slices++) { // a query that names no slice searches them all
            BytesRef[] ids = new BytesRef[slices];
            Arrays.setAll(ids, slice -> new BytesRef("slice" + slice));
            boolean one = slices == 1;
            assertEquals(one, new IVFKnnFloatSlicedVectorQuery("v", floats, 10, 10, null, 0.5f, resolver(), SLICE, ids).canSplitLeaves());
            assertEquals(one, new IVFKnnByteSlicedVectorQuery("v", bytes, 10, 10, null, 0.5f, resolver(), SLICE, ids).canSplitLeaves());
            assertEquals(
                one,
                new DiversifyingChildrenIVFKnnFloatSlicedVectorQuery("v", floats, 10, 10, null, parents, 0.5f, resolver(), SLICE, ids)
                    .canSplitLeaves()
            );
            assertEquals(
                one,
                new DiversifyingChildrenIVFKnnByteSlicedVectorQuery("v", bytes, 10, 10, null, parents, 0.5f, resolver(), SLICE, ids)
                    .canSplitLeaves()
            );
        }
    }

    /**
     * A leaf's work for a query on one slice is that of the slice, not of the whole leaf: the leaf is split when the slice has
     * two parts' worth of work, and not when only the leaf has. The slice's size is read off the skipper alone, so it is within
     * one skipper interval of docs of the truth.
     */
    public void testALeafIsSizedByTheSliceTheQuerySearches() throws IOException {
        int dims = randomIntBetween(8, 32);
        int[] vectorsPerSlice = { 15_000, 2_000, 300, 5_000 };
        int skipperInterval = 4096; // docs per interval of Lucene's doc values skipper
        try (Directory dir = newDirectory()) {
            Supplier<Field> field = () -> new KnnFloatVectorField("v", VectorTestUtils.randomNormalizedFloatVector(dims), DOT_PRODUCT);
            indexSlicedLeaves(dir, field, false, false, vectorsPerSlice);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReaderContext leaf = reader.leaves().get(0);
                int leafVectors = leaf.reader().getFloatVectorValues("v").size();
                assertEquals(Arrays.stream(vectorsPerSlice).sum(), leafVectors);
                for (int slice = 0; slice < vectorsPerSlice.length; slice++) {
                    float[] vector = VectorTestUtils.randomNormalizedFloatVector(dims);
                    BytesRef id = new BytesRef("slice" + slice);
                    // at a visit ratio of 0.5 a search's work is the number of vectors it considers
                    var query = new IVFKnnFloatSlicedVectorQuery("v", vector, 10, 10, null, 0.5f, resolver(), SLICE, id);
                    int searched = query.searchedVectors(leaf, leafVectors);
                    assertTrue(searched + " for slice " + slice, Math.abs(searched - vectorsPerSlice[slice]) <= skipperInterval);
                    var inputs = List.of(new AbstractIVFKnnVectorQuery.LeafInput(leaf, new IVFCollectorManager(10, null), false));
                    boolean twoPartsWorth = vectorsPerSlice[slice] - skipperInterval >= 2 * IVFParallelScan.MIN_PART_WORK;
                    assertEquals("slice " + slice, twoPartsWorth, new IVFParallelScan(query, null, 0.5f, inputs, 4).splitsALeaf());
                }
            }
        }
    }

    /**
     * Finding how much of a leaf a slice holds costs a lookup, and every query for one slice would pay it. So only a leaf that
     * could be split if the slice held all of it is asked: here the first leaf, not the two small ones.
     */
    public void testOnlyLeavesBigEnoughToSplitAreAskedForTheirSlice() throws IOException {
        int dims = randomIntBetween(8, 32);
        try (Directory dir = newDirectory()) {
            Supplier<Field> field = () -> new KnnFloatVectorField("v", VectorTestUtils.randomNormalizedFloatVector(dims), DOT_PRODUCT);
            indexSlicedLeaves(dir, field, false, false, new int[] { 18_000, 600 }, new int[] { 300, 500 }, new int[] { 2_000 });
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                List<Integer> asked = new ArrayList<>();
                float[] vector = VectorTestUtils.randomNormalizedFloatVector(dims);
                var query = new IVFKnnFloatSlicedVectorQuery("v", vector, 10, 10, null, 0.5f, resolver(), SLICE, new BytesRef("slice0")) {
                    @Override
                    int searchedVectors(LeafReaderContext ctx, int numVectors) throws IOException {
                        asked.add(ctx.ord);
                        return super.searchedVectors(ctx, numVectors);
                    }
                };
                List<AbstractIVFKnnVectorQuery.LeafInput> leaves = new ArrayList<>();
                for (LeafReaderContext leaf : reader.leaves()) {
                    leaves.add(new AbstractIVFKnnVectorQuery.LeafInput(leaf, new IVFCollectorManager(10, null), false));
                }
                assertTrue(new IVFParallelScan(query, null, 0.5f, leaves, 4).splitsALeaf());
                assertEquals(List.of(0), asked);
            }
        }
    }

    private static final String SLICE = "_slice";

    /**
     * Indexes one leaf per entry of {@code vectorsPerSlice}, holding that many vectors of slice 0, of slice 1, and so on.
     * The index is sorted by slice, like every index with data slices. The first leaf is merged from two flushed ones: only
     * a merged segment keeps posting lists per slice, a flushed one keeps a single posting list for all its vectors. With
     * {@code nested}, a vector is a child doc, and up to three children share a parent doc. With {@code deletes}, about one
     * doc in twenty with a vector is deleted.
     */
    private static void indexSlicedLeaves(
        Directory dir,
        Supplier<Field> vectorField,
        boolean nested,
        boolean deletes,
        int[]... vectorsPerSlice
    ) throws IOException {
        IndexWriterConfig config = new IndexWriterConfig().setCodec(
            TestUtil.alwaysKnnVectorsFormat(new ESNextDiskBBQVectorsFormat(64, 4, SLICE))
        ).setIndexSort(new Sort(new SortField(SLICE, SortField.Type.STRING, false, SortField.STRING_LAST))).setRAMBufferSizeMB(256);
        if (nested) {
            config.setParentField(Engine.ROOT_DOC_FIELD_NAME);
        }
        try (IndexWriter writer = new IndexWriter(dir, config)) {
            for (int leaf = 0; leaf < vectorsPerSlice.length; leaf++) {
                int flushes = leaf == 0 ? 2 : 1;
                for (int flush = 0; flush < flushes; flush++) {
                    for (int slice = 0; slice < vectorsPerSlice[leaf].length; slice++) {
                        int vectors = vectorsPerSlice[leaf][slice];
                        int inThisFlush = flush < flushes - 1 ? vectors / flushes : vectors - vectors / flushes * (flushes - 1);
                        addSliceDocs(writer, vectorField, nested, deletes, slice, inThisFlush);
                    }
                    writer.flush();
                }
                if (leaf == 0) {
                    writer.forceMerge(1);
                    writer.getConfig().setMergePolicy(NoMergePolicy.INSTANCE); // every later flush stays a leaf
                }
            }
            writer.deleteDocuments(new Term("gone", "yes")); // without merges, the leaves keep them as deleted docs
        }
    }

    private static void addSliceDocs(
        IndexWriter writer,
        Supplier<Field> vectorField,
        boolean nested,
        boolean deletes,
        int slice,
        int vectors
    ) throws IOException {
        BytesRef sliceKey = SliceIndexing.encodeSliceKey("slice" + slice);
        List<Document> block = new ArrayList<>();
        for (int i = 0; i < vectors; i++) {
            Document doc = new Document();
            doc.add(SortedDocValuesField.indexedField(SLICE, sliceKey));
            doc.add(vectorField.get());
            doc.add(new StringField("tag", randomFrom("a", "b", "c"), Field.Store.NO));
            if (randomInt(49) == 0) {
                doc.add(new StringField("rare", "yes", Field.Store.NO));
            }
            if (deletes && randomInt(19) == 0) {
                doc.add(new StringField("gone", "yes", Field.Store.NO));
            }
            if (nested == false) {
                writer.addDocument(doc);
                continue;
            }
            block.add(doc);
            if (block.size() == 3 || i == vectors - 1) {
                Document parent = new Document();
                parent.add(SortedDocValuesField.indexedField(SLICE, sliceKey));
                parent.add(new StringField("docType", "_parent", Field.Store.NO));
                block.add(parent);
                writer.addDocuments(block);
                block.clear();
            }
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

    /** Indexes one leaf per given number of random vectors, in that order. */
    private static void indexLeaves(Directory dir, int dims, int... docsPerLeaf) throws IOException {
        IndexWriterConfig config = new IndexWriterConfig().setCodec(TestUtil.alwaysKnnVectorsFormat(new ES950DiskBBQVectorsFormat(64, 4)))
            .setMergePolicy(NoMergePolicy.INSTANCE)
            .setRAMBufferSizeMB(256);
        try (IndexWriter writer = new IndexWriter(dir, config)) {
            for (int docs : docsPerLeaf) {
                for (int i = 0; i < docs; i++) {
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
                writer.flush();
            }
        }
    }

    private static IVFKnnFloatVectorQuery query(float[] vector, int k, Query filter, float visitRatio) {
        return new IVFKnnFloatVectorQuery("v", vector, k, k, filter, visitRatio, resolver());
    }

    private static TestIvfQueryConfigResolver resolver() {
        return new TestIvfQueryConfigResolver(CentroidIndexFormat.FLAT, QuantEncoding.ONE_BIT_4BIT_QUERY, false, 1.0f);
    }

    /** Searches with the ContextIndexSearcher Elasticsearch uses; a null executor runs every task on the caller. */
    private static TopDocs searchWith(DirectoryReader reader, ExecutorService executor, boolean parallelScan, Query query, int k)
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
        searcher.setIvfParallelScanEnabled(parallelScan);
        return searcher.search(query, k);
    }

    /**
     * Checks that a search returns the same hits for the same work with the parallel scan on and off.
     * <p>
     * While the JIT is still compiling the vector code, a search can score the last bits differently from the same search a
     * moment earlier, with the parallel scan or without it (see {@link #warmUp}). So when the two searches differ, the one
     * without the parallel scan runs again: the difference only counts if that search agrees with itself.
     */
    private static void assertParallelScanMatchesPerLeaf(
        DirectoryReader reader,
        ExecutorService executor,
        Supplier<AbstractIVFKnnVectorQuery> query,
        int k
    ) throws IOException {
        for (int attempt = 0; attempt < 5; attempt++) {
            Hits perLeaf = hits(reader, executor, false, query.get(), k);
            Hits parallel = hits(reader, executor, true, query.get(), k);
            if (perLeaf.equals(parallel) || perLeaf.equals(hits(reader, executor, false, query.get(), k))) {
                assertEquals(perLeaf, parallel);
                return;
            }
        }
        fail("the search without a parallel scan never returned the same hits twice in a row");
    }

    /** What a search found, and how many vectors it scored for it. */
    private record Hits(long vectorOps, List<Integer> docs, List<Float> scores) {}

    private static Hits hits(DirectoryReader reader, ExecutorService executor, boolean parallelScan, AbstractIVFKnnVectorQuery query, int k)
        throws IOException {
        TopDocs topDocs = searchWith(reader, executor, parallelScan, query, k);
        List<Integer> docs = new ArrayList<>();
        List<Float> scores = new ArrayList<>();
        for (ScoreDoc hit : topDocs.scoreDocs) {
            docs.add(hit.doc);
            scores.add(hit.score);
        }
        return new Hits(query.totalVectorOps(), docs, scores);
    }

    /** Records the collectors a leaf's search creates, and counts them by whether their search shares a scan with other parts. */
    private static final class RecordingCollectorManager extends IVFCollectorManager {
        final AtomicInteger sharingAScan = new AtomicInteger();
        final AtomicInteger onTheirOwn = new AtomicInteger();
        final List<AbstractMaxScoreKnnCollector> collectors = new CopyOnWriteArrayList<>();

        RecordingCollectorManager(int k) {
            this(k, null);
        }

        RecordingCollectorManager(int k, SharedMinCompetitiveScore sharedScore) {
            super(k, sharedScore);
        }

        @Override
        public AbstractMaxScoreKnnCollector newCollector(int visitedLimit, KnnSearchStrategy strategy, LeafReaderContext ctx)
            throws IOException {
            boolean sharesAScan = strategy instanceof IVFKnnSearchStrategy ivfStrategy && ivfStrategy.getSegmentScan() != null;
            (sharesAScan ? sharingAScan : onTheirOwn).incrementAndGet();
            AbstractMaxScoreKnnCollector collector = super.newCollector(visitedLimit, strategy, ctx);
            collectors.add(collector);
            return collector;
        }
    }

    /** Checks the parts of leaves that can all be split; each leaf's work is a multiple of the least work a part may have. */
    private static void assertParts(long[] workInParts, int parallelism, int... expected) {
        long[] work = Arrays.stream(workInParts).map(parts -> parts * IVFParallelScan.MIN_PART_WORK).toArray();
        boolean[] splittable = new boolean[work.length];
        Arrays.fill(splittable, true);
        assertArrayEquals(expected, IVFParallelScan.partsPerLeaf(work, splittable, parallelism));
    }

    private static int[] ones(int n) {
        int[] ones = new int[n];
        Arrays.fill(ones, 1);
        return ones;
    }
}
