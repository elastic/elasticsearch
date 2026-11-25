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
import org.elasticsearch.search.vectors.AbstractIVFKnnVectorQuery.LeafInput;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

import static org.apache.lucene.index.VectorSimilarityFunction.DOT_PRODUCT;

public class IVFParallelScanTests extends ESTestCase {

    private static final String SLICE = "_slice";
    private static final BitSetProducer PARENTS = new QueryBitSetProducer(new TermQuery(new Term("docType", "_parent")));

    private static ExecutorService executor;
    private static int minVectorsToSplit;

    @BeforeClass
    public static void startThreads() {
        executor = Executors.newFixedThreadPool(4);
        minVectorsToSplit = IVFParallelScan.minVectorsToSplit;
        IVFParallelScan.minVectorsToSplit = 1; // so that the small leaves of these tests can split
    }

    @AfterClass
    public static void stopThreads() {
        terminate(executor);
        executor = null;
        IVFParallelScan.minVectorsToSplit = minVectorsToSplit;
    }

    public void testALeafUnderAMillionVectorsIsNotSplit() throws IOException {
        int dims = randomIntBetween(8, 32);
        try (Directory dir = newDirectory()) {
            indexLeaves(dir, dims, 20_000);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                IVFKnnFloatVectorQuery query = query(randomVector(dims), 10, null, 0.5f);
                assertTrue(splitsALeaf(reader, query));
                assertEquals(1_000_000, minVectorsToSplit);
                IVFParallelScan.minVectorsToSplit = minVectorsToSplit;
                try {
                    assertFalse(splitsALeaf(reader, query));
                } finally {
                    IVFParallelScan.minVectorsToSplit = 1;
                }
            }
        }
    }

    /** Cases are (work per leaf, tasks) -> parts per leaf. Work is in multiples of the least work a part may have. */
    public void testPartsPerLeaf() {
        assertParts(new long[] { 72 }, 8, 8); // a part per task
        assertParts(new long[] { 72 }, 16, 10); // no part under 10% of the work
        assertParts(new long[] { 72 }, 1, 1);
        assertParts(new long[] { 71, 1 }, 8, 8, 1); // 7.9 tasks' worth is rounded up
        assertParts(new long[] { 25, 25, 25, 5 }, 8, 3, 3, 2, 1); // 2.5 tasks' worth each: never more parts than tasks
        assertParts(new long[] { 15, 85 }, 16, 1, 8); // 8.5 tasks' worth: nine parts would each hold under 10%
        assertParts(new long[] { 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10 }, 8, ones(12)); // under a task's worth: whole
        assertParts(new long[] { 40, 20, 10, 5, 2, 1 }, 4, 2, 1, 1, 1, 1, 1);
        assertParts(new long[] { 1 }, 8, 1); // no part under the least work
        assertParts(new long[] { 5 }, 8, 5);
        assertParts(new long[] { 5, 1 }, 8, 5, 1);
        long min = IVFParallelScan.MIN_PART_WORK;
        boolean[] splittable = { true };
        assertArrayEquals(new int[] { 1 }, IVFParallelScan.partsPerLeaf(new long[] { 2 * min - 1 }, splittable, 8));
        assertArrayEquals(new int[] { 2 }, IVFParallelScan.partsPerLeaf(new long[] { 2 * min }, splittable, 8));
        assertArrayEquals(new int[] { 2 }, IVFParallelScan.partsPerLeaf(new long[] { 3 * min - 1 }, splittable, 8));
    }

    public void testRandomLeavesKeepTheRules() {
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

    public void testParallelScanMatchesPerLeafSearch() throws IOException {
        int dims = randomIntBetween(8, 32);
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, randomConfig())) {
            for (int leaf = 0, leaves = randomIntBetween(1, 6); leaf < leaves; leaf++) {
                // the first leaf is big enough to be split
                int docs = leaf == 0 ? randomIntBetween(15_000, 20_000) : randomIntBetween(10, 1_500);
                for (int i = 0; i < docs; i++) {
                    Document doc = vectorDoc(dims);
                    doc.add(new StringField("tag", randomFrom("a", "b", "c"), Field.Store.NO));
                    if (randomInt(99) == 0) {
                        doc.add(new StringField("rare", "yes", Field.Store.NO));
                    }
                    writer.addDocument(doc);
                }
                writer.flush();
            }
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                warmUp(reader, dims);
                for (int q = 0; q < 10; q++) {
                    int k = randomIntBetween(1, 50);
                    float visitRatio = randomFrom(0f, 0.05f, 0.3f, 0.5f);
                    assertParallelScanMatchesPerLeaf(reader, query(randomVector(dims), k, randomFilter(), visitRatio));
                }
            }
        }
    }

    public void testNestedParallelScanMatchesPerLeafSearch() throws IOException {
        int dims = randomIntBetween(8, 32);
        float[] repeated = randomVector(dims);
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, randomConfig())) {
            for (int leaf = 0, leaves = randomIntBetween(1, 4); leaf < leaves; leaf++) {
                int parents = leaf == 0 ? randomIntBetween(3_000, 4_000) : randomIntBetween(5, 300);
                for (int p = 0; p < parents; p++) {
                    List<Document> block = new ArrayList<>();
                    // alike children tie; copies of one vector make parents tie too
                    float[] alike = randomInt(3) == 0 ? null : randomInt(4) == 0 ? repeated : randomVector(dims);
                    for (int c = randomIntBetween(1, 8); c > 0; c--) {
                        Document child = new Document();
                        child.add(new KnnFloatVectorField("v", alike != null ? alike : randomVector(dims), DOT_PRODUCT));
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
                warmUp(reader, dims);
                for (int q = 0; q < 10; q++) {
                    float[] vector = randomBoolean() ? repeated : randomVector(dims);
                    // a small budget, or the rare filter, makes a search go on past its budget
                    float visitRatio = randomFrom(0f, 0.05f, 0.5f, 0.9f);
                    Query filter = randomBoolean() ? null : new TermQuery(new Term("rare", "yes"));
                    int k = randomIntBetween(1, 50);
                    assertParallelScanMatchesPerLeaf(
                        reader,
                        new DiversifyingChildrenIVFKnnFloatVectorQuery("v", vector, k, k, filter, PARENTS, visitRatio, resolver())
                    );
                }
            }
        }
    }

    /** Segments of other formats ignore a shared scan, so each part would search the whole segment: they are not split. */
    public void testSegmentsOfOtherFormatsAreNotSplit() throws IOException {
        int dims = randomIntBetween(8, 32);
        try (Directory dir = newDirectory()) {
            // plain configs, so that each writer leaves one segment
            IndexWriterConfig otherConfig = new IndexWriterConfig().setCodec(TestUtil.getDefaultCodec()).setRAMBufferSizeMB(256);
            for (IndexWriterConfig config : List.of(otherConfig, ivfConfig())) {
                try (IndexWriter writer = new IndexWriter(dir, config)) {
                    for (int i = 0; i < 20_000; i++) {
                        writer.addDocument(vectorDoc(dims));
                    }
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals(2, reader.leaves().size());
                assertParallelScanMatchesPerLeaf(reader, query(randomVector(dims), 10, null, 0.5f));
            }
        }
    }

    /** A filtered search goes on past its budget. On and off follow the same rule, so comparing them cannot show it. */
    public void testAFilteredSearchGoesOnPastItsBudget() throws IOException {
        int dims = randomIntBetween(8, 32);
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, ivfConfig())) {
                for (int i = 0; i < 18_000; i++) {
                    Document doc = vectorDoc(dims);
                    doc.add(new StringField("tag", randomFrom("a", "b", "c", "d"), Field.Store.NO));
                    writer.addDocument(doc);
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                float[] vector = randomVector(dims);
                Query filter = new TermQuery(new Term("tag", "a"));
                float visitRatio = 0.3f; // a budget big enough for the leaf to be split
                for (boolean parallelScan : new boolean[] { false, true }) {
                    long scored = hits(reader, parallelScan, query(vector, 10, null, visitRatio)).vectorOps();
                    long scoredWithFilter = hits(reader, parallelScan, query(vector, 10, filter, visitRatio)).vectorOps();
                    String what = scoredWithFilter + " of " + scored + " docs scored with the filter, parallel scan " + parallelScan;
                    assertTrue(what, scoredWithFilter >= 0.4 * scored);
                }
            }
        }
    }

    /** Only the leaf holding most of the work is split: the others are searched whole, by one collector that shares no scan. */
    public void testOnlySplitLeavesShareAScan() throws IOException {
        int dims = randomIntBetween(8, 32);
        try (Directory dir = newDirectory()) {
            indexLeaves(dir, dims, 20_000, 300, 300);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                List<RecordingCollectorManager> leaves = recordingManagers(reader, null);
                IVFKnnFloatVectorQuery query = query(randomVector(dims), 10, null, 0.5f);
                TopDocs[] hits = parallelScan(reader, query, leaves, 4).search(new TaskExecutor(Runnable::run));
                for (int leaf = 0; leaf < 3; leaf++) {
                    assertTrue("leaf " + leaf + " was searched", hits[leaf].scoreDocs.length > 0);
                    assertEquals("leaf " + leaf + ", on its own", leaf == 0 ? 0 : 1, leaves.get(leaf).onTheirOwn());
                    assertEquals("leaf " + leaf + ", sharing a scan", leaf == 0, leaves.get(leaf).sharingAScan() > 0);
                }
            }
        }
    }

    /** A query type must opt in to splitting: otherwise none of its collectors shares a scan, however big the leaf. */
    public void testAQueryThatCannotSplitSkipsTheParallelScan() throws IOException {
        int dims = randomIntBetween(8, 32);
        try (Directory dir = newDirectory()) {
            indexLeaves(dir, dims, 20_000, 300);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                for (boolean optsIn : new boolean[] { true, false }) {
                    RecordingQuery query = new RecordingQuery(randomVector(dims), optsIn);
                    assertEquals(10, searchWith(reader, true, query).scoreDocs.length);
                    int sharingAScan = query.leaves.get(0).sharingAScan() + query.leaves.get(1).sharingAScan();
                    assertEquals("opts in: " + optsIn, optsIn, sharingAScan > 0);
                }
            }
        }
    }

    /** A part gets the query's transform cache only if its leaf is searched with the query's own vector. */
    public void testPartsGetTheTransformCacheOfTheirLeaf() throws IOException {
        int dims = randomIntBetween(8, 32);
        try (Directory dir = newDirectory()) {
            indexLeaves(dir, dims, 20_000, 300);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                for (boolean copiesQuery : new boolean[] { false, true }) {
                    RecordingQuery query = new RecordingQuery(randomVector(dims), true) {
                        @Override
                        protected float[] segmentQuery(LeafReaderContext context, boolean usePrecondition) throws IOException {
                            float[] segmentQuery = super.segmentQuery(context, usePrecondition);
                            return copiesQuery ? segmentQuery.clone() : segmentQuery;
                        }
                    };
                    assertEquals(10, searchWith(reader, true, query).scoreDocs.length);
                    QueryTransformCache ofTheQuery = query.leaves.get(0).cache;
                    assertNotSame("a query has a cache of its own", QueryTransformCache.NONE, ofTheQuery);
                    List<QueryTransformCache> ofTheParts = query.leaves.get(0).cachesOfParts;
                    assertFalse("the big leaf is split", ofTheParts.isEmpty());
                    QueryTransformCache expected = copiesQuery ? QueryTransformCache.NONE : ofTheQuery;
                    for (QueryTransformCache ofAPart : ofTheParts) {
                        assertSame("copies the query vector: " + copiesQuery, expected, ofAPart);
                    }
                }
            }
        }
    }

    /** The parts publish to the shard's score cutoff: the small leaf, searched last, starts from theirs. */
    public void testPartsShareTheCutoffOfTheirShard() throws IOException {
        int dims = randomIntBetween(8, 32);
        int k = 10;
        try (Directory dir = newDirectory()) {
            indexLeaves(dir, dims, 20_000, k / 2);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                SharedMinCompetitiveScore cutoff = new SharedMinCompetitiveScore();
                List<RecordingCollectorManager> leaves = recordingManagers(reader, cutoff);
                parallelScan(reader, query(randomVector(dims), k, null, 0.5f), leaves, 4).search(new TaskExecutor(Runnable::run));

                assertTrue("the big leaf was split", leaves.get(0).sharingAScan() > 0);
                assertTrue("a part published its cutoff", cutoff.get() > AbstractMaxScoreKnnCollector.LEAST_COMPETITIVE);
                assertEquals(1, leaves.get(1).collectors.size());
                assertEquals("the small leaf started from it", cutoff.get(), leaves.get(1).collectors.get(0).getMinCompetitiveDocScore());
            }
        }
    }

    public void testAFloatQueryForOneSliceMatchesPerLeafSearch() throws IOException {
        int dims = randomIntBetween(8, 32);
        assertOneSliceMatchesPerLeafSearch(() -> floatField(dims), false, (k, filter, ratio, id) -> {
            float[] vector = randomVector(dims);
            return new IVFKnnFloatSlicedVectorQuery("v", vector, k, k, filter, ratio, resolver(), SLICE, id);
        });
    }

    public void testAByteQueryForOneSliceMatchesPerLeafSearch() throws IOException {
        int dims = randomIntBetween(8, 32);
        assertOneSliceMatchesPerLeafSearch(() -> byteField(dims), false, (k, filter, ratio, id) -> {
            byte[] vector = randomBytes(dims);
            return new IVFKnnByteSlicedVectorQuery("v", vector, k, k, filter, ratio, resolver(), SLICE, id);
        });
    }

    public void testANestedFloatQueryForOneSliceMatchesPerLeafSearch() throws IOException {
        int dims = randomIntBetween(8, 32);
        assertOneSliceMatchesPerLeafSearch(() -> floatField(dims), true, (k, filter, ratio, id) -> {
            float[] vector = randomVector(dims);
            return new DiversifyingChildrenIVFKnnFloatSlicedVectorQuery("v", vector, k, k, filter, PARENTS, ratio, resolver(), SLICE, id);
        });
    }

    public void testANestedByteQueryForOneSliceMatchesPerLeafSearch() throws IOException {
        int dims = randomIntBetween(8, 32);
        assertOneSliceMatchesPerLeafSearch(() -> byteField(dims), true, (k, filter, ratio, id) -> {
            byte[] vector = randomBytes(dims);
            return new DiversifyingChildrenIVFKnnByteSlicedVectorQuery("v", vector, k, k, filter, PARENTS, ratio, resolver(), SLICE, id);
        });
    }

    /** Makes a query for one slice, on a new random vector. */
    @FunctionalInterface
    private interface OneSliceQuery {
        AbstractIVFKnnVectorQuery of(int k, Query filter, float visitRatio, BytesRef slice);
    }

    private static void assertOneSliceMatchesPerLeafSearch(Supplier<Field> vectorField, boolean nested, OneSliceQuery queries)
        throws IOException {
        try (Directory dir = newDirectory()) {
            // slice 0 is most of the first leaf; the last leaf has none of it
            int[][] vectorsPerSlice = { { 18_000, 600, 400 }, { 300, 500 }, { 0, 200, 300 } };
            indexSlicedLeaves(dir, vectorField, nested, randomBoolean(), vectorsPerSlice);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                BytesRef slice0 = new BytesRef("slice0");
                // posting lists are per slice: a small search of slice 0 visits few of its vectors
                AbstractIVFKnnVectorQuery smallSearch = queries.of(10, null, 0.05f, slice0);
                searchWith(reader, false, smallSearch);
                assertTrue(smallSearch.totalVectorOps() + " visited", smallSearch.totalVectorOps() < vectorsPerSlice[0][0] / 2);
                // and a big one splits the first leaf
                assertTrue(splitsALeaf(reader, queries.of(10, null, 0.5f, slice0)));

                for (int q = 0; q < 50; q++) {
                    BytesRef slice = new BytesRef("slice" + (randomBoolean() ? 0 : randomIntBetween(1, 2)));
                    float visitRatio = randomFrom(0f, 0.05f, 0.5f, 0.5f);
                    AbstractIVFKnnVectorQuery query = queries.of(randomIntBetween(1, 50), randomFilter(), visitRatio, slice);
                    if (q < 30) {
                        searchWith(reader, randomBoolean(), query); // warms up, see warmUp
                    } else {
                        assertParallelScanMatchesPerLeaf(reader, query);
                    }
                }
            }
        }
    }

    /** Several slices are several scans of a leaf, so only a query for one slice can split. */
    public void testOnlyAQueryForOneSliceCanSplit() {
        float[] floats = randomVector(8);
        byte[] bytes = randomBytes(8);
        for (int slices = 0; slices <= 3; slices++) { // a query that names no slice searches them all
            BytesRef[] ids = new BytesRef[slices];
            Arrays.setAll(ids, slice -> new BytesRef("slice" + slice));
            boolean one = slices == 1;
            assertEquals(one, new IVFKnnFloatSlicedVectorQuery("v", floats, 10, 10, null, 0.5f, resolver(), SLICE, ids).canSplitLeaves());
            assertEquals(one, new IVFKnnByteSlicedVectorQuery("v", bytes, 10, 10, null, 0.5f, resolver(), SLICE, ids).canSplitLeaves());
            assertEquals(
                one,
                new DiversifyingChildrenIVFKnnFloatSlicedVectorQuery("v", floats, 10, 10, null, PARENTS, 0.5f, resolver(), SLICE, ids)
                    .canSplitLeaves()
            );
            assertEquals(
                one,
                new DiversifyingChildrenIVFKnnByteSlicedVectorQuery("v", bytes, 10, 10, null, PARENTS, 0.5f, resolver(), SLICE, ids)
                    .canSplitLeaves()
            );
        }
    }

    /** A leaf's work for a one-slice query is the slice's, read off the skipper: within one skipper interval of the truth. */
    public void testALeafIsSizedByTheSliceTheQuerySearches() throws IOException {
        int dims = randomIntBetween(8, 32);
        int[] vectorsPerSlice = { 15_000, 2_000, 300, 5_000 };
        int skipperInterval = 4096; // docs per interval of Lucene's doc values skipper
        try (Directory dir = newDirectory()) {
            indexSlicedLeaves(dir, () -> floatField(dims), false, false, vectorsPerSlice);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReaderContext leaf = reader.leaves().get(0);
                int leafVectors = leaf.reader().getFloatVectorValues("v").size();
                assertEquals(Arrays.stream(vectorsPerSlice).sum(), leafVectors);
                for (int slice = 0; slice < vectorsPerSlice.length; slice++) {
                    BytesRef id = new BytesRef("slice" + slice);
                    var query = new IVFKnnFloatSlicedVectorQuery("v", randomVector(dims), 10, 10, null, 0.5f, resolver(), SLICE, id);
                    int searched = query.searchedVectors(leaf, leafVectors);
                    assertTrue(searched + " for slice " + slice, Math.abs(searched - vectorsPerSlice[slice]) <= skipperInterval);
                    boolean split = slice == 0; // only slice 0 has two parts' worth of work
                    assertEquals("slice " + slice, split, splitsALeaf(reader, query));
                }
            }
        }
    }

    /** One leaf per entry, with that many vectors per slice. Only the first, a merged one, keeps posting lists per slice. */
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
                        int inThisFlush = flush == 0 ? vectors / flushes : vectors - vectors / flushes;
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

    /** With {@code nested}, up to three vectors share a parent. With {@code deletes}, about one doc in twenty is deleted. */
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

    private static void indexLeaves(Directory dir, int dims, int... docsPerLeaf) throws IOException {
        try (IndexWriter writer = new IndexWriter(dir, ivfConfig())) {
            for (int docs : docsPerLeaf) {
                for (int i = 0; i < docs; i++) {
                    writer.addDocument(vectorDoc(dims));
                }
                writer.flush();
            }
        }
    }

    /** Every flush stays a leaf. */
    private static IndexWriterConfig ivfConfig() {
        return new IndexWriterConfig().setCodec(TestUtil.alwaysKnnVectorsFormat(new ES950DiskBBQVectorsFormat(64, 4)))
            .setMergePolicy(NoMergePolicy.INSTANCE)
            .setRAMBufferSizeMB(256);
    }

    private static IndexWriterConfig randomConfig() {
        return newIndexWriterConfig().setCodec(TestUtil.alwaysKnnVectorsFormat(new ES950DiskBBQVectorsFormat(64, randomIntBetween(2, 8))))
            .setMergePolicy(NoMergePolicy.INSTANCE)
            .setMaxBufferedDocs(50_000)
            .setRAMBufferSizeMB(256);
    }

    private static Document vectorDoc(int dims) {
        Document doc = new Document();
        doc.add(floatField(dims));
        return doc;
    }

    private static Field floatField(int dims) {
        return new KnnFloatVectorField("v", randomVector(dims), DOT_PRODUCT);
    }

    private static Field byteField(int dims) {
        return new KnnByteVectorField("v", randomBytes(dims), DOT_PRODUCT);
    }

    private static float[] randomVector(int dims) {
        return VectorTestUtils.randomNormalizedFloatVector(dims);
    }

    private static byte[] randomBytes(int dims) {
        return VectorTestUtils.randomByteVector(dims);
    }

    /** No filter, one that keeps a third of the docs, or one that keeps few. */
    private static Query randomFilter() {
        return randomFrom(Arrays.asList(null, new TermQuery(new Term("tag", "a")), new TermQuery(new Term("rare", "yes"))));
    }

    private static IVFKnnFloatVectorQuery query(float[] vector, int k, Query filter, float visitRatio) {
        return new IVFKnnFloatVectorQuery("v", vector, k, k, filter, visitRatio, resolver());
    }

    private static TestIvfQueryConfigResolver resolver() {
        return new TestIvfQueryConfigResolver(CentroidIndexFormat.FLAT, QuantEncoding.ONE_BIT_4BIT_QUERY, false, 1.0f);
    }

    /** Searches with the ContextIndexSearcher Elasticsearch uses. */
    private static TopDocs searchWith(DirectoryReader reader, boolean parallelScan, AbstractIVFKnnVectorQuery query) throws IOException {
        ContextIndexSearcher searcher = new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            randomBoolean(),
            executor,
            4,
            1
        );
        searcher.setIvfParallelScanEnabled(parallelScan);
        return searcher.search(query, query.k);
    }

    /** A parallel scan at a visit ratio of 0.5, each leaf with the collector manager at its position. */
    private static IVFParallelScan parallelScan(
        DirectoryReader reader,
        AbstractIVFKnnVectorQuery query,
        List<? extends IVFCollectorManager> managers,
        int maxTasks
    ) throws IOException {
        List<LeafInput> inputs = new ArrayList<>();
        for (LeafReaderContext leaf : reader.leaves()) {
            inputs.add(new LeafInput(leaf, managers.get(leaf.ord), false));
        }
        return new IVFParallelScan(query, null, 0.5f, inputs, maxTasks);
    }

    private static boolean splitsALeaf(DirectoryReader reader, AbstractIVFKnnVectorQuery query) throws IOException {
        IVFCollectorManager manager = new IVFCollectorManager(10, null, QueryTransformCache.NONE);
        return parallelScan(reader, query, reader.leaves().stream().map(leaf -> manager).toList(), 4).splitsALeaf();
    }

    private static List<RecordingCollectorManager> recordingManagers(DirectoryReader reader, SharedMinCompetitiveScore cutoff) {
        List<RecordingCollectorManager> managers = new ArrayList<>();
        for (int leaf = 0; leaf < reader.leaves().size(); leaf++) {
            managers.add(new RecordingCollectorManager(10, cutoff, QueryTransformCache.NONE));
        }
        return managers;
    }

    /** Compiled and interpreted code can round a float sum differently, so search before comparing results bit for bit. */
    private static void warmUp(DirectoryReader reader, int dims) throws IOException {
        for (int q = 0; q < 30; q++) {
            searchWith(reader, randomBoolean(), query(randomVector(dims), 10, null, randomFrom(0f, 0.05f, 0.5f)));
        }
    }

    /** A difference only counts if the search without the parallel scan, run again, agrees with itself: see {@link #warmUp}. */
    private static void assertParallelScanMatchesPerLeaf(DirectoryReader reader, AbstractIVFKnnVectorQuery query) throws IOException {
        for (int attempt = 0; attempt < 5; attempt++) {
            Hits perLeaf = hits(reader, false, query);
            Hits parallel = hits(reader, true, query);
            if (perLeaf.equals(parallel) || perLeaf.equals(hits(reader, false, query))) {
                assertEquals(perLeaf, parallel);
                return;
            }
        }
        fail("the search without a parallel scan never returned the same hits twice in a row");
    }

    private record Hits(long vectorOps, List<Integer> docs, List<Float> scores) {}

    private static Hits hits(DirectoryReader reader, boolean parallelScan, AbstractIVFKnnVectorQuery query) throws IOException {
        List<Integer> docs = new ArrayList<>();
        List<Float> scores = new ArrayList<>();
        for (ScoreDoc hit : searchWith(reader, parallelScan, query).scoreDocs) {
            docs.add(hit.doc);
            scores.add(hit.score);
        }
        return new Hits(query.totalVectorOps(), docs, scores);
    }

    /** Records the collector manager of each leaf. As a subclass, it splits its leaves only if it opts in. */
    private static class RecordingQuery extends IVFKnnFloatVectorQuery {
        final List<RecordingCollectorManager> leaves = new ArrayList<>();
        private final boolean optsIn;

        RecordingQuery(float[] vector, boolean optsIn) {
            super("v", vector, 10, 10, null, 0.5f, resolver());
            this.optsIn = optsIn;
        }

        @Override
        boolean canSplitLeaves() {
            return optsIn || super.canSplitLeaves();
        }

        @Override
        protected IVFCollectorManager getKnnCollectorManager(int k, SharedMinCompetitiveScore sharedScore, QueryTransformCache cache) {
            RecordingCollectorManager leaf = new RecordingCollectorManager(k, sharedScore, cache);
            leaves.add(leaf);
            return leaf;
        }
    }

    /** Records the collectors a leaf's search creates, and the caches of those that share a scan. */
    private static final class RecordingCollectorManager extends IVFCollectorManager {
        final QueryTransformCache cache; // the leaf's own
        final List<AbstractMaxScoreKnnCollector> collectors = new CopyOnWriteArrayList<>();
        final List<QueryTransformCache> cachesOfParts = new CopyOnWriteArrayList<>();

        RecordingCollectorManager(int k, SharedMinCompetitiveScore sharedScore, QueryTransformCache cache) {
            super(k, sharedScore, cache);
            this.cache = cache;
        }

        @Override
        public AbstractMaxScoreKnnCollector newCollector(int visitedLimit, KnnSearchStrategy strategy, LeafReaderContext ctx)
            throws IOException {
            if (strategy instanceof IVFKnnSearchStrategy ivfStrategy && ivfStrategy.getSegmentScan() != null) {
                cachesOfParts.add(ivfStrategy.getQueryTransformCache());
            }
            AbstractMaxScoreKnnCollector collector = super.newCollector(visitedLimit, strategy, ctx);
            collectors.add(collector);
            return collector;
        }

        int sharingAScan() {
            return cachesOfParts.size();
        }

        int onTheirOwn() {
            return collectors.size() - cachesOfParts.size();
        }
    }

    /** Every leaf can be split. */
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
