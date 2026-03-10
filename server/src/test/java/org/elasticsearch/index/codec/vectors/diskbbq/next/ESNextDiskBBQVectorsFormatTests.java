/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.codec.vectors.diskbbq.next;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidIterator;
import org.elasticsearch.index.codec.vectors.diskbbq.ES920DiskBBQVectorsFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.PrefetchingCentroidIterator;
import org.elasticsearch.index.codec.vectors.es93.DirectIOCapableLucene99FlatVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93BFloat16FlatVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93FlatVectorScorer;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.search.vectors.IVFKnnSearchStrategy;
import org.junit.Before;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;
import static org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat.DEFAULT_PRECONDITIONING_BLOCK_DIMENSION;
import static org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat.MAX_CENTROIDS_PER_PARENT_CLUSTER;
import static org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat.MAX_PRECONDITIONING_BLOCK_DIMS;
import static org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat.MAX_VECTORS_PER_CLUSTER;
import static org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat.MIN_CENTROIDS_PER_PARENT_CLUSTER;
import static org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat.MIN_PRECONDITIONING_BLOCK_DIMS;
import static org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat.MIN_VECTORS_PER_CLUSTER;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

public class ESNextDiskBBQVectorsFormatTests extends BaseKnnVectorsFormatTestCase {

    static {
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }
    KnnVectorsFormat format;

    @Before
    @Override
    public void setUp() throws Exception {
        ESNextDiskBBQVectorsFormat.QuantEncoding encoding = ESNextDiskBBQVectorsFormat.QuantEncoding.values()[random().nextInt(
            ESNextDiskBBQVectorsFormat.QuantEncoding.values().length
        )];
        boolean disableFlatOnFlush = random().nextBoolean();
        if (rarely()) {
            int vectorPerCluster = random().nextInt(2 * MIN_VECTORS_PER_CLUSTER, MAX_VECTORS_PER_CLUSTER);
            int flatVectorThreshold = disableFlatOnFlush ? 0 : ESNextDiskBBQVectorsFormat.defaultFlatThreshold(vectorPerCluster);
            format = new ESNextDiskBBQVectorsFormat(
                encoding,
                vectorPerCluster,
                random().nextInt(8, MAX_CENTROIDS_PER_PARENT_CLUSTER),
                DenseVectorFieldMapper.ElementType.FLOAT,
                false,
                null,
                1,
                false,
                DEFAULT_PRECONDITIONING_BLOCK_DIMENSION,
                flatVectorThreshold
            );
        } else if (rarely()) {
            int vectorPerCluster = random().nextInt(MIN_VECTORS_PER_CLUSTER, MAX_VECTORS_PER_CLUSTER);
            int flatVectorThreshold = disableFlatOnFlush ? 0 : ESNextDiskBBQVectorsFormat.defaultFlatThreshold(vectorPerCluster);
            format = new ESNextDiskBBQVectorsFormat(
                encoding,
                vectorPerCluster,
                random().nextInt(MIN_CENTROIDS_PER_PARENT_CLUSTER, MAX_CENTROIDS_PER_PARENT_CLUSTER),
                DenseVectorFieldMapper.ElementType.FLOAT,
                false,
                null,
                1,
                true,
                random().nextInt(MIN_PRECONDITIONING_BLOCK_DIMS, MAX_PRECONDITIONING_BLOCK_DIMS),
                flatVectorThreshold
            );
        } else {
            // run with low numbers to force many clusters with parents
            int vectorPerCluster = random().nextInt(MIN_VECTORS_PER_CLUSTER, 2 * MIN_VECTORS_PER_CLUSTER);
            int flatVectorThreshold = disableFlatOnFlush ? 0 : ESNextDiskBBQVectorsFormat.defaultFlatThreshold(vectorPerCluster);
            format = new ESNextDiskBBQVectorsFormat(
                encoding,
                vectorPerCluster,
                random().nextInt(MIN_CENTROIDS_PER_PARENT_CLUSTER, 8),
                DenseVectorFieldMapper.ElementType.FLOAT,
                false,
                null,
                1,
                false,
                DEFAULT_PRECONDITIONING_BLOCK_DIMENSION,
                flatVectorThreshold
            );
        }
        super.setUp();
    }

    @Override
    protected VectorSimilarityFunction randomSimilarity() {
        return RandomPicks.randomFrom(
            random(),
            List.of(
                VectorSimilarityFunction.DOT_PRODUCT,
                VectorSimilarityFunction.EUCLIDEAN,
                VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT
            )
        );
    }

    @Override
    protected VectorEncoding randomVectorEncoding() {
        return VectorEncoding.FLOAT32;
    }

    @Override
    public void testSearchWithVisitedLimit() {
        // ivf doesn't enforce visitation limit
    }

    @Override
    protected Codec getCodec() {
        return TestUtil.alwaysKnnVectorsFormat(format);
    }

    @Override
    protected void assertOffHeapByteSize(LeafReader r, String fieldName) throws IOException {
        var fieldInfo = r.getFieldInfos().fieldInfo(fieldName);

        if (r instanceof CodecReader codecReader) {
            KnnVectorsReader knnVectorsReader = codecReader.getVectorReader();
            if (knnVectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader fieldsReader) {
                knnVectorsReader = fieldsReader.getFieldReader(fieldName);
            }
            var offHeap = knnVectorsReader.getOffHeapByteSize(fieldInfo);
            long totalByteSize = offHeap.values().stream().mapToLong(Long::longValue).sum();
            assertThat(offHeap.size(), equalTo(3));
            assertThat(totalByteSize, equalTo(offHeap.values().stream().mapToLong(Long::longValue).sum()));
        } else {
            throw new AssertionError("unexpected:" + r.getClass());
        }
    }

    @Override
    public void testAdvance() throws Exception {
        // TODO re-enable with hierarchical IVF, clustering as it is is flaky
    }

    public void testToString() {
        FilterCodec customCodec = new FilterCodec("foo", Codec.getDefault()) {
            @Override
            public KnnVectorsFormat knnVectorsFormat() {
                return new ESNextDiskBBQVectorsFormat(128, 4);
            }
        };
        String expectedPattern = "ESNextDiskBBQVectorsFormat(vectorPerCluster=128, mergeExec=false)";

        var defaultScorer = format(Locale.ROOT, expectedPattern, "DefaultFlatVectorScorer");
        var memSegScorer = format(Locale.ROOT, expectedPattern, "Lucene99MemorySegmentFlatVectorsScorer");
        assertThat(customCodec.knnVectorsFormat().toString(), is(oneOf(defaultScorer, memSegScorer)));
    }

    public void testLimits() {
        expectThrows(IllegalArgumentException.class, () -> new ESNextDiskBBQVectorsFormat(MIN_VECTORS_PER_CLUSTER - 1, 16));
        expectThrows(IllegalArgumentException.class, () -> new ESNextDiskBBQVectorsFormat(MAX_VECTORS_PER_CLUSTER + 1, 16));
        expectThrows(IllegalArgumentException.class, () -> new ESNextDiskBBQVectorsFormat(128, MIN_CENTROIDS_PER_PARENT_CLUSTER - 1));
        expectThrows(IllegalArgumentException.class, () -> new ESNextDiskBBQVectorsFormat(128, MAX_CENTROIDS_PER_PARENT_CLUSTER + 1));
    }

    public void testSimpleOffHeapSize() throws IOException {
        float[] vector = randomVector(random().nextInt(12, 500));
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
            Document doc = new Document();
            doc.add(new KnnFloatVectorField("f", vector, VectorSimilarityFunction.EUCLIDEAN));
            w.addDocument(doc);
            w.commit();
            try (IndexReader reader = DirectoryReader.open(w)) {
                LeafReader r = getOnlyLeafReader(reader);
                if (r instanceof CodecReader codecReader) {
                    KnnVectorsReader knnVectorsReader = codecReader.getVectorReader();
                    if (knnVectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader fieldsReader) {
                        knnVectorsReader = fieldsReader.getFieldReader("f");
                    }
                    var fieldInfo = r.getFieldInfos().fieldInfo("f");
                    var offHeap = knnVectorsReader.getOffHeapByteSize(fieldInfo);
                    assertEquals(3, offHeap.size());
                }
            }
        }
    }

    public void testFewVectorManyTimes() throws IOException {
        int numDifferentVectors = random().nextInt(1, 20);
        float[][] vectors = new float[numDifferentVectors][];
        int dimensions = random().nextInt(12, 500);
        for (int i = 0; i < numDifferentVectors; i++) {
            vectors[i] = randomVector(dimensions);
        }
        int numDocs = random().nextInt(100, 10_000);
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
            for (int i = 0; i < numDocs; i++) {
                float[] vector = vectors[random().nextInt(numDifferentVectors)];
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("f", vector, VectorSimilarityFunction.EUCLIDEAN));
                w.addDocument(doc);
            }
            w.commit();
            if (rarely()) {
                w.forceMerge(1);
            }
            try (IndexReader reader = DirectoryReader.open(w)) {
                List<LeafReaderContext> subReaders = reader.leaves();
                for (LeafReaderContext r : subReaders) {
                    LeafReader leafReader = r.reader();
                    float[] vector = randomVector(dimensions);
                    TopDocs topDocs = leafReader.searchNearestVectors(
                        "f",
                        vector,
                        10,
                        AcceptDocs.fromLiveDocs(leafReader.getLiveDocs(), leafReader.maxDoc()),
                        Integer.MAX_VALUE
                    );
                    assertEquals(Math.min(leafReader.maxDoc(), 10), topDocs.scoreDocs.length);
                }

            }
        }
    }

    public void testOneRepeatedVector() throws IOException {
        int dimensions = random().nextInt(12, 500);
        float[] repeatedVector = randomVector(dimensions);
        int numDocs = random().nextInt(100, 10_000);
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
            for (int i = 0; i < numDocs; i++) {
                float[] vector = random().nextInt(3) == 0 ? repeatedVector : randomVector(dimensions);
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("f", vector, VectorSimilarityFunction.EUCLIDEAN));
                w.addDocument(doc);
            }
            w.commit();
            if (rarely()) {
                w.forceMerge(1);
            }
            try (IndexReader reader = DirectoryReader.open(w)) {
                List<LeafReaderContext> subReaders = reader.leaves();
                for (LeafReaderContext r : subReaders) {
                    LeafReader leafReader = r.reader();
                    float[] vector = randomVector(dimensions);
                    TopDocs topDocs = leafReader.searchNearestVectors(
                        "f",
                        vector,
                        10,
                        AcceptDocs.fromLiveDocs(leafReader.getLiveDocs(), leafReader.maxDoc()),
                        Integer.MAX_VALUE
                    );
                    assertEquals(Math.min(leafReader.maxDoc(), 10), topDocs.scoreDocs.length);
                }

            }
        }
    }

    // this is a modified version of lucene's TestSearchWithThreads test case
    public void testWithThreads() throws Exception {
        final int numThreads = random().nextInt(2, 5);
        final int numSearches = atLeast(100);
        final int numDocs = atLeast(1000);
        final int dimensions = random().nextInt(12, 500);
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
            for (int docCount = 0; docCount < numDocs; docCount++) {
                final Document doc = new Document();
                doc.add(new KnnFloatVectorField("f", randomVector(dimensions), VectorSimilarityFunction.EUCLIDEAN));
                w.addDocument(doc);
            }
            w.forceMerge(1);
            try (IndexReader reader = DirectoryReader.open(w)) {
                final AtomicBoolean failed = new AtomicBoolean();
                Thread[] threads = new Thread[numThreads];
                for (int threadID = 0; threadID < numThreads; threadID++) {
                    threads[threadID] = new Thread(() -> {
                        try {
                            long totSearch = 0;
                            for (; totSearch < numSearches && failed.get() == false; totSearch++) {
                                float[] vector = randomVector(dimensions);
                                LeafReader leafReader = getOnlyLeafReader(reader);
                                leafReader.searchNearestVectors(
                                    "f",
                                    vector,
                                    10,
                                    AcceptDocs.fromLiveDocs(leafReader.getLiveDocs(), leafReader.maxDoc()),
                                    Integer.MAX_VALUE
                                );
                            }
                            assertTrue(totSearch > 0);
                        } catch (Exception exc) {
                            failed.set(true);
                            throw new RuntimeException(exc);
                        }
                    });
                    threads[threadID].setDaemon(true);
                }

                for (Thread t : threads) {
                    t.start();
                }

                for (Thread t : threads) {
                    t.join();
                }
            }
        }
    }

    public void testRestrictiveFilterDense() throws IOException {
        doRestrictiveFilter(true);
    }

    public void testRestrictiveFilterSparse() throws IOException {
        doRestrictiveFilter(false);
    }

    private void doRestrictiveFilter(boolean dense) throws IOException {
        int dimensions = random().nextInt(12, 500);
        int maxMatchingDocs = random().nextInt(1, 10);
        int matchingDocs = 0;
        int numDocs = random().nextInt(100, 3_000);
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                if (dense || rarely() == false) {
                    float[] vector = randomVector(dimensions);
                    doc.add(new KnnFloatVectorField("f", vector, VectorSimilarityFunction.EUCLIDEAN));
                }
                doc.add(new KeywordField("k", new BytesRef("B"), Field.Store.NO));
                w.addDocument(doc);
                if (matchingDocs < maxMatchingDocs && rarely()) {
                    matchingDocs++;
                    doc = new Document();
                    doc.add(new KnnFloatVectorField("f", randomVector(dimensions), VectorSimilarityFunction.EUCLIDEAN));
                    doc.add(new KeywordField("k", new BytesRef("A"), Field.Store.NO));
                    w.addDocument(doc);
                }
                if (dense == false && rarely()) {
                    doc = new Document();
                    doc.add(new KeywordField("k", new BytesRef("A"), Field.Store.NO));
                    w.addDocument(doc);
                }
            }
            if (matchingDocs == 0) {
                // make sure we have at least one matching doc with a vector
                matchingDocs++;
                float[] vector = randomVector(dimensions);
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("f", vector, VectorSimilarityFunction.EUCLIDEAN));
                doc.add(new KeywordField("k", new BytesRef("A"), Field.Store.NO));
                w.addDocument(doc);
            }
            w.commit();
            // force one leave
            w.forceMerge(1);
            try (IndexReader reader = DirectoryReader.open(w)) {
                LeafReader leafReader = getOnlyLeafReader(reader);
                float[] vector = randomVector(dimensions);
                // we might collect the same document twice because of soar assignments
                KnnCollector collector;
                if (random().nextBoolean()) {
                    collector = new TopKnnCollector(random().nextInt(2 * matchingDocs, 3 * matchingDocs), Integer.MAX_VALUE);
                } else {
                    collector = new TopKnnCollector(
                        random().nextInt(2 * matchingDocs, 3 * matchingDocs),
                        Integer.MAX_VALUE,
                        new IVFKnnSearchStrategy(0.25f, null)
                    );
                }
                leafReader.searchNearestVectors(
                    "f",
                    vector,
                    collector,
                    AcceptDocs.fromIteratorSupplier(
                        () -> leafReader.postings(new Term("k", new BytesRef("A"))),
                        leafReader.getLiveDocs(),
                        leafReader.maxDoc()
                    )
                );
                TopDocs topDocs = collector.topDocs();
                Set<Integer> uniqueDocIds = new HashSet<>();
                for (int i = 0; i < topDocs.scoreDocs.length; i++) {
                    uniqueDocIds.add(topDocs.scoreDocs[i].doc);
                }
                assertEquals(matchingDocs, uniqueDocIds.size());
                // match no docs
                leafReader.searchNearestVectors(
                    "f",
                    vector,
                    new TopKnnCollector(2, Integer.MAX_VALUE),
                    AcceptDocs.fromIteratorSupplier(DocIdSetIterator::empty, leafReader.getLiveDocs(), leafReader.maxDoc())
                );
            }
        }
    }

    public void testDifferentPrefetchDepthsProduceSameResults() throws IOException {
        int numDocs = 500;
        int dimensions = random().nextInt(12, 64);
        int k = 10;
        long seed = random().nextLong();

        float[] queryVector = randomVector(dimensions);
        int[] depths = { 1, 2, 4, 8 };
        TopDocs[] allResults = new TopDocs[depths.length];

        for (int d = 0; d < depths.length; d++) {
            KnnVectorsFormat depthFormat = new PrefetchDepthOverrideFormat(depths[d]);
            try (Directory dir = newDirectory()) {
                IndexWriterConfig iwc = newIndexWriterConfig();
                iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(depthFormat));
                iwc.setMaxBufferedDocs(numDocs + 1);
                iwc.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);

                Random rng = new Random(seed);
                try (IndexWriter w = new IndexWriter(dir, iwc)) {
                    for (int i = 0; i < numDocs; i++) {
                        Document doc = new Document();
                        float[] vec = new float[dimensions];
                        for (int v = 0; v < dimensions; v++) {
                            vec[v] = rng.nextFloat() - 0.5f;
                        }
                        VectorUtil.l2normalize(vec);
                        doc.add(new KnnFloatVectorField("field", vec, VectorSimilarityFunction.DOT_PRODUCT));
                        w.addDocument(doc);
                    }
                    w.forceMerge(1);
                }

                try (IndexReader reader = DirectoryReader.open(dir)) {
                    LeafReader leaf = getOnlyLeafReader(reader);
                    allResults[d] = leaf.searchNearestVectors(
                        "field",
                        queryVector,
                        k,
                        AcceptDocs.fromLiveDocs(leaf.getLiveDocs(), leaf.maxDoc()),
                        Integer.MAX_VALUE
                    );
                }
            }
        }

        TopDocs baseline = allResults[0];
        for (int d = 1; d < depths.length; d++) {
            assertEquals(
                "depth=" + depths[d] + " returned different count than depth=1",
                baseline.scoreDocs.length,
                allResults[d].scoreDocs.length
            );
            for (int j = 0; j < baseline.scoreDocs.length; j++) {
                assertEquals(
                    "doc mismatch at position " + j + " with depth=" + depths[d],
                    baseline.scoreDocs[j].doc,
                    allResults[d].scoreDocs[j].doc
                );
                assertEquals(
                    "score mismatch at position " + j + " with depth=" + depths[d],
                    baseline.scoreDocs[j].score,
                    allResults[d].scoreDocs[j].score,
                    1e-4f
                );
            }
        }
    }

    public void testDifferentPrefetchDepthsWithFilter() throws IOException {
        int numDocs = 500;
        int dimensions = random().nextInt(12, 64);
        int k = 10;
        long seed = random().nextLong();

        float[] queryVector = randomVector(dimensions);
        int[] depths = { 1, 4, 8 };
        TopDocs[] allResults = new TopDocs[depths.length];

        for (int d = 0; d < depths.length; d++) {
            KnnVectorsFormat depthFormat = new PrefetchDepthOverrideFormat(depths[d]);
            try (Directory dir = newDirectory()) {
                IndexWriterConfig iwc = newIndexWriterConfig();
                iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(depthFormat));
                iwc.setMaxBufferedDocs(numDocs + 1);
                iwc.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);

                Random rng = new Random(seed);
                try (IndexWriter w = new IndexWriter(dir, iwc)) {
                    for (int i = 0; i < numDocs; i++) {
                        Document doc = new Document();
                        float[] vec = new float[dimensions];
                        for (int v = 0; v < dimensions; v++) {
                            vec[v] = rng.nextFloat() - 0.5f;
                        }
                        VectorUtil.l2normalize(vec);
                        doc.add(new KnnFloatVectorField("field", vec, VectorSimilarityFunction.DOT_PRODUCT));
                        w.addDocument(doc);
                    }
                    w.forceMerge(1);
                }

                try (IndexReader reader = DirectoryReader.open(dir)) {
                    LeafReader leaf = getOnlyLeafReader(reader);
                    Bits evenDocs = new Bits() {
                        @Override
                        public boolean get(int index) {
                            return index % 2 == 0;
                        }

                        @Override
                        public int length() {
                            return leaf.maxDoc();
                        }
                    };
                    allResults[d] = leaf.searchNearestVectors(
                        "field",
                        queryVector,
                        k,
                        AcceptDocs.fromLiveDocs(evenDocs, leaf.maxDoc()),
                        Integer.MAX_VALUE
                    );
                }
            }
        }

        TopDocs baseline = allResults[0];
        for (int d = 1; d < depths.length; d++) {
            assertEquals(
                "depth=" + depths[d] + " returned different count than depth=1 (filtered)",
                baseline.scoreDocs.length,
                allResults[d].scoreDocs.length
            );
            for (int j = 0; j < baseline.scoreDocs.length; j++) {
                assertEquals(
                    "doc mismatch at position " + j + " with depth=" + depths[d] + " (filtered)",
                    baseline.scoreDocs[j].doc,
                    allResults[d].scoreDocs[j].doc
                );
                assertEquals(
                    "score mismatch at position " + j + " with depth=" + depths[d] + " (filtered)",
                    baseline.scoreDocs[j].score,
                    allResults[d].scoreDocs[j].score,
                    1e-4f
                );
            }
        }
    }

    /**
     * An {@link ES920DiskBBQVectorsFormat} subclass that produces a reader with a configurable
     * prefetch depth, allowing tests to verify that different depths yield identical results.
     */
    private static class PrefetchDepthOverrideFormat extends ESNextDiskBBQVectorsFormat {
        private final int prefetchDepth;

        PrefetchDepthOverrideFormat(int prefetchDepth) {
            super(128, 4);
            this.prefetchDepth = prefetchDepth;
        }

        @Override
        public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
            var float32Fmt = new DirectIOCapableLucene99FlatVectorsFormat(ES93FlatVectorScorer.INSTANCE);
            var bfloat16Fmt = new ES93BFloat16FlatVectorsFormat(FlatVectorScorerUtil.getLucene99FlatVectorsScorer());
            var formats = Map.of(float32Fmt.getName(), float32Fmt, bfloat16Fmt.getName(), bfloat16Fmt);
            return new ESNextDiskBBQVectorsReader(state, (f, dio) -> {
                var fmt = formats.get(f);
                if (fmt == null) return null;
                return fmt.fieldsReader(state, dio);
            }) {
                @Override
                public CentroidIterator getPostingListPrefetchIterator(CentroidIterator centroidIterator, IndexInput postingListSlice)
                    throws IOException {
                    return new PrefetchingCentroidIterator(centroidIterator, postingListSlice, prefetchDepth);
                }
            };
        }
    }

}
