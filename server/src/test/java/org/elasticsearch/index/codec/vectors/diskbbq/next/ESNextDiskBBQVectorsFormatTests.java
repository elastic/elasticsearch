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
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.search.vectors.IVFKnnSearchStrategy;
import org.junit.Before;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;
import static org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat.MAX_CENTROIDS_PER_PARENT_CLUSTER;
import static org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat.MAX_VECTORS_PER_CLUSTER;
import static org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat.MIN_CENTROIDS_PER_PARENT_CLUSTER;
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
        if (rarely()) {
            format = new ESNextDiskBBQVectorsFormat(
                encoding,
                random().nextInt(2 * MIN_VECTORS_PER_CLUSTER, ESNextDiskBBQVectorsFormat.MAX_VECTORS_PER_CLUSTER),
                random().nextInt(8, ESNextDiskBBQVectorsFormat.MAX_CENTROIDS_PER_PARENT_CLUSTER)
            );
        } else {
            // run with low numbers to force many clusters with parents
            format = new ESNextDiskBBQVectorsFormat(
                encoding,
                random().nextInt(MIN_VECTORS_PER_CLUSTER, 2 * MIN_VECTORS_PER_CLUSTER),
                random().nextInt(MIN_CENTROIDS_PER_PARENT_CLUSTER, 8)
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
        String expectedPattern = "ESNextDiskBBQVectorsFormat(vectorPerCluster=128)";

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
}
