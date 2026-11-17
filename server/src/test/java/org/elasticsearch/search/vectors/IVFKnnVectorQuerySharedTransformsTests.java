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
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.index.codec.vectors.VectorTestUtils;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidIndexFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfSegmentConfig;
import org.elasticsearch.index.codec.vectors.diskbbq.QuantEncoding;
import org.elasticsearch.index.codec.vectors.diskbbq.TestIvfQueryConfigResolver;
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskASHTestFixture;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.hamcrest.Matchers.greaterThan;

/**
 * Sharing of query transformations across the leaves of an IVF query, and the named slices a
 * {@link IVFKnnFloatSlicedVectorQuery} searches within a leaf, end to end on ASH segments that share their projection
 * matrix, with the leaves searched concurrently.
 * <p>
 * The queries here leave out the cutoff shared across leaves, so that what each leaf prunes does not depend on timing and
 * the results of the queries with and without shared transformations can be compared exactly.
 */
public class IVFKnnVectorQuerySharedTransformsTests extends ESTestCase {

    static {
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    private static final String FIELD = "f";
    private static final String SLICE_FIELD = AbstractIVFKnnSlicedVectorQueryTestCase.SLICE_FIELD;
    private static final int DIMS = 128;
    private static final int SLICES = 8;

    private IndexWriterConfig writerConfig(int queryBitsPerDim, boolean sliced) {
        IndexWriterConfig iwc = new IndexWriterConfig();
        iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(ESNextDiskASHTestFixture.ashFormat(queryBitsPerDim, sliced ? SLICE_FIELD : null)));
        if (sliced) {
            iwc.setIndexSort(AbstractIVFKnnSlicedVectorQueryTestCase.sliceIndexSort());
        }
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        return iwc;
    }

    private static void addDocs(IndexWriter w, boolean sliced) throws IOException {
        int docs = randomIntBetween(600, 1000);
        for (int i = 0; i < docs; i++) {
            Document doc = new Document();
            if (sliced) {
                AbstractIVFKnnSlicedVectorQueryTestCase.addSliceFields(doc, SLICE_FIELD, sliceId(randomIntBetween(0, SLICES - 1)));
            }
            doc.add(new KnnFloatVectorField(FIELD, VectorTestUtils.randomFloatVector(DIMS), VectorSimilarityFunction.EUCLIDEAN));
            w.addDocument(doc);
        }
    }

    private static String sliceId(int slice) {
        return "slice" + slice;
    }

    /**
     * An index whose segments are copies of one segment, so they share its projection matrix, plus one with its own: two
     * distinct projection matrices in all.
     */
    private Directory sharedProjectionIndex(Directory source, boolean sliced) throws IOException {
        int queryBitsPerDim = randomFrom(0, IvfSegmentConfig.AshConfig.DEFAULT_QUERY_BITS_PER_DIM);
        boolean merged = sliced && randomBoolean();
        IndexWriterConfig sourceConfig = writerConfig(queryBitsPerDim, sliced);
        if (merged) {
            // only merge when forced
            LogDocMergePolicy mergePolicy = new LogDocMergePolicy();
            mergePolicy.setMergeFactor(100);
            sourceConfig.setMergePolicy(mergePolicy);
        }
        try (IndexWriter w = new IndexWriter(source, sourceConfig)) {
            addDocs(w, sliced);
            if (merged) {
                // a merged sliced segment has centroids per named slice, rather than a single flat posting list
                w.commit();
                addDocs(w, true);
                w.forceMerge(1);
            }
        }
        Directory dir = newDirectory();
        try (IndexWriter w = new IndexWriter(dir, writerConfig(queryBitsPerDim, sliced))) {
            int copies = randomIntBetween(2, 6);
            for (int c = 0; c < copies; c++) {
                w.addIndexes(source);
            }
            addDocs(w, sliced);
        }
        return dir;
    }

    private static TestIvfQueryConfigResolver resolver() {
        return new TestIvfQueryConfigResolver(CentroidIndexFormat.FLAT, QuantEncoding.ONE_BIT_4BIT_QUERY, false, 1.0f);
    }

    public void testConcurrentLeavesShareProjections() throws Exception {
        doTestShareProjections(false);
    }

    public void testSlicesShareProjections() throws Exception {
        doTestShareProjections(true);
    }

    private void doTestShareProjections(boolean sliced) throws Exception {
        try (
            Directory source = newDirectory();
            Directory dir = sharedProjectionIndex(source, sliced);
            DirectoryReader reader = DirectoryReader.open(dir);
            ExecutorService executor = Executors.newFixedThreadPool(randomIntBetween(2, 8))
        ) {
            assertThat(reader.leaves().size(), greaterThan(2));
            IndexSearcher searcher = new IndexSearcher(reader, executor);
            int k = randomIntBetween(1, 20);
            for (int q = 0; q < 5; q++) {
                float[] query = VectorTestUtils.randomFloatVector(DIMS);
                BytesRef[] slices = randomSliceKeys();
                List<SharedQueryTransforms> used = Collections.synchronizedList(new ArrayList<>());
                TopDocs shared = searcher.search(query(query, k, sliced, slices, used, true), k);
                TopDocs unshared = searcher.search(query(query, k, sliced, slices, null, false), k);

                assertThat("the comparison needs hits", shared.scoreDocs.length, greaterThan(0));
                assertEquals(unshared.scoreDocs.length, shared.scoreDocs.length);
                for (int i = 0; i < shared.scoreDocs.length; i++) {
                    assertEquals(unshared.scoreDocs[i].doc, shared.scoreDocs[i].doc);
                    assertEquals(unshared.scoreDocs[i].score, shared.scoreDocs[i].score, 0f);
                }
                // every segment has documents in every requested named slice, so every segment is searched
                assertEquals("one search strategy per segment", reader.leaves().size(), used.size());
                for (SharedQueryTransforms transforms : used) {
                    assertSame("every leaf searches with the same shared transforms", used.get(0), transforms);
                }
                // the copies share one projection, and the other segment has its own, however many slices each searches
                assertEquals(2, Objects.requireNonNull(used.get(0)).size());
            }
        }
    }

    private static BytesRef[] randomSliceKeys() {
        // several named slices of every segment, so that each segment is searched once per named slice
        int count = randomIntBetween(2, SLICES);
        BytesRef[] keys = new BytesRef[count];
        for (int i = 0; i < count; i++) {
            keys[i] = new BytesRef(sliceId(i));
        }
        return keys;
    }

    public void testLeafWithAnotherVectorDoesNotShare() throws Exception {
        boolean sliced = randomBoolean();
        try (
            Directory source = newDirectory();
            Directory dir = sharedProjectionIndex(source, sliced);
            DirectoryReader reader = DirectoryReader.open(dir)
        ) {
            float[] query = VectorTestUtils.randomFloatVector(DIMS);
            List<SharedQueryTransforms> used = Collections.synchronizedList(new ArrayList<>());
            Query copying = sliced ? new SlicedQuery(query, 10, used, true, randomSliceKeys()) {
                @Override
                protected float[] segmentQuery(LeafReaderContext context, boolean usePrecondition) {
                    // as a preconditioned segment does
                    return query.clone();
                }
            } : new LeafQuery(query, 10, used, true) {
                @Override
                protected float[] segmentQuery(LeafReaderContext context, boolean usePrecondition) {
                    return query.clone();
                }
            };
            newSearcher(reader).search(copying, 10);
            assertFalse(used.isEmpty());
            for (SharedQueryTransforms transforms : used) {
                assertNull("a leaf searching with another vector gets no shared transforms", transforms);
            }
        }
    }

    private static Query query(float[] query, int k, boolean sliced, BytesRef[] slices, List<SharedQueryTransforms> used, boolean share) {
        return sliced ? new SlicedQuery(query, k, used, share, slices) : new LeafQuery(query, k, used, share);
    }

    /**
     * A collector manager without the shared cutoff, and with the given transforms, that records the transforms each
     * leaf's search strategy carries if {@code used} is not null.
     */
    private static AbstractIVFKnnVectorQuery.IVFCollectorManager collectorManager(
        int k,
        SharedQueryTransforms sharedQueryTransforms,
        List<SharedQueryTransforms> used
    ) {
        return new AbstractIVFKnnVectorQuery.IVFCollectorManager(k, null, sharedQueryTransforms) {
            @Override
            public AbstractMaxScoreKnnCollector newCollector(int visitedLimit, KnnSearchStrategy searchStrategy, LeafReaderContext ctx)
                throws IOException {
                if (used != null) {
                    used.add(((IVFKnnSearchStrategy) searchStrategy).getSharedQueryTransforms());
                }
                return super.newCollector(visitedLimit, searchStrategy, ctx);
            }
        };
    }

    private static class LeafQuery extends IVFKnnFloatVectorQuery {
        private final List<SharedQueryTransforms> used;
        private final boolean share;

        LeafQuery(float[] query, int k, List<SharedQueryTransforms> used, boolean share) {
            super(FIELD, query, k, k, null, 0.1f, resolver());
            this.used = used;
            this.share = share;
        }

        @Override
        protected IVFCollectorManager getKnnCollectorManager(
            int k,
            SharedMinCompetitiveScore sharedScore,
            SharedQueryTransforms sharedQueryTransforms
        ) {
            return collectorManager(k, share ? sharedQueryTransforms : null, used);
        }
    }

    private static class SlicedQuery extends IVFKnnFloatSlicedVectorQuery {
        private final List<SharedQueryTransforms> used;
        private final boolean share;

        SlicedQuery(float[] query, int k, List<SharedQueryTransforms> used, boolean share, BytesRef... slices) {
            super(FIELD, query, k, k, null, 0.1f, resolver(), SLICE_FIELD, slices);
            this.used = used;
            this.share = share;
        }

        @Override
        protected IVFCollectorManager getKnnCollectorManager(
            int k,
            SharedMinCompetitiveScore sharedScore,
            SharedQueryTransforms sharedQueryTransforms
        ) {
            return collectorManager(k, share ? sharedQueryTransforms : null, used);
        }
    }
}
