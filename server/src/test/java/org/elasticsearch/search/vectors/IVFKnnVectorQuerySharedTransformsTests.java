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
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.index.codec.vectors.VectorTestUtils;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidIndexFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.QuantEncoding;
import org.elasticsearch.index.codec.vectors.diskbbq.TestIvfQueryConfigResolver;
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskASHTestFixture;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * End to end sharing of query transformations on ASH segments that share their projection matrix: across the leaves of a
 * query, searched concurrently, and the named slices a {@link IVFKnnFloatSlicedVectorQuery} searches within a leaf.
 * {@link AbstractIVFKnnVectorQueryTestCase} covers how the query hands the transformations to its leaves; this covers that
 * the ASH format shares its projections through them, without changing results.
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

    private static IndexWriterConfig writerConfig(boolean sliced) {
        IndexWriterConfig iwc = new IndexWriterConfig();
        // clustering every named slice, however few vectors it has, so that a merged segment has centroids per named slice
        iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(ESNextDiskASHTestFixture.ashFormat(sliced ? SLICE_FIELD : null, sliced ? 0 : -1)));
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
    private static Directory sharedProjectionIndex(Directory source, boolean sliced) throws IOException {
        boolean merged = sliced && randomBoolean();
        IndexWriterConfig sourceConfig = writerConfig(sliced);
        if (merged) {
            // only merge when forced
            LogDocMergePolicy mergePolicy = new LogDocMergePolicy();
            mergePolicy.setMergeFactor(100);
            sourceConfig.setMergePolicy(mergePolicy);
        }
        try (IndexWriter w = new IndexWriter(source, sourceConfig)) {
            addDocs(w, sliced);
            if (merged) {
                w.commit();
                addDocs(w, true);
                w.forceMerge(1);
            }
        }
        if (merged) {
            try (DirectoryReader reader = DirectoryReader.open(source)) {
                assertThat(
                    "a merged segment has centroids per named slice",
                    ESNextDiskASHTestFixture.numSlices(reader.leaves().get(0).reader(), FIELD),
                    greaterThan(0)
                );
            }
        }
        Directory dir = newDirectory();
        try (IndexWriter w = new IndexWriter(dir, writerConfig(sliced))) {
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

    public void testNamedSlicesShareProjections() throws Exception {
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
                BytesRef[] slices = randomSliceIds();
                CountingSharedQueryTransforms transforms = new CountingSharedQueryTransforms();
                TopDocs shared = searcher.search(query(query, k, sliced, slices, transforms), k);
                TopDocs unshared = searcher.search(query(query, k, sliced, slices, SharedQueryTransforms.NONE), k);

                assertThat("the comparison needs hits", shared.scoreDocs.length, greaterThan(0));
                assertThat(shared.scoreDocs.length, equalTo(unshared.scoreDocs.length));
                for (int i = 0; i < shared.scoreDocs.length; i++) {
                    assertThat(shared.scoreDocs[i].doc, equalTo(unshared.scoreDocs[i].doc));
                    assertThat(shared.scoreDocs[i].score, equalTo(unshared.scoreDocs[i].score));
                }
                // the copies share one projection, and the other segment has its own, however many named slices each searches
                assertThat(transforms.computed(), equalTo(2));
            }
        }
    }

    private static BytesRef[] randomSliceIds() {
        // several named slices of every segment, so that each segment is searched once per named slice
        int count = randomIntBetween(2, SLICES);
        BytesRef[] ids = new BytesRef[count];
        for (int i = 0; i < count; i++) {
            ids[i] = new BytesRef(sliceId(i));
        }
        return ids;
    }

    public void testLeafWithAnotherVectorDoesNotShare() throws Exception {
        boolean sliced = randomBoolean();
        try (
            Directory source = newDirectory();
            Directory dir = sharedProjectionIndex(source, sliced);
            DirectoryReader reader = DirectoryReader.open(dir)
        ) {
            float[] query = VectorTestUtils.randomFloatVector(DIMS);
            CountingSharedQueryTransforms transforms = new CountingSharedQueryTransforms();
            // every leaf searches with a copy of the query vector, as a preconditioned segment does with its own vector
            Query copying = sliced ? new SlicedQuery(query, 10, transforms, randomSliceIds()) {
                @Override
                protected float[] segmentQuery(LeafReaderContext context, boolean usePrecondition) {
                    return query.clone();
                }
            } : new LeafQuery(query, 10, transforms) {
                @Override
                protected float[] segmentQuery(LeafReaderContext context, boolean usePrecondition) {
                    return query.clone();
                }
            };
            TopDocs topDocs = newSearcher(reader).search(copying, 10);
            assertThat(topDocs.scoreDocs.length, greaterThan(0));
            assertThat("a leaf searching with another vector does not use the shared transforms", transforms.computed(), equalTo(0));
        }
    }

    private static Query query(float[] query, int k, boolean sliced, BytesRef[] slices, SharedQueryTransforms transforms) {
        return sliced ? new SlicedQuery(query, k, transforms, slices) : new LeafQuery(query, k, transforms);
    }

    /** Searches with the given transforms in place of the query's own, and without the cutoff shared across leaves. */
    private static class LeafQuery extends IVFKnnFloatVectorQuery {
        private final SharedQueryTransforms transforms;

        LeafQuery(float[] query, int k, SharedQueryTransforms transforms) {
            super(FIELD, query, k, k, null, 0.1f, resolver());
            this.transforms = transforms;
        }

        @Override
        protected IVFCollectorManager getKnnCollectorManager(
            int k,
            SharedMinCompetitiveScore sharedScore,
            SharedQueryTransforms sharedQueryTransforms
        ) {
            return new IVFCollectorManager(k, null, transforms);
        }
    }

    /** As {@link LeafQuery}, for the given named slices. */
    private static class SlicedQuery extends IVFKnnFloatSlicedVectorQuery {
        private final SharedQueryTransforms transforms;

        SlicedQuery(float[] query, int k, SharedQueryTransforms transforms, BytesRef... slices) {
            super(FIELD, query, k, k, null, 0.1f, resolver(), SLICE_FIELD, slices);
            this.transforms = transforms;
        }

        @Override
        protected IVFCollectorManager getKnnCollectorManager(
            int k,
            SharedMinCompetitiveScore sharedScore,
            SharedQueryTransforms sharedQueryTransforms
        ) {
            return new IVFCollectorManager(k, null, transforms);
        }
    }
}
