/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.next;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.codec.vectors.ESBaseKnnVectorsFormatTestCase;
import org.elasticsearch.index.codec.vectors.VectorTestUtils;
import org.elasticsearch.index.codec.vectors.ash.AshProjectionMatrix;
import org.elasticsearch.index.codec.vectors.diskbbq.AshConfigResolver;
import org.elasticsearch.index.codec.vectors.diskbbq.IVFVectorsReader;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfFlushConfigSource;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfMergeConfigResolver;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfSegmentConfig;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.search.vectors.ESAcceptDocs;
import org.elasticsearch.search.vectors.ESAcceptDocs.SliceAcceptDocs;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import static org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskASHVectorsFormat.MAX_CENTROIDS_PER_PARENT_CLUSTER;
import static org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskASHVectorsFormat.MAX_VECTORS_PER_CLUSTER;
import static org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskASHVectorsFormat.MIN_CENTROIDS_PER_PARENT_CLUSTER;
import static org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskASHVectorsFormat.MIN_VECTORS_PER_CLUSTER;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Tests for {@link ESNextDiskASHVectorsFormat}.
 * Extends the standard Lucene vector format compliance test suite.
 */
public class ESNextDiskASHVectorsFormatTests extends ESBaseKnnVectorsFormatTestCase {

    KnnVectorsFormat format;

    @Override
    protected boolean supportsFloatVectorFallback() {
        return false;
    }

    @Override
    protected VectorSimilarityFunction randomSimilarity() {
        return randomFrom(
            VectorSimilarityFunction.DOT_PRODUCT,
            VectorSimilarityFunction.EUCLIDEAN,
            VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT
        );
    }

    @Override
    protected VectorEncoding randomVectorEncoding() {
        return VectorEncoding.FLOAT32;
    }

    @Override
    public void testSearchWithVisitedLimit() {
        // IVF doesn't enforce visitation limit
    }

    @Override
    public void testAdvance() {
        // TODO re-enable with hierarchical IVF, clustering as it is is flaky
    }

    @Override
    protected Codec getCodec() {
        if (format == null) {
            if (random().nextInt(4) == 0) {
                // the configuration the mapping uses by default, which mostly means 1 bit and no reduction at these sizes
                format = ashDimsDerivedFormat();
            } else if (rarely()) {
                int vectorPerCluster = random().nextInt(2 * MIN_VECTORS_PER_CLUSTER, MAX_VECTORS_PER_CLUSTER);
                format = new ESNextDiskASHVectorsFormat(
                    vectorPerCluster,
                    random().nextInt(MIN_CENTROIDS_PER_PARENT_CLUSTER, MAX_CENTROIDS_PER_PARENT_CLUSTER),
                    null
                );
            } else {
                // run with low numbers to force many clusters with parents
                int vectorPerCluster = random().nextInt(MIN_VECTORS_PER_CLUSTER, 2 * MIN_VECTORS_PER_CLUSTER);
                format = new ESNextDiskASHVectorsFormat(
                    vectorPerCluster,
                    random().nextInt(MIN_CENTROIDS_PER_PARENT_CLUSTER, MAX_CENTROIDS_PER_PARENT_CLUSTER),
                    null
                );
            }
        }
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
            assertThat(offHeap, aMapWithSize(3));
            assertThat(totalByteSize, greaterThanOrEqualTo(0L));
        } else {
            throw new AssertionError("unexpected:" + r.getClass());
        }
    }

    public void testAshIndexAndSearch() throws IOException {
        int dimensions = 64;
        int numDocs = 200;
        Codec ashCodec = TestUtil.alwaysKnnVectorsFormat(ashTestFormat());
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = newIndexWriterConfig();
            iwc.setCodec(ashCodec);
            try (IndexWriter w = new IndexWriter(dir, iwc)) {
                for (int i = 0; i < numDocs; i++) {
                    Document doc = new Document();
                    doc.add(new KnnFloatVectorField("f", randomVector(dimensions), VectorSimilarityFunction.DOT_PRODUCT));
                    w.addDocument(doc);
                }
                w.forceMerge(1);
                try (IndexReader reader = DirectoryReader.open(w)) {
                    for (LeafReaderContext ctx : reader.leaves()) {
                        LeafReader leafReader = ctx.reader();
                        float[] query = randomVector(dimensions);
                        TopDocs topDocs = leafReader.searchNearestVectors(
                            "f",
                            query,
                            10,
                            AcceptDocs.fromLiveDocs(leafReader.getLiveDocs(), leafReader.maxDoc()),
                            Integer.MAX_VALUE
                        );
                        assertThat(topDocs.scoreDocs, arrayWithSize(Math.min(leafReader.maxDoc(), 10)));
                        for (int i = 0; i < topDocs.scoreDocs.length - 1; i++) {
                            assertThat(
                                "Scores should be descending",
                                topDocs.scoreDocs[i].score,
                                greaterThanOrEqualTo(topDocs.scoreDocs[i + 1].score)
                            );
                        }
                    }
                }
            }
        }
    }

    public void testAshAllSimilarityFunctions() throws IOException {
        int dimensions = 64;
        int numDocs = 200;
        Codec ashCodec = TestUtil.alwaysKnnVectorsFormat(ashTestFormat());
        for (VectorSimilarityFunction sim : new VectorSimilarityFunction[] {
            VectorSimilarityFunction.DOT_PRODUCT,
            VectorSimilarityFunction.EUCLIDEAN,
            VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT }) {
            try (Directory dir = newDirectory()) {
                IndexWriterConfig iwc = newIndexWriterConfig();
                iwc.setCodec(ashCodec);
                try (IndexWriter w = new IndexWriter(dir, iwc)) {
                    for (int i = 0; i < numDocs; i++) {
                        Document doc = new Document();
                        doc.add(new KnnFloatVectorField("f", randomVector(dimensions), sim));
                        w.addDocument(doc);
                    }
                    w.forceMerge(1);
                    try (IndexReader reader = DirectoryReader.open(w)) {
                        for (LeafReaderContext ctx : reader.leaves()) {
                            LeafReader leafReader = ctx.reader();
                            float[] query = randomVector(dimensions);
                            TopDocs topDocs = leafReader.searchNearestVectors(
                                "f",
                                query,
                                10,
                                AcceptDocs.fromLiveDocs(leafReader.getLiveDocs(), leafReader.maxDoc()),
                                Integer.MAX_VALUE
                            );
                            assertThat("similarity=" + sim, topDocs.scoreDocs, arrayWithSize(Math.min(leafReader.maxDoc(), 10)));
                            for (int i = 0; i < topDocs.scoreDocs.length - 1; i++) {
                                assertThat(
                                    "Scores should be descending for " + sim,
                                    topDocs.scoreDocs[i].score,
                                    greaterThanOrEqualTo(topDocs.scoreDocs[i + 1].score)
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * With a dims-derived configuration the writer picks the bits and projected dimensions per field from the
     * dimension it writes, on flush and on merge, and the reader scores with whatever the segment recorded.
     */
    public void testDimsDerivedConfigFollowsFieldDimension() throws IOException {
        Codec ashCodec = TestUtil.alwaysKnnVectorsFormat(ashDimsDerivedFormat());
        int[] fieldDims = { random().nextInt(64, 512), random().nextInt(512, 2049), 2049 };
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = newIndexWriterConfig();
            iwc.setCodec(ashCodec);
            try (IndexWriter w = new IndexWriter(dir, iwc)) {
                int numDocs = random().nextInt(100, 200);
                for (int i = 0; i < numDocs; i++) {
                    Document doc = new Document();
                    for (int f = 0; f < fieldDims.length; f++) {
                        doc.add(new KnnFloatVectorField("f" + f, randomVector(fieldDims[f]), VectorSimilarityFunction.DOT_PRODUCT));
                    }
                    w.addDocument(doc);
                    if (rarely()) {
                        w.commit();
                    }
                }
                w.commit();
                assertDimsDerivedSegments(w, fieldDims);
                w.forceMerge(1);
                assertDimsDerivedSegments(w, fieldDims);
            }
        }
    }

    private void assertDimsDerivedSegments(IndexWriter w, int[] fieldDims) throws IOException {
        try (IndexReader reader = DirectoryReader.open(w)) {
            for (LeafReaderContext ctx : reader.leaves()) {
                LeafReader leafReader = ctx.reader();
                for (int f = 0; f < fieldDims.length; f++) {
                    String field = "f" + f;
                    int dims = fieldDims[f];
                    var expected = IvfSegmentConfig.AshConfig.forDims(dims, null, IvfSegmentConfig.AshConfig.DEFAULT_QUERY_BITS_PER_DIM);
                    ESNextDiskASHVectorsReader ashReader = ashReader(leafReader, field);
                    FieldInfo fieldInfo = leafReader.getFieldInfos().fieldInfo(field);
                    assertEquals("bits for dims [" + dims + "]", expected.bitsPerDim(), ashReader.getBitsPerDimForTest(fieldInfo));
                    assertEquals(
                        "projected dims for dims [" + dims + "]",
                        expected.projectedDims(dims),
                        ashReader.getProjectionMatrix(fieldInfo).nDims()
                    );
                    TopDocs topDocs = leafReader.searchNearestVectors(
                        field,
                        randomVector(dims),
                        10,
                        AcceptDocs.fromLiveDocs(leafReader.getLiveDocs(), leafReader.maxDoc()),
                        Integer.MAX_VALUE
                    );
                    assertThat(topDocs.scoreDocs, arrayWithSize(Math.min(leafReader.maxDoc(), 10)));
                }
            }
        }
    }

    /**
     * Changing the bits of a field mixes segments written with different bits but the same projection shape. Each
     * segment is read with the bits it recorded, and merging them writes the bits currently configured.
     */
    public void testMixedBitsSegments() throws IOException {
        int dims = random().nextInt(512, 1024);
        int oldBits = 2;
        // not 8: scoring needs at least as many query bits as document bits, and the query bits are 4
        int newBits = randomFrom(1, 3, 4);
        try (Directory dir = newDirectory()) {
            int docsPerWriter = random().nextInt(50, 100);
            for (int bits : new int[] { oldBits, newBits }) {
                IndexWriterConfig iwc = newIndexWriterConfig();
                iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(ashDimsDerivedFormat(bits)));
                iwc.setMergePolicy(NoMergePolicy.INSTANCE);
                try (IndexWriter w = new IndexWriter(dir, iwc)) {
                    for (int i = 0; i < docsPerWriter; i++) {
                        Document doc = new Document();
                        doc.add(new KnnFloatVectorField("f", randomVector(dims), VectorSimilarityFunction.DOT_PRODUCT));
                        w.addDocument(doc);
                    }
                    w.commit();
                }
            }

            try (IndexReader reader = DirectoryReader.open(dir)) {
                Set<Integer> bitsSeen = new HashSet<>();
                for (LeafReaderContext ctx : reader.leaves()) {
                    LeafReader leafReader = ctx.reader();
                    FieldInfo fieldInfo = leafReader.getFieldInfos().fieldInfo("f");
                    ESNextDiskASHVectorsReader ashReader = ashReader(leafReader, "f");
                    bitsSeen.add(ashReader.getBitsPerDimForTest(fieldInfo));
                    assertEquals(
                        IvfSegmentConfig.AshConfig.forDims(dims, null, 4).projectedDims(dims),
                        ashReader.getProjectionMatrix(fieldInfo).nDims()
                    );
                    TopDocs topDocs = leafReader.searchNearestVectors(
                        "f",
                        randomVector(dims),
                        10,
                        AcceptDocs.fromLiveDocs(leafReader.getLiveDocs(), leafReader.maxDoc()),
                        Integer.MAX_VALUE
                    );
                    assertThat(topDocs.scoreDocs, arrayWithSize(Math.min(leafReader.maxDoc(), 10)));
                }
                assertEquals(Set.of(oldBits, newBits), bitsSeen);
            }

            IndexWriterConfig iwc = newIndexWriterConfig();
            iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(ashDimsDerivedFormat(newBits)));
            try (IndexWriter w = new IndexWriter(dir, iwc)) {
                w.forceMerge(1);
                try (IndexReader reader = DirectoryReader.open(w)) {
                    LeafReader leafReader = getOnlyLeafReader(reader);
                    FieldInfo fieldInfo = leafReader.getFieldInfos().fieldInfo("f");
                    assertEquals(newBits, ashReader(leafReader, "f").getBitsPerDimForTest(fieldInfo));
                    TopDocs topDocs = leafReader.searchNearestVectors(
                        "f",
                        randomVector(dims),
                        10,
                        AcceptDocs.fromLiveDocs(leafReader.getLiveDocs(), leafReader.maxDoc()),
                        Integer.MAX_VALUE
                    );
                    assertThat(topDocs.scoreDocs, arrayWithSize(10));
                }
            }
        }
    }

    /**
     * Each segment's projection matrix records an id of its own and the size of the sample it was learned from, which
     * survive reading the segment back.
     */
    public void testProjectionMatrixRecordsIdentity() throws IOException {
        int dims = 64;
        // the configuration of ashTestFormat; segments are flushed, so they sample with the flush training factor
        IvfSegmentConfig.AshConfig config = IvfSegmentConfig.AshConfig.defaults();
        int nDims = config.projectedDims(dims);
        int maxSample = config.flushTrainingFactor() * dims;
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = newIndexWriterConfig();
            iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(ashTestFormat()));
            iwc.setMergePolicy(NoMergePolicy.INSTANCE);
            // one segment per commit
            iwc.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
            iwc.setRAMBufferSizeMB(256);
            // at least twice as many vectors as projected dimensions, so that each projection is learned
            int[] docsPerSegment = new int[] { randomIntBetween(2 * nDims, maxSample - 1), randomIntBetween(maxSample, 2 * maxSample) };
            try (IndexWriter w = new IndexWriter(dir, iwc)) {
                for (int docs : docsPerSegment) {
                    for (int i = 0; i < docs; i++) {
                        Document doc = new Document();
                        doc.add(new KnnFloatVectorField("f", randomVector(dims), VectorSimilarityFunction.DOT_PRODUCT));
                        w.addDocument(doc);
                    }
                    w.commit();
                }
            }
            try (IndexReader reader = DirectoryReader.open(dir)) {
                assertThat(reader.leaves(), hasSize(docsPerSegment.length));
                Set<UUID> ids = new HashSet<>();
                for (LeafReaderContext ctx : reader.leaves()) {
                    LeafReader leafReader = ctx.reader();
                    FieldInfo fieldInfo = leafReader.getFieldInfos().fieldInfo("f");
                    AshProjectionMatrix matrix = ashReader(leafReader, "f").getProjectionMatrix(fieldInfo);
                    assertTrue(matrix.isLearned());
                    assertEquals(Math.min(maxSample, leafReader.maxDoc()), matrix.trainingSampleSize());
                    ids.add(matrix.id());
                }
                assertEquals("every segment learned its own projection", docsPerSegment.length, ids.size());
            }
        }
    }

    /**
     * A merge looks up each input segment's projection matrix with that segment's own field number, which can differ from
     * the merged segment's: here {@code addIndexes} merges a segment that numbers the field differently. A warm start with
     * too few vectors returns the inherited matrix unchanged, so the merged segment shows which matrix it inherited.
     */
    public void testMergeInheritsByFieldNameAcrossFieldNumbers() throws IOException {
        int dims = 64;
        int nDims = IvfSegmentConfig.AshConfig.defaults().projectedDims(dims);
        int docs = 2 * nDims + 10;
        try (Directory source = newDirectory(); Directory target = newDirectory()) {
            IndexWriterConfig sourceConfig = newIndexWriterConfig();
            sourceConfig.setCodec(TestUtil.alwaysKnnVectorsFormat(ashTestFormat()));
            sourceConfig.setMergePolicy(NoMergePolicy.INSTANCE);
            // one segment
            sourceConfig.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
            sourceConfig.setRAMBufferSizeMB(256);
            try (IndexWriter w = new IndexWriter(source, sourceConfig)) {
                for (int i = 0; i < docs; i++) {
                    Document doc = new Document();
                    // another ASH field of the same shape comes first, so it takes the field number the target gives "f": looked
                    // up by the merged segment's field number, its learned matrix would be inherited instead of "f"'s
                    doc.add(new KnnFloatVectorField("a", VectorTestUtils.randomFloatVector(dims), VectorSimilarityFunction.EUCLIDEAN));
                    doc.add(new KnnFloatVectorField("f", VectorTestUtils.randomFloatVector(dims), VectorSimilarityFunction.EUCLIDEAN));
                    doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
                    w.addDocument(doc);
                }
                w.commit();
                // fewer live vectors than the merge needs to refine the inherited matrix, so it inherits it unchanged
                for (int i = 0; i < docs - (2 * nDims - 1); i++) {
                    w.deleteDocuments(new Term("id", Integer.toString(i)));
                }
                w.commit();
            }
            IndexWriterConfig targetConfig = newIndexWriterConfig();
            targetConfig.setCodec(TestUtil.alwaysKnnVectorsFormat(ashTestFormat()));
            targetConfig.setMergePolicy(NoMergePolicy.INSTANCE);
            try (DirectoryReader sourceReader = DirectoryReader.open(source); IndexWriter w = new IndexWriter(target, targetConfig)) {
                assertThat(sourceReader.leaves(), hasSize(1));
                LeafReader sourceLeaf = sourceReader.leaves().get(0).reader();
                FieldInfo sourceField = sourceLeaf.getFieldInfos().fieldInfo("f");
                AshProjectionMatrix sourceMatrix = ashReader(sourceLeaf, "f").getProjectionMatrix(sourceField);
                assertThat(sourceMatrix.isLearned(), equalTo(true));

                Document doc = new Document();
                doc.add(new KnnFloatVectorField("f", VectorTestUtils.randomFloatVector(dims), VectorSimilarityFunction.EUCLIDEAN));
                w.addDocument(doc);
                w.commit();
                w.addIndexes((CodecReader) sourceLeaf);
                w.commit();
                try (DirectoryReader reader = DirectoryReader.open(w)) {
                    LeafReader merged = reader.leaves()
                        .stream()
                        .map(LeafReaderContext::reader)
                        .filter(r -> r.maxDoc() > 1)
                        .findFirst()
                        .orElseThrow();
                    FieldInfo mergedField = merged.getFieldInfos().fieldInfo("f");
                    assertThat("the field is numbered differently", mergedField.number, not(equalTo(sourceField.number)));
                    assertThat(
                        "the source numbers another ASH field as the target numbers this one",
                        sourceLeaf.getFieldInfos().fieldInfo(mergedField.number).name,
                        equalTo("a")
                    );
                    AshProjectionMatrix mergedMatrix = ashReader(merged, "f").getProjectionMatrix(mergedField);
                    assertThat("inherited the source segment's matrix of the field", mergedMatrix.id(), equalTo(sourceMatrix.id()));
                }
            }
        }
    }

    /**
     * Segments with fewer than twice as many vectors as projected dimensions use the seeded random projection: they store
     * only its seed, and share its id, and the query projection searches use, with every other such segment.
     */
    public void testSmallSegmentsShareTheSeededProjection() throws IOException {
        int dims = 64;
        int nDims = IvfSegmentConfig.AshConfig.defaults().projectedDims(dims);
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = newIndexWriterConfig();
            iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(ashTestFormat()));
            iwc.setMergePolicy(NoMergePolicy.INSTANCE);
            // one segment per commit
            iwc.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
            iwc.setRAMBufferSizeMB(256);
            int smallSegments = randomIntBetween(2, 4);
            int[] docsPerSegment = new int[smallSegments + 1];
            for (int s = 0; s < smallSegments; s++) {
                docsPerSegment[s] = randomIntBetween(1, 2 * nDims - 1);
            }
            docsPerSegment[smallSegments] = randomIntBetween(2 * nDims, 4 * nDims);
            try (IndexWriter w = new IndexWriter(dir, iwc)) {
                for (int docs : docsPerSegment) {
                    for (int i = 0; i < docs; i++) {
                        Document doc = new Document();
                        doc.add(
                            new KnnFloatVectorField("f", VectorTestUtils.randomFloatVector(dims), VectorSimilarityFunction.DOT_PRODUCT)
                        );
                        w.addDocument(doc);
                    }
                    w.commit();
                }
            }
            try (IndexReader reader = DirectoryReader.open(dir)) {
                assertThat(reader.leaves(), hasSize(docsPerSegment.length));
                Set<UUID> seededIds = new HashSet<>();
                for (LeafReaderContext ctx : reader.leaves()) {
                    LeafReader leafReader = ctx.reader();
                    AshProjectionMatrix matrix = ashReader(leafReader, "f").getProjectionMatrix(leafReader.getFieldInfos().fieldInfo("f"));
                    boolean small = leafReader.maxDoc() < 2 * nDims;
                    assertThat("segment of " + leafReader.maxDoc() + " docs", matrix.isSeeded(), equalTo(small));
                    assertThat(matrix.isLearned(), equalTo(small == false));
                    if (small) {
                        seededIds.add(matrix.id());
                    }
                    // searches work on both kinds of segment
                    TopDocs topDocs = leafReader.searchNearestVectors(
                        "f",
                        VectorTestUtils.randomFloatVector(dims),
                        5,
                        AcceptDocs.fromLiveDocs(leafReader.getLiveDocs(), leafReader.maxDoc()),
                        Integer.MAX_VALUE
                    );
                    assertThat(topDocs.scoreDocs.length, greaterThan(0));
                }
                assertThat("the small segments share the seeded projection", seededIds, hasSize(1));
            }
        }
    }

    /**
     * Byte vectors are not ASH-encoded, but the writer still resolves the dims-derived configuration for them and
     * records its bits, on flush and on merge.
     */
    public void testDimsDerivedConfigWithByteVectors() throws IOException {
        Codec ashCodec = TestUtil.alwaysKnnVectorsFormat(ashDimsDerivedFormat());
        int dims = randomFrom(random().nextInt(64, 512), random().nextInt(512, 2049), 2049);
        var expected = IvfSegmentConfig.AshConfig.forDims(dims, null, IvfSegmentConfig.AshConfig.DEFAULT_QUERY_BITS_PER_DIM);
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = newIndexWriterConfig();
            iwc.setCodec(ashCodec);
            try (IndexWriter w = new IndexWriter(dir, iwc)) {
                int numDocs = random().nextInt(100, 200);
                for (int i = 0; i < numDocs; i++) {
                    Document doc = new Document();
                    doc.add(new KnnByteVectorField("b", randomVector8(dims), VectorSimilarityFunction.EUCLIDEAN));
                    w.addDocument(doc);
                    if (rarely()) {
                        w.commit();
                    }
                }
                w.forceMerge(1);
                try (IndexReader reader = DirectoryReader.open(w)) {
                    LeafReader leafReader = getOnlyLeafReader(reader);
                    FieldInfo fieldInfo = leafReader.getFieldInfos().fieldInfo("b");
                    ESNextDiskASHVectorsReader ashReader = ashReader(leafReader, "b");
                    assertEquals(expected.bitsPerDim(), ashReader.getBitsPerDimForTest(fieldInfo));
                    assertNull(ashReader.getProjectionMatrix(fieldInfo));
                    ByteVectorValues values = leafReader.getByteVectorValues("b");
                    assertEquals(numDocs, values.size());
                    assertEquals(dims, values.dimension());
                    TopDocs topDocs = leafReader.searchNearestVectors(
                        "b",
                        randomVector8(dims),
                        10,
                        AcceptDocs.fromLiveDocs(leafReader.getLiveDocs(), leafReader.maxDoc()),
                        Integer.MAX_VALUE
                    );
                    assertThat(topDocs.scoreDocs, arrayWithSize(Math.min(numDocs, 10)));
                }
            }
        }
    }

    private static ESNextDiskASHVectorsReader ashReader(LeafReader leafReader, String field) {
        KnnVectorsReader vectorReader = ((CodecReader) leafReader).getVectorReader();
        if (vectorReader instanceof PerFieldKnnVectorsFormat.FieldsReader fieldsReader) {
            vectorReader = fieldsReader.getFieldReader(field);
        }
        assertThat(vectorReader, instanceOf(ESNextDiskASHVectorsReader.class));
        return (ESNextDiskASHVectorsReader) vectorReader;
    }

    /**
     * A sliced segment produced by a single flush is not clustered per slice ({@code numSlices == 0}) and is
     * searched over a doc id range. Plain Lucene {@link AcceptDocs} (as used by {@code CheckIndex}) carry no slice
     * information and must fall back to the whole segment, while an {@link ESAcceptDocs} without a slice ordinal
     * violates the reader contract and is rejected by assertion rather than silently searching a wrong range.
     */
    public void testSlicedFlushedSegmentWithoutSliceOrdinal() throws IOException {
        String sliceField = "_slice";
        String vectorField = "vector";
        int numDocs = random().nextInt(10, 200);
        int dimensions = random().nextInt(12, 128);
        ESNextDiskASHVectorsFormat localFormat = ashSlicedFormat(sliceField);
        IndexWriterConfig iwc = newIndexWriterConfig();
        SortField sliceSortField = new SortField(sliceField, SortField.Type.STRING, false, SortField.STRING_LAST);
        iwc.setIndexSort(new Sort(sliceSortField));
        iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(localFormat));
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, iwc)) {
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                doc.add(SortedDocValuesField.indexedField(sliceField, new BytesRef("" + random().nextInt(5))));
                doc.add(new KnnFloatVectorField(vectorField, randomUnitRangeVector(dimensions), VectorSimilarityFunction.EUCLIDEAN));
                w.addDocument(doc);
            }
            w.commit();
            try (IndexReader reader = DirectoryReader.open(w)) {
                for (LeafReaderContext context : reader.leaves()) {
                    LeafReader leafReader = context.reader();
                    KnnVectorsReader vectorReader = ((CodecReader) leafReader).getVectorReader();
                    if (vectorReader instanceof PerFieldKnnVectorsFormat.FieldsReader fieldsReader) {
                        vectorReader = fieldsReader.getFieldReader(vectorField);
                    }
                    assertThat(vectorReader, instanceOf(ESNextDiskASHVectorsReader.class));
                    // a flushed sliced segment is written as a single flat posting list
                    try (
                        IVFVectorsReader.CentroidData<?> centroidData = ((ESNextDiskASHVectorsReader) vectorReader).readCentroidData(
                            vectorField
                        )
                    ) {
                        assertThat(centroidData, notNullValue());
                        assertThat(centroidData.numCentroids(), equalTo(1));
                    }
                    float[] vector = randomUnitRangeVector(dimensions);
                    KnnCollector collector = new TopKnnCollector(leafReader.maxDoc(), Integer.MAX_VALUE);
                    leafReader.searchNearestVectors(vectorField, vector, collector, AcceptDocs.fromLiveDocs(null, leafReader.maxDoc()));
                    Set<Integer> docIds = new HashSet<>();
                    for (ScoreDoc scoreDoc : collector.topDocs().scoreDocs) {
                        docIds.add(scoreDoc.doc);
                    }
                    assertThat(docIds, hasSize(leafReader.maxDoc()));

                    // Call getPostingVisitor directly via a package-private test helper, bypassing the
                    // assertion in getNumberOfVectors, to test the fix in the numSlices==0 branch itself.
                    ESNextDiskASHVectorsReader ashReader = (ESNextDiskASHVectorsReader) vectorReader;
                    AssertionError error = expectThrows(
                        AssertionError.class,
                        () -> ashReader.getPostingVisitorForTest(vectorField, vector, new ESAcceptDocs.ESAcceptDocsAll())
                    );
                    assertThat(error.getMessage(), equalTo("sliced segment searched without a slice ordinal"));
                }
            }
        }
    }

    public void testSlicedIndexOneVectorPerSlice() throws IOException {
        String sliceField = "_slice";
        String vectorField = "vector";
        int slices = random().nextInt(2, 100);
        int dimensions = random().nextInt(12, 128);
        ESNextDiskASHVectorsFormat localFormat = ashSlicedFormat(sliceField);
        IndexWriterConfig iwc = newIndexWriterConfig();
        SortField sliceSortField = new SortField(sliceField, SortField.Type.STRING, false, SortField.STRING_LAST);
        iwc.setIndexSort(new Sort(sliceSortField));
        iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(localFormat));
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, iwc)) {
            for (int slice = 0; slice < slices; slice++) {
                Document doc = new Document();
                doc.add(SortedDocValuesField.indexedField(sliceField, new BytesRef("" + slice)));
                doc.add(new KnnFloatVectorField(vectorField, randomUnitRangeVector(dimensions), VectorSimilarityFunction.EUCLIDEAN));
                w.addDocument(doc);
            }
            w.commit();
            w.forceMerge(1);
            try (IndexReader reader = DirectoryReader.open(w)) {
                assertEquals(1, reader.leaves().size());
                LeafReader leafReader = reader.leaves().get(0).reader();
                KnnVectorsReader vectorReader = ((CodecReader) leafReader).getVectorReader();
                if (vectorReader instanceof PerFieldKnnVectorsFormat.FieldsReader fieldsReader) {
                    vectorReader = fieldsReader.getFieldReader(vectorField);
                }
                assertThat(vectorReader, instanceOf(ESNextDiskASHVectorsReader.class));
                try (
                    IVFVectorsReader.CentroidData<?> centroidData = ((ESNextDiskASHVectorsReader) vectorReader).readCentroidData(
                        vectorField
                    )
                ) {
                    assertNotNull(centroidData);
                    assertThat(centroidData.numCentroids(), equalTo(1));
                    assertThat(centroidData.centroids().size(), equalTo(1));
                }
            }
        }
    }

    public void testSlicesDense() throws IOException {
        doTestSlices(() -> true, false);
    }

    public void testSlicesDenseWithFilter() throws IOException {
        doTestSlices(() -> true, true);
    }

    public void testSlicesSparse() throws IOException {
        int bound = random().nextInt(2, 50);
        doTestSlices(() -> random().nextInt(bound) == 0, false);
    }

    public void testSlicesSparseWithFilter() throws IOException {
        int bound = random().nextInt(2, 50);
        doTestSlices(() -> random().nextInt(bound) == 0, true);
    }

    private void doTestSlices(BooleanSupplier hasVectorSupplier, boolean applyFilter) throws IOException {
        String sliceField = "_slice";
        String filterField = "_filter";
        String filterValue = "match";
        String filterMiss = "miss";
        String docIdField = "_doc_id";
        String vectorField = "vector";
        ESNextDiskASHVectorsFormat localFormat = ashSlicedFormat(sliceField);
        int dimensions = random().nextInt(12, 128);
        int slices = random().nextInt(2, 50);
        int numDocs = random().nextInt(100, 5000);
        int[] docsPerSlice = new int[slices];
        int[] docsPerSliceFiltered = new int[slices];
        int[] docSlices = new int[numDocs];
        boolean[] docHasVector = new boolean[numDocs];
        boolean[] docFilterMatch = new boolean[numDocs];
        IndexWriterConfig iwc = newIndexWriterConfig();
        SortField sliceSortField = new SortField(sliceField, SortField.Type.STRING, false, SortField.STRING_LAST);
        iwc.setIndexSort(new Sort(sliceSortField));
        iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(localFormat));
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, iwc)) {
            for (int i = 0; i < numDocs; i++) {
                int slice = random().nextInt(slices);
                Document doc = new Document();
                doc.add(SortedDocValuesField.indexedField(sliceField, new BytesRef("" + slice)));
                boolean filterMatch = random().nextBoolean();
                String filterText = filterMatch ? filterValue : filterMiss;
                doc.add(new StringField(filterField, filterText, Field.Store.NO));
                doc.add(new StoredField(filterField, new BytesRef(filterText)));
                doc.add(new StringField(docIdField, "doc_" + i, Field.Store.NO));
                boolean hasVector = hasVectorSupplier.getAsBoolean();
                if (hasVector) {
                    docsPerSlice[slice]++;
                    if (filterMatch) {
                        docsPerSliceFiltered[slice]++;
                    }
                    doc.add(new KnnFloatVectorField(vectorField, randomUnitRangeVector(dimensions), VectorSimilarityFunction.EUCLIDEAN));
                }
                doc.add(new StoredField(sliceField, new BytesRef("" + slice)));
                w.addDocument(doc);
                docSlices[i] = slice;
                docHasVector[i] = hasVector;
                docFilterMatch[i] = filterMatch;
            }
            w.commit();
            if (random().nextBoolean()) {
                int deleteCount = random().nextInt(0, Math.max(1, numDocs / 10));
                Set<Integer> docsToDelete = new HashSet<>();
                while (docsToDelete.size() < deleteCount) {
                    docsToDelete.add(random().nextInt(numDocs));
                }
                for (int docId : docsToDelete) {
                    if (docHasVector[docId]) {
                        docsPerSlice[docSlices[docId]]--;
                        if (docFilterMatch[docId]) {
                            docsPerSliceFiltered[docSlices[docId]]--;
                        }
                    }
                    w.deleteDocuments(new Term(docIdField, "doc_" + docId));
                }
                if (docsToDelete.isEmpty() == false) {
                    w.commit();
                }
            } else if (random().nextBoolean()) {
                w.forceMerge(1);
            }
            float[] vector = randomUnitRangeVector(dimensions);
            try (IndexReader reader = DirectoryReader.open(w)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                Weight filterWeight = null;
                if (applyFilter) {
                    Query filterQuery = new TermQuery(new Term(filterField, filterValue));
                    filterWeight = filterQuery.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1);
                }
                for (int slice = 0; slice < slices; slice++) {
                    int expectedDocs = applyFilter ? docsPerSliceFiltered[slice] : docsPerSlice[slice];
                    Query query = SortedDocValuesField.newSlowExactQuery(sliceField, new BytesRef("" + slice));
                    Weight weight = query.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1);
                    TopDocs[] topDocsArray = new TopDocs[reader.leaves().size()];
                    for (int i = 0; i < reader.leaves().size(); i++) {
                        LeafReaderContext context = reader.leaves().get(i);
                        LeafReader leafReader = context.reader();

                        int ord = leafReader.getSortedDocValues(sliceField).lookupTerm(new BytesRef("" + slice));
                        if (ord < 0) {
                            topDocsArray[i] = TopDocsCollector.EMPTY_TOPDOCS;
                            continue;
                        }

                        ScorerSupplier scorerSupplier = weight.scorerSupplier(context);
                        DocIdSetIterator iterator = scorerSupplier.get(DocIdSetIterator.NO_MORE_DOCS).iterator();
                        int minDoc = iterator.nextDoc();
                        if (minDoc == DocIdSetIterator.NO_MORE_DOCS) {
                            assertEquals(0, expectedDocs);
                            continue;
                        }
                        int maxDoc = minDoc;
                        while (iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                            maxDoc = iterator.docID();
                        }
                        ESAcceptDocs.SliceAcceptDocs sliceAcceptDocs = new SliceAcceptDocs(minDoc, maxDoc + 1);
                        Bits liveDocs = leafReader.getLiveDocs();
                        ESAcceptDocs acceptDocs;
                        if (applyFilter) {
                            ScorerSupplier filterSupplier = filterWeight.scorerSupplier(context);
                            if (filterSupplier == null) {
                                topDocsArray[i] = TopDocsCollector.EMPTY_TOPDOCS;
                                continue;
                            }
                            acceptDocs = new ESAcceptDocs.ScorerSupplierAcceptDocs(
                                () -> filterSupplier.get(Long.MAX_VALUE).iterator(),
                                filterSupplier::cost,
                                liveDocs,
                                leafReader.maxDoc(),
                                ord,
                                () -> sliceAcceptDocs
                            );
                        } else if (liveDocs == null) {
                            acceptDocs = new ESAcceptDocs.ESAcceptDocsAll(ord, () -> sliceAcceptDocs);
                        } else {
                            acceptDocs = new ESAcceptDocs.BitsAcceptDocs(liveDocs, leafReader.maxDoc(), ord, () -> sliceAcceptDocs);
                        }

                        // we might collect the same document twice because of soar assignments
                        KnnCollector collector = new TopKnnCollector(2 * Math.max(1, expectedDocs), Integer.MAX_VALUE);
                        weight.scorer(context);
                        leafReader.searchNearestVectors(vectorField, vector, collector, acceptDocs);
                        TopDocs leafTopDocs = collector.topDocs();
                        ScoreDoc[] adjusted = new ScoreDoc[leafTopDocs.scoreDocs.length];
                        for (int docIndex = 0; docIndex < leafTopDocs.scoreDocs.length; docIndex++) {
                            ScoreDoc scoreDoc = leafTopDocs.scoreDocs[docIndex];
                            adjusted[docIndex] = new ScoreDoc(scoreDoc.doc + context.docBase, scoreDoc.score);
                        }
                        topDocsArray[i] = new TopDocs(leafTopDocs.totalHits, adjusted);
                    }
                    TopDocs topDocs = TopDocs.merge(2 * expectedDocs, topDocsArray);
                    Set<Integer> uniqueDocIds = new HashSet<>();
                    for (int i = 0; i < topDocs.scoreDocs.length; i++) {
                        uniqueDocIds.add(topDocs.scoreDocs[i].doc);
                        Document document = reader.storedFields().document(topDocs.scoreDocs[i].doc);
                        assertThat(document.getField(sliceField).binaryValue().utf8ToString(), equalTo("" + slice));
                        if (applyFilter) {
                            assertThat(document.getField(filterField).binaryValue().utf8ToString(), equalTo(filterValue));
                        }
                    }
                    assertThat(uniqueDocIds, hasSize(expectedDocs));
                }
            }
        }
    }

    private static float[] randomUnitRangeVector(int dims) {
        float[] v = new float[dims];
        for (int i = 0; i < dims; i++) {
            v[i] = random().nextFloat() * 2 - 1;
        }
        return v;
    }

    private static ESNextDiskASHVectorsFormat ashTestFormat() {
        return new ESNextDiskASHVectorsFormat(
            MIN_VECTORS_PER_CLUSTER,
            ESNextDiskASHVectorsFormat.DEFAULT_CENTROIDS_PER_PARENT_CLUSTER,
            null
        );
    }

    private static ESNextDiskASHVectorsFormat ashDimsDerivedFormat() {
        return ashDimsDerivedFormat(null);
    }

    private static ESNextDiskASHVectorsFormat ashDimsDerivedFormat(Integer bits) {
        return new ESNextDiskASHVectorsFormat(
            AshConfigResolver.dimsDerived(bits, IvfSegmentConfig.AshConfig.DEFAULT_QUERY_BITS_PER_DIM),
            MIN_VECTORS_PER_CLUSTER,
            ESNextDiskASHVectorsFormat.DEFAULT_CENTROIDS_PER_PARENT_CLUSTER,
            DenseVectorFieldMapper.ElementType.FLOAT,
            false,
            null,
            1,
            null,
            -1,
            null,
            IvfFlushConfigSource.empty(),
            IvfMergeConfigResolver.useCodecDefault(),
            false
        );
    }

    private static ESNextDiskASHVectorsFormat ashSlicedFormat(String sliceField) {
        return new ESNextDiskASHVectorsFormat(
            MIN_VECTORS_PER_CLUSTER,
            ESNextDiskASHVectorsFormat.DEFAULT_CENTROIDS_PER_PARENT_CLUSTER,
            sliceField
        );
    }
}
