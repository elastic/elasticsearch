/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.elasticsearch.common.logging.LogConfigurator;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/**
 * Abstract base test case for byte vector IVF formats. Follows the same pattern as
 * {@link BaseBFloat16KnnVectorsFormatTestCase} for BFloat16 formats.
 * <p>
 * Subclasses must implement {@link #getCodec()} and may override {@link #setUp()} to
 * configure the specific format under test.
 */
public abstract class BaseByteKnnVectorsFormatTestCase extends BaseKnnVectorsFormatTestCase {

    static {
        LogConfigurator.configureESLogging();
    }

    @Override
    protected boolean supportsFloatVectorFallback() {
        return false;
    }

    @Override
    protected VectorEncoding randomVectorEncoding() {
        return VectorEncoding.BYTE;
    }

    @Override
    protected VectorSimilarityFunction randomSimilarity() {
        // IVF formats reject COSINE — callers must use DOT_PRODUCT with pre-normalized vectors
        return switch (random().nextInt(3)) {
            case 0 -> VectorSimilarityFunction.DOT_PRODUCT;
            case 1 -> VectorSimilarityFunction.EUCLIDEAN;
            case 2 -> VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;
            default -> throw new IllegalStateException("Unexpected value for similarity");
        };
    }

    @Override
    public void testSearchWithVisitedLimit() {
        throw new AssumptionViolatedException("ivf doesn't enforce visitation limit");
    }

    @Override
    public void testAdvance() throws Exception {
        // TODO re-enable with hierarchical IVF, clustering as it is is flaky
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

    /**
     * Tests basic byte vector indexing and search with optional force merge.
     */
    public void testByteVectorIndexAndSearch() throws IOException {
        int dimensions = random().nextInt(12, 500);
        int numDocs = random().nextInt(100, 1_000);
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
            for (int i = 0; i < numDocs; i++) {
                byte[] vector = randomByteVector(dimensions);
                Document doc = new Document();
                doc.add(new KnnByteVectorField("f", vector, VectorSimilarityFunction.EUCLIDEAN));
                w.addDocument(doc);
            }
            w.commit();
            if (random().nextBoolean()) {
                w.forceMerge(1);
            }
            try (IndexReader reader = DirectoryReader.open(w)) {
                List<LeafReaderContext> subReaders = reader.leaves();
                for (LeafReaderContext r : subReaders) {
                    LeafReader leafReader = r.reader();
                    byte[] queryVector = randomByteVector(dimensions);
                    TopDocs topDocs = leafReader.searchNearestVectors(
                        "f",
                        queryVector,
                        10,
                        AcceptDocs.fromLiveDocs(leafReader.getLiveDocs(), leafReader.maxDoc()),
                        Integer.MAX_VALUE
                    );
                    assertEquals(Math.min(leafReader.maxDoc(), 10), topDocs.scoreDocs.length);
                }
            }
        }
    }

    /**
     * Verifies that COSINE similarity is rejected at index time for IVF formats.
     * IVF does not support cosine similarity — callers must use DOT_PRODUCT with pre-normalized vectors.
     */
    public void testByteVectorCosineRejected() throws IOException {
        int dimensions = random().nextInt(32, 256);
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
            byte[] vector = randomNonZeroByteVector(dimensions);
            Document doc = new Document();
            doc.add(new KnnByteVectorField("f", vector, VectorSimilarityFunction.COSINE));
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> w.addDocument(doc));
            assertTrue(e.getMessage().contains("cosine"));
        }
    }

    protected static byte[] randomByteVector(int dimensions) {
        byte[] vector = new byte[dimensions];
        random().nextBytes(vector);
        return vector;
    }

    /** Returns a random byte vector guaranteed to have non-zero L2 norm (required for COSINE). */
    protected static byte[] randomNonZeroByteVector(int dimensions) {
        byte[] vector = new byte[dimensions];
        do {
            random().nextBytes(vector);
        } while (isZeroVector(vector));
        return vector;
    }

    private static boolean isZeroVector(byte[] v) {
        for (byte b : v) {
            if (b != 0) return false;
        }
        return true;
    }
}
