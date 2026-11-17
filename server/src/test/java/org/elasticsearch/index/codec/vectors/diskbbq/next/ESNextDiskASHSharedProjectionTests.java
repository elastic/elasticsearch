/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.next;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfSegmentConfig;
import org.elasticsearch.search.vectors.IVFKnnSearchStrategy;
import org.elasticsearch.search.vectors.SharedQueryTransforms;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.Matchers.hasSize;

/**
 * Reuse of a query's projection across the segments of a query that share the ASH projection matrix W, through
 * {@link SharedQueryTransforms}.
 */
public class ESNextDiskASHSharedProjectionTests extends ESTestCase {

    private static final String FIELD = "f";
    // ASH defaults project 128 dims to 64, learning W from a sample of up to 10 * 128 vectors
    private static final int DIMS = 128;
    private static final int FULL_SAMPLE = 10 * DIMS;

    private int queryBitsPerDim;

    @Before
    public void randomizeQueryBits() {
        // both the float scoring path (0) and the integer one, whose quantized query is shared too
        queryBitsPerDim = randomFrom(0, IvfSegmentConfig.AshConfig.DEFAULT_QUERY_BITS_PER_DIM);
    }

    private IndexWriterConfig writerConfig() {
        IndexWriterConfig iwc = new IndexWriterConfig();
        iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(ESNextDiskASHTestFixture.ashFormat(queryBitsPerDim, null)));
        // keep each flush its own segment
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        iwc.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
        iwc.setRAMBufferSizeMB(1024);
        return iwc;
    }

    /** A low-rank basis, so that the learned projection keeps most of the vectors. */
    private static float[][] basis() {
        float[][] basis = new float[16][DIMS];
        for (float[] b : basis) {
            for (int d = 0; d < DIMS; d++) {
                b[d] = randomFloat() * 2 - 1;
            }
        }
        return basis;
    }

    private static float[] vector(float[][] basis) {
        float[] v = new float[DIMS];
        for (float[] b : basis) {
            float w = randomFloat() * 2 - 1;
            for (int d = 0; d < DIMS; d++) {
                v[d] += w * b[d];
            }
        }
        for (int d = 0; d < DIMS; d++) {
            v[d] += (randomFloat() - 0.5f) * 0.1f;
        }
        return v;
    }

    private static void flush(IndexWriter w, float[][] basis, int docs) throws IOException {
        for (int i = 0; i < docs; i++) {
            Document doc = new Document();
            doc.add(new KnnFloatVectorField(FIELD, vector(basis), VectorSimilarityFunction.EUCLIDEAN));
            w.addDocument(doc);
        }
        w.commit();
    }

    /** The projection matrix ids of the segments, in segment order. */
    private static List<UUID> segmentMatrixIds(DirectoryReader reader) {
        List<UUID> ids = new ArrayList<>();
        for (LeafReaderContext ctx : reader.leaves()) {
            FieldInfo fieldInfo = ctx.reader().getFieldInfos().fieldInfo(FIELD);
            ids.add(ESNextDiskASHTestFixture.ashReader(ctx.reader(), FIELD).getProjectionMatrix(fieldInfo).id());
        }
        return ids;
    }

    public void testQueryIsProjectedOncePerSharedProjection() throws IOException {
        float[][] basis = basis();
        try (Directory source = newDirectory(); Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(source, writerConfig())) {
                flush(w, basis, FULL_SAMPLE + randomIntBetween(0, 100));
            }
            int copies = randomIntBetween(2, 4);
            try (IndexWriter w = new IndexWriter(dir, writerConfig())) {
                // copies of a segment keep its projection matrix, and so share it
                for (int c = 0; c < copies; c++) {
                    w.addIndexes(source);
                }
                try (DirectoryReader reader = DirectoryReader.open(w)) {
                    assertThat(reader.leaves(), hasSize(copies));
                    assertEquals(1, new HashSet<>(segmentMatrixIds(reader)).size());
                    for (int q = 0; q < 5; q++) {
                        float[] query = vector(basis);
                        SharedQueryTransforms transforms = new SharedQueryTransforms();
                        for (LeafReaderContext ctx : reader.leaves()) {
                            assertSameResults(search(ctx.reader(), query, null), search(ctx.reader(), query, transforms));
                        }
                        assertEquals("one projection for all the segments sharing W", 1, transforms.size());
                    }
                }
            }
        }
    }

    public void testQueryIsProjectedPerDistinctProjection() throws IOException {
        float[][] basis = basis();
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, writerConfig())) {
            int segments = randomIntBetween(2, 4);
            for (int s = 0; s < segments; s++) {
                flush(w, basis, FULL_SAMPLE);
            }
            try (DirectoryReader reader = DirectoryReader.open(w)) {
                assertThat(reader.leaves(), hasSize(segments));
                int distinct = new HashSet<>(segmentMatrixIds(reader)).size();
                assertEquals("each flush learns its own projection", segments, distinct);
                float[] query = vector(basis);
                SharedQueryTransforms transforms = new SharedQueryTransforms();
                for (LeafReaderContext ctx : reader.leaves()) {
                    assertSameResults(search(ctx.reader(), query, null), search(ctx.reader(), query, transforms));
                }
                assertEquals(distinct, transforms.size());
            }
        }
    }

    public void testSegmentSearchedRepeatedlyProjectsOnce() throws IOException {
        // a sliced query searches a segment once for each named slice it requests, with the same query
        float[][] basis = basis();
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, writerConfig())) {
            flush(w, basis, randomIntBetween(FULL_SAMPLE / 2, FULL_SAMPLE));
            try (DirectoryReader reader = DirectoryReader.open(w)) {
                LeafReader leafReader = reader.leaves().get(0).reader();
                float[] query = vector(basis);
                TopDocs expected = search(leafReader, query, null);
                SharedQueryTransforms transforms = new SharedQueryTransforms();
                int searches = randomIntBetween(2, 5);
                for (int i = 0; i < searches; i++) {
                    assertSameResults(expected, search(leafReader, query, transforms));
                }
                assertEquals(1, transforms.size());
            }
        }
    }

    private static TopDocs search(LeafReader leafReader, float[] query, SharedQueryTransforms transforms) throws IOException {
        int k = 10;
        IVFKnnSearchStrategy strategy = new IVFKnnSearchStrategy(1.0f, k, k, null, transforms);
        TopKnnCollector collector = new TopKnnCollector(k, Integer.MAX_VALUE, strategy);
        leafReader.searchNearestVectors(FIELD, query, collector, AcceptDocs.fromLiveDocs(leafReader.getLiveDocs(), leafReader.maxDoc()));
        return collector.topDocs();
    }

    private static void assertSameResults(TopDocs expected, TopDocs actual) {
        assertEquals(expected.scoreDocs.length, actual.scoreDocs.length);
        for (int i = 0; i < expected.scoreDocs.length; i++) {
            assertEquals(expected.scoreDocs[i].doc, actual.scoreDocs[i].doc);
            assertEquals(expected.scoreDocs[i].score, actual.scoreDocs[i].score, 0f);
        }
    }
}
