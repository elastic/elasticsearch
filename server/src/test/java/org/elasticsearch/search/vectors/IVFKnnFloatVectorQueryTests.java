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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.LongAccumulator;

public class IVFKnnFloatVectorQueryTests extends AbstractIVFKnnVectorQueryTestCase<float[]> {

    @Override
    float[] vector(int... components) {
        float[] v = new float[components.length];
        for (int i = 0; i < components.length; i++) {
            v[i] = components[i];
        }
        return v;
    }

    @Override
    float[][] createVectorArray(int size) {
        return new float[size][];
    }

    @Override
    IVFKnnFloatVectorQuery getKnnVectorQuery(String field, float[] query, int k, Query queryFilter, float visitRatio) {
        return new IVFKnnFloatVectorQuery(field, query, k, k, queryFilter, visitRatio, testResolver());
    }

    @Override
    float[] randomVector(int dim) {
        return VectorTestUtils.randomNormalizedFloatVector(dim);
    }

    @Override
    Field getKnnVectorField(String name, float[] vector, VectorSimilarityFunction similarityFunction) {
        return new KnnFloatVectorField(name, vector, similarityFunction);
    }

    @Override
    Field getKnnVectorField(String name, float[] vector) {
        return new KnnFloatVectorField(name, vector);
    }

    public void testEqualsDifferentResolver() {
        float[] queryVector = new float[] { 0, 1 };
        TestIvfQueryConfigResolver resolver1 = new TestIvfQueryConfigResolver(
            CentroidIndexFormat.FLAT,
            QuantEncoding.ONE_BIT_4BIT_QUERY,
            false,
            1.0f
        );
        TestIvfQueryConfigResolver resolver2 = new TestIvfQueryConfigResolver(
            CentroidIndexFormat.FLAT,
            QuantEncoding.ONE_BIT_4BIT_QUERY,
            false,
            2.0f
        );
        IVFKnnFloatVectorQuery q1 = new IVFKnnFloatVectorQuery("field", queryVector, 10, 10, null, 0.05f, resolver1);
        IVFKnnFloatVectorQuery q2 = new IVFKnnFloatVectorQuery("field", queryVector, 10, 10, null, 0.05f, resolver2);
        IVFKnnFloatVectorQuery q3 = new IVFKnnFloatVectorQuery("field", queryVector, 10, 10, null, 0.05f, resolver1);

        // Queries with different resolvers must not be equal (prevents query cache collisions)
        assertNotEquals(q1, q2);
        assertNotEquals(q1.hashCode(), q2.hashCode());

        // Queries with the same resolver config must still be equal
        assertEquals(q1, q3);
        assertEquals(q1.hashCode(), q3.hashCode());
    }

    public void testToString() throws IOException {
        try (
            Directory indexStore = getIndexStore("field", vector(0, 1), vector(1, 2), vector(0, 0));
            IndexReader reader = DirectoryReader.open(indexStore)
        ) {
            AbstractIVFKnnVectorQuery query = getKnnVectorQuery("field", new float[] { 0.0f, 1.0f }, 10);
            assertEquals("IVFKnnFloatVectorQuery:field[0.0,...][10]", query.toString("ignored"));

            assertDocScoreQueryToString(query.rewrite(newSearcher(reader)));

            // test with filter
            Query filter = new TermQuery(new Term("id", "text"));
            query = getKnnVectorQuery("field", new float[] { 0.0f, 1.0f }, 10, filter);
            assertEquals("IVFKnnFloatVectorQuery:field[0.0,...][10][id:text]", query.toString("ignored"));
        }
    }

    /**
     * The cross-leaf min-competitive {@link LongAccumulator} must be a single instance shared by every
     * per-leaf collector manager; otherwise each leaf accumulates in isolation and cross-leaf pruning is
     * inert. This is a wiring regression: it fails if the accumulator is (re)created per collector manager.
     */
    public void testAccumulatorSharedAcrossLeaves() throws IOException {
        int dim = 8;
        try (Directory dir = buildMultiSegmentIndex(4, 3, dim); IndexReader reader = DirectoryReader.open(dir)) {
            assertTrue("test requires multiple segments", reader.leaves().size() > 1);
            IndexSearcher searcher = newSearcher(reader);
            List<LongAccumulator> captured = Collections.synchronizedList(new ArrayList<>());
            AbstractIVFKnnVectorQuery query = new CapturingIVFKnnFloatVectorQuery("field", randomVector(dim), 3, testResolver(), captured);
            query.rewrite(searcher);

            assertEquals("one collector manager per leaf", reader.leaves().size(), captured.size());
            Set<LongAccumulator> distinct = Collections.newSetFromMap(new IdentityHashMap<>());
            distinct.addAll(captured);
            assertEquals("all leaves must share a single accumulator instance", 1, distinct.size());
            assertNotNull("multi-leaf accumulator must be non-null", captured.get(0));
        }
    }

    /** With a single leaf there is nothing to share, so the accumulator is left null (no atomic churn). */
    public void testAccumulatorNullForSingleLeaf() throws IOException {
        int dim = 8;
        try (Directory dir = newDirectoryForTest()) {
            IndexWriterConfig iwc = new IndexWriterConfig().setCodec(TestUtil.alwaysKnnVectorsFormat(format));
            try (IndexWriter w = new IndexWriter(dir, iwc)) {
                for (int i = 0; i < 8; i++) {
                    Document doc = new Document();
                    doc.add(getKnnVectorField("field", randomVector(dim)));
                    w.addDocument(doc);
                }
                w.forceMerge(1);
            }
            try (IndexReader reader = DirectoryReader.open(dir)) {
                assertEquals(1, reader.leaves().size());
                IndexSearcher searcher = newSearcher(reader);
                List<LongAccumulator> captured = Collections.synchronizedList(new ArrayList<>());
                AbstractIVFKnnVectorQuery query = new CapturingIVFKnnFloatVectorQuery(
                    "field",
                    randomVector(dim),
                    3,
                    testResolver(),
                    captured
                );
                query.rewrite(searcher);
                assertEquals(1, captured.size());
                assertNull("single-leaf accumulator must be null", captured.get(0));
            }
        }
    }

    /**
     * Cross-leaf A/B against the pre-fix behavior: sharing one accumulator across leaves must never make the
     * search scan MORE vectors than a per-leaf accumulator, and must not change the top-k.
     * <p>
     * Note on {@code vectorOpsCount}: in the ES920 codec the two counts are in fact <b>equal</b>. The codec
     * consults {@code minCompetitiveSimilarity()} only to skip queue insertions ({@code collectBulk}), while
     * the scored-vector count is governed by the per-segment visit budget ({@code maxVectorsToVisit}) and is
     * incremented unconditionally. The shared global floor therefore saves heap/collect work, not scoring
     * work, so it does not lower {@code vectorOpsCount} here. The assertion is {@code <=} to encode the
     * invariant "sharing never costs more"; tighten it to strictly-less-than if a codec begins honoring the
     * global floor to skip scoring.
     */
    public void testCrossLeafFloorDoesNotIncreaseWorkOrChangeResults() throws IOException {
        int dim = 32;
        int k = 10;
        try (Directory dir = buildMultiSegmentIndex(3, 128, dim); IndexReader reader = DirectoryReader.open(dir)) {
            assertTrue("test requires multiple segments", reader.leaves().size() > 1);
            IndexSearcher searcher = new IndexSearcher(reader); // serial: deterministic leaf order

            float[] q = randomVector(dim);
            IVFKnnFloatVectorQuery shared = new IVFKnnFloatVectorQuery("field", q, k, k, null, 0.5f, testResolver());
            IVFKnnFloatVectorQuery perLeaf = new PerLeafAccumulatorIVFKnnFloatVectorQuery("field", q, k, testResolver());

            TopDocs sharedDocs = searcher.search(shared, k);
            TopDocs perLeafDocs = searcher.search(perLeaf, k);

            assertTrue(
                "sharing the cross-leaf floor must not scan more than isolating it per-leaf: shared="
                    + shared.vectorOpsCount
                    + " perLeaf="
                    + perLeaf.vectorOpsCount,
                shared.vectorOpsCount <= perLeaf.vectorOpsCount
            );

            assertEquals(perLeafDocs.scoreDocs.length, sharedDocs.scoreDocs.length);
            for (int i = 0; i < sharedDocs.scoreDocs.length; i++) {
                assertEquals(perLeafDocs.scoreDocs[i].doc, sharedDocs.scoreDocs[i].doc);
                assertEquals(perLeafDocs.scoreDocs[i].score, sharedDocs.scoreDocs[i].score, EPSILON);
            }
        }
    }

    private Directory buildMultiSegmentIndex(int numSegments, int docsPerSegment, int dim) throws IOException {
        Directory dir = newDirectoryForTest();
        IndexWriterConfig iwc = new IndexWriterConfig().setCodec(TestUtil.alwaysKnnVectorsFormat(format));
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        try (IndexWriter w = new IndexWriter(dir, iwc)) {
            for (int s = 0; s < numSegments; s++) {
                for (int i = 0; i < docsPerSegment; i++) {
                    Document doc = new Document();
                    doc.add(getKnnVectorField("field", randomVector(dim)));
                    w.addDocument(doc);
                }
                w.commit(); // force a new segment per commit (merges disabled)
            }
        }
        return dir;
    }

    /** Records the {@link LongAccumulator} handed to each per-leaf collector manager during {@code rewrite()}. */
    private static class CapturingIVFKnnFloatVectorQuery extends IVFKnnFloatVectorQuery {
        private final List<LongAccumulator> captured;

        CapturingIVFKnnFloatVectorQuery(
            String field,
            float[] query,
            int k,
            TestIvfQueryConfigResolver resolver,
            List<LongAccumulator> captured
        ) {
            super(field, query, k, k, null, 0.05f, resolver);
            this.captured = captured;
        }

        @Override
        protected AbstractIVFKnnVectorQuery.IVFCollectorManager getKnnCollectorManager(int k, LongAccumulator longAccumulator) {
            captured.add(longAccumulator);
            return super.getKnnCollectorManager(k, longAccumulator);
        }
    }

    /** Reproduces the pre-fix behavior: a fresh per-leaf accumulator, so nothing is shared across leaves. */
    private static class PerLeafAccumulatorIVFKnnFloatVectorQuery extends IVFKnnFloatVectorQuery {
        PerLeafAccumulatorIVFKnnFloatVectorQuery(String field, float[] query, int k, TestIvfQueryConfigResolver resolver) {
            super(field, query, k, k, null, 0.5f, resolver);
        }

        @Override
        protected AbstractIVFKnnVectorQuery.IVFCollectorManager getKnnCollectorManager(int k, LongAccumulator ignored) {
            return super.getKnnCollectorManager(k, new LongAccumulator(Long::max, AbstractMaxScoreKnnCollector.LEAST_COMPETITIVE));
        }
    }
}
