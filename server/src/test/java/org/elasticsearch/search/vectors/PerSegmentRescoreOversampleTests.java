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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextRescoreOversampleTestFixture;
import org.elasticsearch.index.codec.vectors.diskbbq.next.IvfMergeConfigResolver;
import org.elasticsearch.index.codec.vectors.diskbbq.next.IvfSegmentConfig;

import java.io.IOException;

public class PerSegmentRescoreOversampleTests extends LuceneTestCase {

    static {
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging();
    }

    public void testResolveCollectorKWithFinitePersistedUsesPersisted() {
        // persisted = 3.0 wins regardless of fallback; collectorK = round(2 * ceil(10 * 3)) = 60, capped at 60.
        assertEquals(60, AbstractIVFKnnVectorQuery.resolveCollectorK(3.0f, 10, 2.0f, 60));
        // capped to maxK when the math would exceed it.
        assertEquals(40, AbstractIVFKnnVectorQuery.resolveCollectorK(3.0f, 10, 2.0f, 40));
    }

    public void testResolveCollectorKWithNonFinitePersistedFallsBackToFallback() {
        // persisted NaN, fallback = 2.0 -> oversample = 2.0; collectorK = round(2 * ceil(10*2)) = 40.
        assertEquals(40, AbstractIVFKnnVectorQuery.resolveCollectorK(Float.NaN, 10, 2.0f, 60));
    }

    public void testResolveCollectorKWithNoOversampleClampsToOne() {
        // both NaN -> oversample = 1.0; collectorK = round(2 * 10) = 20.
        assertEquals(20, AbstractIVFKnnVectorQuery.resolveCollectorK(Float.NaN, 10, Float.NaN, 60));
        // sub-1 oversample is also clamped up to 1.0.
        assertEquals(20, AbstractIVFKnnVectorQuery.resolveCollectorK(0.5f, 10, Float.NaN, 60));
    }

    public void testResolveCollectorKHonorsMaxKAndMinimum() {
        // result must be >= 1 even when maxK is unrealistically small.
        assertEquals(1, AbstractIVFKnnVectorQuery.resolveCollectorK(3.0f, 10, 2.0f, 0));
        assertEquals(1, AbstractIVFKnnVectorQuery.resolveCollectorK(3.0f, 10, 2.0f, 1));
    }

    public void testPerSegmentCollectorKOnNextFormatWithDefaultMetaUsesFallback() throws IOException {
        // The default ESNextDiskBBQVectorsFormat persists NaN for rescoreOversample, so each leaf
        // should fall through to the supplied fallback. With kRequest=10, fallback=2.0, maxK=60:
        // oversample_seg = 2.0 -> collectorK = round(2 * 20) = 40.
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = newIndexWriterConfig();
            iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(new ESNextDiskBBQVectorsFormat(128, 4, null)));
            try (IndexWriter w = new IndexWriter(dir, iwc)) {
                for (int i = 0; i < 10; i++) {
                    Document d = new Document();
                    d.add(new KnnFloatVectorField("f", randomUnitVector(8)));
                    w.addDocument(d);
                }
                w.commit();
            }
            try (IndexReader reader = DirectoryReader.open(dir)) {
                assertFalse("expected at least one leaf", reader.leaves().isEmpty());
                for (LeafReaderContext leaf : reader.leaves()) {
                    int collectorK = AbstractIVFKnnVectorQuery.perSegmentCollectorK(leaf, "f", 10, 2.0f, 60);
                    assertEquals("fallback (NaN persisted -> mapping default 2.0)", 40, collectorK);

                    int legacyEquivalent = AbstractIVFKnnVectorQuery.perSegmentCollectorK(leaf, "f", 10, Float.NaN, 60);
                    assertEquals("no fallback (NaN persisted, NaN fallback -> oversample=1.0)", 20, legacyEquivalent);
                }
            }
        }
    }

    public void testIVFQueryLegacyConstructorKeepsLegacyKEquality() {
        // kRequest defaults to k for legacy callers — the per-segment path is bypassed.
        IVFKnnFloatVectorQuery q1 = new IVFKnnFloatVectorQuery("f", new float[] { 0f, 1f }, 30, 30, null, 0.05f, false);
        IVFKnnFloatVectorQuery q2 = new IVFKnnFloatVectorQuery("f", new float[] { 0f, 1f }, 30, 30, null, 0.05f, false);
        assertEquals(q1, q2);
        assertEquals(q1.hashCode(), q2.hashCode());
        assertEquals("IVFKnnFloatVectorQuery:f[0.0,...][30]", q1.toString("ignored"));
    }

    public void testIVFQueryPerSegmentConstructorDifferentiatesEquality() {
        // Same outer params but different (kRequest, oversampleFallback) must not be equal,
        // because they imply different per-leaf collector sizing at search time.
        IVFKnnFloatVectorQuery legacy = new IVFKnnFloatVectorQuery("f", new float[] { 0f, 1f }, 30, 30, null, 0.05f, false);
        IVFKnnFloatVectorQuery perSeg = new IVFKnnFloatVectorQuery("f", new float[] { 0f, 1f }, 30, 30, null, 0.05f, false, 10, 2.0f);
        assertNotEquals(legacy, perSeg);
        // toString includes the new fields when the per-segment path is active.
        assertEquals("IVFKnnFloatVectorQuery:f[0.0,...][30][kRequest=10,oversampleFallback=2.0]", perSeg.toString("ignored"));
    }

    public void testIVFQueryRejectsKRequestLargerThanK() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new IVFKnnFloatVectorQuery("f", new float[] { 0f, 1f }, 10, 10, null, 0.05f, false, 11, Float.NaN)
        );
    }

    public void testIVFQueryRejectsZeroOrNegativeKRequest() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new IVFKnnFloatVectorQuery("f", new float[] { 0f, 1f }, 10, 10, null, 0.05f, false, 0, Float.NaN)
        );
    }

    public void testPerLeafCollectorKMatchesPersistedDistinctOversampleAcrossSegments() throws IOException {
        float o0 = 2.0f;
        float o1 = 5.0f;
        try (
            Directory dir = newDirectory();
            DirectoryReader reader = ESNextRescoreOversampleTestFixture.buildTwoCommitsTwoSegments(
                dir,
                random(),
                8,
                16,
                o0,
                o1,
                IvfMergeConfigResolver.useCodecDefault()
            )
        ) {
            ESNextRescoreOversampleTestFixture.assertLeafOversamples(reader, o0, o1);
            int kRequest = 10;
            float fallback = 1.25f;
            int maxK = 200;
            for (LeafReaderContext ctx : reader.leaves()) {
                float persisted = ESNextRescoreOversampleTestFixture.persistedOversampleOnLeaf(ctx.reader());
                int expect = AbstractIVFKnnVectorQuery.resolveCollectorK(persisted, kRequest, fallback, maxK);
                assertEquals(
                    expect,
                    AbstractIVFKnnVectorQuery.perSegmentCollectorK(
                        ctx,
                        ESNextRescoreOversampleTestFixture.FIELD_NAME,
                        kRequest,
                        fallback,
                        maxK
                    )
                );
            }
        }
    }

    public void testMergePersistsRescoreOversampleFromMergeConfigResolver() throws IOException {
        IvfMergeConfigResolver resolver = (fieldInfo, mergeState, codecDefault) -> new IvfSegmentConfig(
            codecDefault.quantEncoding(),
            codecDefault.usePrecondition(),
            7.75f
        );
        try (
            Directory dir = newDirectory();
            DirectoryReader reader = ESNextRescoreOversampleTestFixture.buildTwoLeavesThenMergedOneSegment(
                dir,
                random(),
                8,
                12,
                2.0f,
                5.5f,
                resolver,
                7.75f
            )
        ) {
            assertEquals(1, reader.leaves().size());
        }
    }

    public void testIVFQueryRunsWithPerSegmentParametersOnNextFormat() throws IOException {
        // End-to-end smoke: a per-segment-configured IVFKnnFloatVectorQuery rewrites cleanly on a
        // real ESNextDiskBBQVectorsFormat index and returns at most the requested top-k results.
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = newIndexWriterConfig();
            iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(new ESNextDiskBBQVectorsFormat(128, 4, null)));
            try (IndexWriter w = new IndexWriter(dir, iwc)) {
                for (int i = 0; i < 64; i++) {
                    Document d = new Document();
                    d.add(new KnnFloatVectorField("f", randomUnitVector(8)));
                    w.addDocument(d);
                }
                w.commit();
            }
            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                int kRequest = 5;
                int adjustedK = 15; // simulate oversampleMax = 3
                IVFKnnFloatVectorQuery query = new IVFKnnFloatVectorQuery(
                    "f",
                    randomUnitVector(8),
                    adjustedK,
                    adjustedK,
                    null,
                    0.05f,
                    false,
                    kRequest,
                    Float.NaN
                );
                Query rewritten = query.rewrite(searcher);
                assertNotNull(rewritten);
                // Search for the full adjustedK and verify we don't blow past it.
                assertTrue(searcher.search(query, adjustedK).scoreDocs.length <= adjustedK);
            }
        }
    }

    private static float[] randomUnitVector(int dims) {
        float[] v = new float[dims];
        for (int i = 0; i < dims; i++) {
            v[i] = random().nextFloat();
        }
        VectorUtil.l2normalize(v);
        return v;
    }
}
