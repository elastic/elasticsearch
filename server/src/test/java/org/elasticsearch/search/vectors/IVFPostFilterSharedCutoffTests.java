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
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.index.codec.vectors.cluster.BulkNeighborQueue;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidIndexFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfMergeConfigResolver;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfQueryConfigResolver;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfSegmentConfig;
import org.elasticsearch.index.codec.vectors.diskbbq.QuantEncoding;
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextRescoreOversampleTestFixture;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextRescoreOversampleTestFixture.FIELD_NAME;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.sameInstance;

/**
 * A leaf can retain candidates at or below the cutoff it shares with the other leaves, depending on when it adopted that
 * cutoff. A post-filter delegate hands its un-merged per-leaf candidates to {@link PostFilterKnnQuery}, where they would make
 * results unrepeatable and stand in for better matches, so it must not hand those over.
 */
public class IVFPostFilterSharedCutoffTests extends ESTestCase {

    public void testCandidatesAtOrBelowCutoffAreDropped() {
        long cutoff = BulkNeighborQueue.encodeRaw(7, 0.5f);
        TopDocs mixed = topDocs(new ScoreDoc(1, 0.9f), new ScoreDoc(2, 0.5f), new ScoreDoc(3, 0.1f), new ScoreDoc(4, 0.6f));
        TopDocs allAbove = topDocs(new ScoreDoc(5, 0.7f));
        TopDocs none = topDocs();

        TopDocs[] trimmed = AbstractIVFKnnVectorQuery.aboveSharedCutoff(new TopDocs[] { mixed, allAbove, none }, cutoff);

        assertThat(trimmed[0].scoreDocs, arrayWithSize(2));
        assertEquals(1, trimmed[0].scoreDocs[0].doc);
        assertEquals(4, trimmed[0].scoreDocs[1].doc);
        assertEquals(mixed.totalHits, trimmed[0].totalHits);
        assertThat(trimmed[1], sameInstance(allAbove));
        assertThat(trimmed[2], sameInstance(none));
    }

    public void testNothingDroppedWhenNoCutoffWasPublished() {
        TopDocs[] perLeaf = { topDocs(new ScoreDoc(1, Float.NEGATIVE_INFINITY), new ScoreDoc(2, -3f)) };
        assertThat(
            AbstractIVFKnnVectorQuery.aboveSharedCutoff(perLeaf, AbstractMaxScoreKnnCollector.LEAST_COMPETITIVE),
            sameInstance(perLeaf)
        );
    }

    /**
     * The first segment holds only vectors far from the query, the second only near ones. Without an executor the far
     * segment is searched first, before any cutoff exists, so it fills its queue; the near segment then publishes a cutoff
     * above all of them.
     */
    public void testPostFilterCandidatesScoreAboveSharedCutoff() throws IOException {
        int dims = 64;
        float[] queryVector = randomUnitVector(dims);
        float[] opposite = new float[dims];
        for (int i = 0; i < dims; i++) {
            opposite[i] = -queryVector[i];
        }
        IvfSegmentConfig config = IvfSegmentConfig.of(
            CentroidIndexFormat.FLAT,
            new IvfSegmentConfig.OsqConfig(QuantEncoding.ONE_BIT_4BIT_QUERY),
            false,
            3f
        );
        IndexWriterConfig iwc = new IndexWriterConfig().setCodec(
            ESNextRescoreOversampleTestFixture.createDiskBbqCodec(
                (state, fieldInfo) -> Optional.of(config),
                IvfMergeConfigResolver.useCodecDefault()
            )
        ).setMergePolicy(NoMergePolicy.INSTANCE);
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (float[] center : new float[][] { opposite, queryVector }) {
                    for (int i = 0; i < 2000; i++) {
                        Document doc = new Document();
                        doc.add(new KnnFloatVectorField(FIELD_NAME, near(center), VectorSimilarityFunction.EUCLIDEAN));
                        writer.addDocument(doc);
                    }
                    writer.commit();
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals(2, reader.leaves().size());
                AtomicReference<SharedMinCompetitiveScore> shared = new AtomicReference<>();
                var query = new IVFKnnFloatVectorQuery(
                    FIELD_NAME,
                    queryVector,
                    10,
                    500,
                    null,
                    1f,
                    IvfQueryConfigResolver.from(true, false, 4, 3f, null),
                    true
                ) {
                    @Override
                    protected IVFCollectorManager getKnnCollectorManager(int k, SharedMinCompetitiveScore sharedScore) {
                        shared.set(sharedScore);
                        return super.getKnnCollectorManager(k, sharedScore);
                    }
                };

                query.rewrite(new IndexSearcher(reader));

                assertNotNull("two leaves must share a cutoff", shared.get());
                long cutoff = shared.get().get();
                assertNotEquals("a leaf must have published a cutoff", AbstractMaxScoreKnnCollector.LEAST_COMPETITIVE, cutoff);
                float cutoffScore = BulkNeighborQueue.decodeScoreRaw(cutoff);

                List<String> atOrBelow = new ArrayList<>();
                int candidates = 0;
                ScoreDoc[][] perLeafCandidates = query.getPostFilterCandidates();
                for (int leaf = 0; leaf < perLeafCandidates.length; leaf++) {
                    if (perLeafCandidates[leaf] == null) {
                        continue;
                    }
                    for (ScoreDoc candidate : perLeafCandidates[leaf]) {
                        candidates++;
                        if (candidate.score <= cutoffScore) {
                            atOrBelow.add("leaf=" + leaf + " doc=" + candidate.doc + " score=" + candidate.score);
                        }
                    }
                }
                assertThat("the pool must not be empty", candidates, greaterThan(0));
                assertThat("candidates at or below the shared cutoff " + cutoffScore, atOrBelow, empty());
            }
        }
    }

    /** A unit vector close to {@code center}. */
    private static float[] near(float[] center) {
        float[] vector = new float[center.length];
        for (int i = 0; i < vector.length; i++) {
            vector[i] = center[i] + 0.05f * (randomFloat() - 0.5f);
        }
        VectorUtil.l2normalize(vector);
        return vector;
    }

    private static float[] randomUnitVector(int dims) {
        float[] vector = new float[dims];
        for (int i = 0; i < dims; i++) {
            vector[i] = randomFloat() - 0.5f;
        }
        VectorUtil.l2normalize(vector);
        return vector;
    }

    private static TopDocs topDocs(ScoreDoc... scoreDocs) {
        return new TopDocs(new TotalHits(scoreDocs.length, TotalHits.Relation.EQUAL_TO), scoreDocs);
    }
}
