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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskASHVectorsFormat.MIN_VECTORS_PER_CLUSTER;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

/**
 * Tests for {@link ESNextDiskASHVectorsFormat}.
 */
public class ESNextDiskASHVectorsFormatTests extends ESTestCase {

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

    public void testAshEuclideanScoreCorrectness() throws IOException {
        int dimensions = 64;
        int numDocs = 200;
        int k = 10;
        Codec ashCodec = TestUtil.alwaysKnnVectorsFormat(ashTestFormat());
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = newIndexWriterConfig();
            iwc.setCodec(ashCodec);
            float[][] storedVectors = new float[numDocs][];
            try (IndexWriter w = new IndexWriter(dir, iwc)) {
                for (int i = 0; i < numDocs; i++) {
                    Document doc = new Document();
                    float[] vec = randomVector(dimensions);
                    storedVectors[i] = vec;
                    doc.add(new KnnFloatVectorField("f", vec, VectorSimilarityFunction.EUCLIDEAN));
                    doc.add(new StoredField("id", i));
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
                            k,
                            AcceptDocs.fromLiveDocs(leafReader.getLiveDocs(), leafReader.maxDoc()),
                            Integer.MAX_VALUE
                        );
                        assertThat(topDocs.scoreDocs, arrayWithSize(Math.min(leafReader.maxDoc(), k)));

                        // Compute brute-force exact EUCLIDEAN scores for all docs
                        float[] exactScores = new float[numDocs];
                        for (int i = 0; i < numDocs; i++) {
                            float sqDist = 0;
                            for (int d = 0; d < dimensions; d++) {
                                float diff = query[d] - storedVectors[i][d];
                                sqDist += diff * diff;
                            }
                            exactScores[i] = 1f / (1f + sqDist);
                        }

                        // Verify each ASH result score is within relative error tolerance of exact score
                        for (ScoreDoc sd : topDocs.scoreDocs) {
                            float exact = exactScores[sd.doc];
                            float relError = Math.abs(sd.score - exact) / Math.max(exact, 1e-6f);
                            assertThat(
                                "EUCLIDEAN score for doc " + sd.doc + ": ASH=" + sd.score + " exact=" + exact,
                                (double) relError,
                                lessThan(0.4)
                            );
                        }

                        // Verify the ASH top-1 is within the true top-50 (recall sanity check)
                        int ashTop1Doc = topDocs.scoreDocs[0].doc;
                        float[] sortedExact = exactScores.clone();
                        java.util.Arrays.sort(sortedExact);
                        float threshold = sortedExact[numDocs - 50]; // 50th best exact score
                        assertThat(
                            "ASH top-1 doc " + ashTop1Doc + " should be in true top-50",
                            exactScores[ashTop1Doc],
                            greaterThanOrEqualTo(threshold)
                        );
                    }
                }
            }
        }
    }

    private static float[] randomVector(int dims) {
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
}
