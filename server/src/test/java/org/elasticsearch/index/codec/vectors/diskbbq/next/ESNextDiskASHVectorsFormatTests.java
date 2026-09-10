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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskASHVectorsFormat.MIN_VECTORS_PER_CLUSTER;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

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
