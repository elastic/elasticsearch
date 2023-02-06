/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.common.lucene.LuceneTests;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class VectorSimilarityQueryTests extends ESTestCase {

    public void testSimpleEuclidean() throws Exception {
        try (Directory d = newDirectory()) {
            try (IndexWriter w = new IndexWriter(d, new IndexWriterConfig())) {
                Document document = new Document();
                KnnFloatVectorField vectorField = new KnnFloatVectorField("float_vector", new float[] { 1, 1, 1 });
                document.add(vectorField);
                w.addDocument(document);
                vectorField.setVectorValue(new float[] { 2, 1, 1 });
                w.addDocument(document);
                vectorField.setVectorValue(new float[] { 1, 2, 1 });
                w.addDocument(document);
                vectorField.setVectorValue(new float[] { 1, 1, 2 });
                w.addDocument(document);
                vectorField.setVectorValue(new float[] { 2, 2, 2 });
                w.addDocument(document);

                w.commit();
            }
            try (IndexReader reader = DirectoryReader.open(d)) {
                IndexSearcher searcher = LuceneTests.newSearcher(reader);
                // Should match all, worst distance is 3
                TopDocs docs = searcher.search(
                    new VectorSimilarityQuery(new KnnFloatVectorQuery("float_vector", new float[] { 1, 1, 1 }, 5), 3f, 0.25f),
                    5
                );
                assertThat(docs.totalHits.value, equalTo(5L));

                // Should match only 4
                docs = searcher.search(
                    new VectorSimilarityQuery(new KnnFloatVectorQuery("float_vector", new float[] { 1, 1, 1 }, 5), 1f, 0.5f),
                    5
                );
                assertThat(docs.totalHits.value, equalTo(4L));
            }
        }
    }

    public void testSimpleCosine() throws IOException {
        try (Directory d = newDirectory()) {
            try (IndexWriter w = new IndexWriter(d, new IndexWriterConfig())) {
                Document document = new Document();
                KnnFloatVectorField vectorField = new KnnFloatVectorField(
                    "float_vector",
                    new float[] { 1, 1, 1 },
                    VectorSimilarityFunction.COSINE
                );
                document.add(vectorField);
                w.addDocument(document);
                vectorField.setVectorValue(new float[] { 2, 1, 1 });
                w.addDocument(document);
                vectorField.setVectorValue(new float[] { 1, 2, 1 });
                w.addDocument(document);
                vectorField.setVectorValue(new float[] { 1, 1, 2 });
                w.addDocument(document);
                vectorField.setVectorValue(new float[] { 2, 0, 2 });
                w.addDocument(document);

                w.commit();
            }
            try (IndexReader reader = DirectoryReader.open(d)) {
                IndexSearcher searcher = LuceneTests.newSearcher(reader);
                // Should match all actually worse distance is
                TopDocs docs = searcher.search(
                    new VectorSimilarityQuery(new KnnFloatVectorQuery("float_vector", new float[] { 1, 1, 1 }, 5), .8f, .9f),
                    5
                );
                assertThat(docs.totalHits.value, equalTo(5L));

                // Should match only 4
                docs = searcher.search(
                    new VectorSimilarityQuery(new KnnFloatVectorQuery("float_vector", new float[] { 1, 1, 1 }, 5), .9f, 0.95f),
                    5
                );
                assertThat(docs.totalHits.value, equalTo(4L));
            }
        }
    }

    public void testExplain() throws IOException {
        try (Directory d = newDirectory()) {
            try (IndexWriter w = new IndexWriter(d, new IndexWriterConfig())) {
                Document document = new Document();
                KnnFloatVectorField vectorField = new KnnFloatVectorField("float_vector", new float[] { 1, 1, 1 });
                document.add(vectorField);
                w.addDocument(document);
                vectorField.setVectorValue(new float[] { 2, 1, 1 });
                w.addDocument(document);
                vectorField.setVectorValue(new float[] { 1, 2, 1 });
                w.addDocument(document);
                vectorField.setVectorValue(new float[] { 1, 1, 2 });
                w.addDocument(document);
                vectorField.setVectorValue(new float[] { 2, 2, 2 });
                w.addDocument(document);
            }
            try (IndexReader reader = DirectoryReader.open(d)) {
                IndexSearcher searcher = LuceneTests.newSearcher(reader);
                Query q = searcher.rewrite(
                    new VectorSimilarityQuery(new KnnFloatVectorQuery("float_vector", new float[] { 1, 1, 1 }, 5), 1f, 0.5f)
                );
                Weight w = q.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
                Explanation ex = w.explain(searcher.getIndexReader().leaves().get(0), 1);
                assertTrue(ex.isMatch());

                ex = w.explain(searcher.getIndexReader().leaves().get(0), 4);
                assertFalse(ex.isMatch());
            }
        }
    }

    public void testDumb() {
        float v = VectorSimilarityFunction.EUCLIDEAN.compare(new byte[] { 127, -128, 0, 10, -1 }, new byte[] { 127, -128, 0, 1, -1 });
        float v2 = VectorUtil.squareDistance(new byte[] { 127, -128, 0, 10, -1 }, new byte[] { 127, -128, 0, 1, -1 });
        assertTrue(false);
    }

}
