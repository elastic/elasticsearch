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
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.lucene.LuceneTests;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

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
                assertThat(docs.totalHits.value(), equalTo(5L));

                // Should match only 4
                docs = searcher.search(
                    new VectorSimilarityQuery(new KnnFloatVectorQuery("float_vector", new float[] { 1, 1, 1 }, 5), 1f, 0.5f),
                    5
                );
                assertThat(docs.totalHits.value(), equalTo(4L));
            }
        }
    }

    public void testEuclideanInvariant() throws Exception {
        int dim = 4;
        int L = 1;
        int n = 100;
        String fieldName = "vector";
        Supplier<float[]> vectorValue = () -> new float[] { randomFloat(), randomFloat(), randomFloat(), randomFloat() };
        try (Directory d = newDirectory()) {
            try (IndexWriter w = new IndexWriter(d, new IndexWriterConfig())) {
                KnnFloatVectorField vectorField = new KnnFloatVectorField(fieldName, vectorValue.get());
                Document document = new Document();
                document.add(vectorField);
                for (int i = 0; i < n; i++) {
                    w.addDocument(document);
                    vectorField.setVectorValue(vectorValue.get());
                }
                w.commit();
            }
            try (IndexReader reader = DirectoryReader.open(d)) {
                float radius = 0.25f * (float) Math.sqrt(dim) * L;
                float score = 1f / (1f + radius);
                IndexSearcher searcher = LuceneTests.newSearcher(reader);
                for (int i = 0; i < 10; i++) {
                    TopDocs docs = searcher.search(
                        new VectorSimilarityQuery(new KnnFloatVectorQuery(fieldName, vectorValue.get(), n), radius, score),
                        n
                    );
                    for (ScoreDoc scoreDoc : docs.scoreDocs) {
                        float dist = (1 / scoreDoc.score) - 1;
                        assertThat(dist, lessThanOrEqualTo(radius));
                    }
                }
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
                assertThat(docs.totalHits.value(), equalTo(5L));

                // Should match only 4
                docs = searcher.search(
                    new VectorSimilarityQuery(new KnnFloatVectorQuery("float_vector", new float[] { 1, 1, 1 }, 5), .9f, 0.95f),
                    5
                );
                assertThat(docs.totalHits.value(), equalTo(4L));
            }
        }
    }

    public void testCosineInvariant() throws Exception {
        int dim = 4;
        int L = 1;
        int n = 100;
        String fieldName = "vector";
        Supplier<float[]> vectorValue = () -> new float[] { randomFloat(), randomFloat(), randomFloat(), randomFloat() };
        try (Directory d = newDirectory()) {
            try (IndexWriter w = new IndexWriter(d, new IndexWriterConfig())) {
                KnnFloatVectorField vectorField = new KnnFloatVectorField(fieldName, vectorValue.get(), VectorSimilarityFunction.COSINE);
                Document document = new Document();
                document.add(vectorField);
                for (int i = 0; i < n; i++) {
                    w.addDocument(document);
                    vectorField.setVectorValue(vectorValue.get());
                }
                w.commit();
            }
            try (IndexReader reader = DirectoryReader.open(d)) {
                float radius = 0.25f * (float) Math.sqrt(dim) * L;
                float cos = (float) Math.cos(radius);
                float score = (1 + radius) / 2f;
                IndexSearcher searcher = LuceneTests.newSearcher(reader);
                for (int i = 0; i < 10; i++) {
                    TopDocs docs = searcher.search(
                        new VectorSimilarityQuery(new KnnFloatVectorQuery(fieldName, vectorValue.get(), n), radius, score),
                        n
                    );
                    for (ScoreDoc scoreDoc : docs.scoreDocs) {
                        float dist = (2 * scoreDoc.score) - 1;
                        float distCos = (float) Math.cos(dist);
                        assertThat(scoreDoc.score, greaterThanOrEqualTo(score));
                        assertThat(distCos, lessThanOrEqualTo(cos));
                    }
                }
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

}
