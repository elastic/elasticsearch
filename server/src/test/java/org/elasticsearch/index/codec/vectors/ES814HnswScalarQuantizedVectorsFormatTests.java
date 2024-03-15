/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.elasticsearch.common.logging.LogConfigurator;

import java.nio.file.Path;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

// @com.carrotsearch.randomizedtesting.annotations.Repeat(iterations = 50) // tests.directory sys property?
public class ES814HnswScalarQuantizedVectorsFormatTests extends BaseKnnVectorsFormatTestCase {

    static {
        LogConfigurator.configureESLogging(); // TODO: strange initialization issue if I don't enable this.
    }

    @Override
    protected Codec getCodec() {
        return new Lucene99Codec() {
            @Override
            public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                return new ES814HnswScalarQuantizedVectorsFormat();
            }
        };
    }

    // The following test scenarios are similar to their superclass namesakes,
    // but here we ensure that the Directory implementation is a FSDirectory
    // which helps test the native code vector distance implementation

    public void testAddIndexesDirectory0FS() throws Exception {
        Path root = createTempDir();
        String fieldName = "field";
        Document doc = new Document();
        doc.add(new KnnFloatVectorField(fieldName, new float[4], VectorSimilarityFunction.DOT_PRODUCT));
        try (Directory dir = newFSDirectory(root.resolve("dir1")); Directory dir2 = newFSDirectory(root.resolve("dir2"))) {
            try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
                w.addDocument(doc);
            }
            try (IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig())) {
                w2.addIndexes(dir);
                w2.forceMerge(1);
                try (IndexReader reader = DirectoryReader.open(w2)) {
                    LeafReader r = getOnlyLeafReader(reader);
                    FloatVectorValues vectorValues = r.getFloatVectorValues(fieldName);
                    assertEquals(0, vectorValues.nextDoc());
                    assertEquals(0, vectorValues.vectorValue()[0], 0);
                    assertEquals(NO_MORE_DOCS, vectorValues.nextDoc());
                }
            }
        }
    }

    public void testAddIndexesDirectory01FSCosine() throws Exception {
        testAddIndexesDirectory01FS(VectorSimilarityFunction.COSINE);
    }

    public void testAddIndexesDirectory01FSDot() throws Exception {
        testAddIndexesDirectory01FS(VectorSimilarityFunction.DOT_PRODUCT);
    }

    public void testAddIndexesDirectory01FSEuclidean() throws Exception {
        testAddIndexesDirectory01FS(VectorSimilarityFunction.EUCLIDEAN);
    }

    public void testAddIndexesDirectory01FSMaxIP() throws Exception {
        testAddIndexesDirectory01FS(VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT);
    }

    private void testAddIndexesDirectory01FS(VectorSimilarityFunction similarityFunction) throws Exception {
        Path root = createTempDir();
        String fieldName = "field";
        float[] vector = new float[] { 1f };
        Document doc = new Document();
        doc.add(new KnnFloatVectorField(fieldName, vector, similarityFunction));
        try (Directory dir = newFSDirectory(root.resolve("dir1")); Directory dir2 = newFSDirectory(root.resolve("dir2"))) {
            try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
                w.addDocument(doc);
            }
            try (IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig())) {
                vector[0] = 2f;
                w2.addDocument(doc);
                w2.addIndexes(dir);
                w2.forceMerge(1);
                try (IndexReader reader = DirectoryReader.open(w2)) {
                    LeafReader r = getOnlyLeafReader(reader);
                    FloatVectorValues vectorValues = r.getFloatVectorValues(fieldName);
                    assertEquals(0, vectorValues.nextDoc());
                    // The merge order is randomized, we might get 1 first, or 2
                    float value = vectorValues.vectorValue()[0];
                    assertTrue(value == 1 || value == 2);
                    assertEquals(1, vectorValues.nextDoc());
                    value += vectorValues.vectorValue()[0];
                    assertEquals(3f, value, 0);
                }
            }
        }
    }
}
