/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene100.Lucene100Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.elasticsearch.common.logging.LogConfigurator;

import java.nio.file.Path;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

// @com.carrotsearch.randomizedtesting.annotations.Repeat(iterations = 50) // tests.directory sys property?
public class ES814HnswScalarQuantizedVectorsFormatTests extends BaseKnnVectorsFormatTestCase {

    static {
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    @Override
    protected Codec getCodec() {
        return new Lucene100Codec() {
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
        try (Directory dir = new MMapDirectory(root.resolve("dir1")); Directory dir2 = new MMapDirectory(root.resolve("dir2"))) {
            try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
                w.addDocument(doc);
            }
            try (IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig())) {
                w2.addIndexes(dir);
                w2.forceMerge(1);
                try (IndexReader reader = DirectoryReader.open(w2)) {
                    LeafReader r = getOnlyLeafReader(reader);
                    FloatVectorValues vectorValues = r.getFloatVectorValues(fieldName);
                    KnnVectorValues.DocIndexIterator iterator = vectorValues.iterator();
                    assertEquals(0, iterator.nextDoc());
                    assertEquals(0, vectorValues.vectorValue(iterator.index())[0], 0);
                    assertEquals(NO_MORE_DOCS, iterator.nextDoc());
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
        try (Directory dir = new MMapDirectory(root.resolve("dir1")); Directory dir2 = new MMapDirectory(root.resolve("dir2"))) {
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
                    KnnVectorValues.DocIndexIterator iterator = vectorValues.iterator();
                    assertEquals(0, iterator.nextDoc());
                    // The merge order is randomized, we might get 1 first, or 2
                    float value = vectorValues.vectorValue(iterator.index())[0];
                    assertTrue(value == 1 || value == 2);
                    assertEquals(1, iterator.nextDoc());
                    value += vectorValues.vectorValue(iterator.index())[0];
                    assertEquals(3f, value, 0);
                }
            }
        }
    }

    public void testSingleVectorPerSegmentCosine() throws Exception {
        testSingleVectorPerSegment(VectorSimilarityFunction.COSINE);
    }

    public void testSingleVectorPerSegmentDot() throws Exception {
        testSingleVectorPerSegment(VectorSimilarityFunction.DOT_PRODUCT);
    }

    public void testSingleVectorPerSegmentEuclidean() throws Exception {
        testSingleVectorPerSegment(VectorSimilarityFunction.EUCLIDEAN);
    }

    public void testSingleVectorPerSegmentMIP() throws Exception {
        testSingleVectorPerSegment(VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT);
    }

    private void testSingleVectorPerSegment(VectorSimilarityFunction sim) throws Exception {
        var codec = getCodec();
        try (Directory dir = new MMapDirectory(createTempDir().resolve("dir1"))) {
            try (IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig().setCodec(codec))) {
                Document doc2 = new Document();
                doc2.add(new KnnFloatVectorField("field", new float[] { 0.8f, 0.6f }, sim));
                doc2.add(newTextField("id", "A", Field.Store.YES));
                writer.addDocument(doc2);
                writer.commit();

                Document doc1 = new Document();
                doc1.add(new KnnFloatVectorField("field", new float[] { 0.6f, 0.8f }, sim));
                doc1.add(newTextField("id", "B", Field.Store.YES));
                writer.addDocument(doc1);
                writer.commit();

                Document doc3 = new Document();
                doc3.add(new KnnFloatVectorField("field", new float[] { -0.6f, -0.8f }, sim));
                doc3.add(newTextField("id", "C", Field.Store.YES));
                writer.addDocument(doc3);
                writer.commit();

                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReader leafReader = getOnlyLeafReader(reader);
                StoredFields storedFields = reader.storedFields();
                float[] queryVector = new float[] { 0.6f, 0.8f };
                var hits = leafReader.searchNearestVectors("field", queryVector, 3, null, 100);
                assertEquals(hits.scoreDocs.length, 3);
                assertEquals("B", storedFields.document(hits.scoreDocs[0].doc).get("id"));
                assertEquals("A", storedFields.document(hits.scoreDocs[1].doc).get("id"));
                assertEquals("C", storedFields.document(hits.scoreDocs[2].doc).get("id"));
            }
        }
    }
}
