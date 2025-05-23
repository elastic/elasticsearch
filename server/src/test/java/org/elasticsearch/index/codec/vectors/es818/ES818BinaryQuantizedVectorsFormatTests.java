/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2024 Elasticsearch B.V.
 */
package org.elasticsearch.index.codec.vectors.es818;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene912.Lucene912Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.index.codec.vectors.BQVectorUtils;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;

import java.io.IOException;
import java.util.Locale;

import static java.lang.String.format;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

public class ES818BinaryQuantizedVectorsFormatTests extends BaseKnnVectorsFormatTestCase {

    static {
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    @Override
    protected Codec getCodec() {
        return new Lucene912Codec() {
            @Override
            public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                return new ES818BinaryQuantizedVectorsFormat();
            }
        };
    }

    public void testSearch() throws Exception {
        String fieldName = "field";
        int numVectors = random().nextInt(99, 500);
        int dims = random().nextInt(4, 65);
        float[] vector = randomVector(dims);
        VectorSimilarityFunction similarityFunction = randomSimilarity();
        KnnFloatVectorField knnField = new KnnFloatVectorField(fieldName, vector, similarityFunction);
        IndexWriterConfig iwc = newIndexWriterConfig();
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, iwc)) {
                for (int i = 0; i < numVectors; i++) {
                    Document doc = new Document();
                    knnField.setVectorValue(randomVector(dims));
                    doc.add(knnField);
                    w.addDocument(doc);
                }
                w.commit();

                try (IndexReader reader = DirectoryReader.open(w)) {
                    IndexSearcher searcher = new IndexSearcher(reader);
                    final int k = random().nextInt(5, 50);
                    float[] queryVector = randomVector(dims);
                    Query q = new KnnFloatVectorQuery(fieldName, queryVector, k);
                    TopDocs collectedDocs = searcher.search(q, k);
                    assertEquals(k, collectedDocs.totalHits.value);
                    assertEquals(TotalHits.Relation.EQUAL_TO, collectedDocs.totalHits.relation);
                }
            }
        }
    }

    public void testToString() {
        FilterCodec customCodec = new FilterCodec("foo", Codec.getDefault()) {
            @Override
            public KnnVectorsFormat knnVectorsFormat() {
                return new ES818BinaryQuantizedVectorsFormat();
            }
        };
        String expectedPattern = "ES818BinaryQuantizedVectorsFormat("
            + "name=ES818BinaryQuantizedVectorsFormat, "
            + "flatVectorScorer=ES818BinaryFlatVectorsScorer(nonQuantizedDelegate=%s()))";
        var defaultScorer = format(Locale.ROOT, expectedPattern, "DefaultFlatVectorScorer");
        var memSegScorer = format(Locale.ROOT, expectedPattern, "Lucene99MemorySegmentFlatVectorsScorer");
        assertThat(customCodec.knnVectorsFormat().toString(), is(oneOf(defaultScorer, memSegScorer)));
    }

    @Override
    public void testRandomWithUpdatesAndGraph() {
        // graph not supported
    }

    @Override
    public void testSearchWithVisitedLimit() {
        // visited limit is not respected, as it is brute force search
    }

    public void testQuantizedVectorsWriteAndRead() throws IOException {
        String fieldName = "field";
        int numVectors = random().nextInt(99, 500);
        int dims = random().nextInt(4, 65);

        float[] vector = randomVector(dims);
        VectorSimilarityFunction similarityFunction = randomSimilarity();
        KnnFloatVectorField knnField = new KnnFloatVectorField(fieldName, vector, similarityFunction);
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
                for (int i = 0; i < numVectors; i++) {
                    Document doc = new Document();
                    knnField.setVectorValue(randomVector(dims));
                    doc.add(knnField);
                    w.addDocument(doc);
                    if (i % 101 == 0) {
                        w.commit();
                    }
                }
                w.commit();
                w.forceMerge(1);

                try (IndexReader reader = DirectoryReader.open(w)) {
                    LeafReader r = getOnlyLeafReader(reader);
                    FloatVectorValues vectorValues = r.getFloatVectorValues(fieldName);
                    assertEquals(vectorValues.size(), numVectors);
                    BinarizedByteVectorValues qvectorValues = ((ES818BinaryQuantizedVectorsReader.BinarizedVectorValues) vectorValues)
                        .getQuantizedVectorValues();
                    float[] centroid = qvectorValues.getCentroid();
                    assertEquals(centroid.length, dims);

                    OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(similarityFunction);
                    byte[] quantizedVector = new byte[dims];
                    byte[] expectedVector = new byte[BQVectorUtils.discretize(dims, 64) / 8];
                    if (similarityFunction == VectorSimilarityFunction.COSINE) {
                        vectorValues = new ES818BinaryQuantizedVectorsWriter.NormalizedFloatVectorValues(vectorValues);
                    }
                    while (vectorValues.nextDoc() != NO_MORE_DOCS) {
                        OptimizedScalarQuantizer.QuantizationResult corrections = quantizer.scalarQuantize(
                            vectorValues.vectorValue(),
                            quantizedVector,
                            (byte) 1,
                            centroid
                        );
                        BQVectorUtils.packAsBinary(quantizedVector, expectedVector);
                        assertArrayEquals(expectedVector, qvectorValues.vectorValue());
                        assertEquals(corrections, qvectorValues.getCorrectiveTerms());
                    }
                }
            }
        }
    }
}
