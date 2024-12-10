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
package org.elasticsearch.index.codec.vectors.es816;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene100.Lucene100Codec;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.util.SameThreadExecutorService;
import org.elasticsearch.common.logging.LogConfigurator;

import java.util.Arrays;
import java.util.Locale;

import static java.lang.String.format;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

public class ES816HnswBinaryQuantizedVectorsFormatTests extends BaseKnnVectorsFormatTestCase {

    static {
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    @Override
    protected Codec getCodec() {
        return new Lucene100Codec() {
            @Override
            public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                return new ES816HnswBinaryQuantizedRWVectorsFormat();
            }
        };
    }

    public void testToString() {
        FilterCodec customCodec = new FilterCodec("foo", Codec.getDefault()) {
            @Override
            public KnnVectorsFormat knnVectorsFormat() {
                return new ES816HnswBinaryQuantizedVectorsFormat(10, 20, 1, null);
            }
        };
        String expectedPattern =
            "ES816HnswBinaryQuantizedVectorsFormat(name=ES816HnswBinaryQuantizedVectorsFormat, maxConn=10, beamWidth=20,"
                + " flatVectorFormat=ES816BinaryQuantizedVectorsFormat(name=ES816BinaryQuantizedVectorsFormat,"
                + " flatVectorScorer=ES816BinaryFlatVectorsScorer(nonQuantizedDelegate=%s())))";

        var defaultScorer = format(Locale.ROOT, expectedPattern, "DefaultFlatVectorScorer");
        var memSegScorer = format(Locale.ROOT, expectedPattern, "Lucene99MemorySegmentFlatVectorsScorer");
        assertThat(customCodec.knnVectorsFormat().toString(), is(oneOf(defaultScorer, memSegScorer)));
    }

    public void testSingleVectorCase() throws Exception {
        float[] vector = randomVector(random().nextInt(12, 500));
        for (VectorSimilarityFunction similarityFunction : VectorSimilarityFunction.values()) {
            try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("f", vector, similarityFunction));
                w.addDocument(doc);
                w.commit();
                try (IndexReader reader = DirectoryReader.open(w)) {
                    LeafReader r = getOnlyLeafReader(reader);
                    FloatVectorValues vectorValues = r.getFloatVectorValues("f");
                    KnnVectorValues.DocIndexIterator docIndexIterator = vectorValues.iterator();
                    assert (vectorValues.size() == 1);
                    while (docIndexIterator.nextDoc() != NO_MORE_DOCS) {
                        assertArrayEquals(vector, vectorValues.vectorValue(docIndexIterator.index()), 0.00001f);
                    }
                    TopDocs td = r.searchNearestVectors("f", randomVector(vector.length), 1, null, Integer.MAX_VALUE);
                    assertEquals(1, td.totalHits.value());
                    assertTrue(td.scoreDocs[0].score >= 0);
                }
            }
        }
    }

    public void testLimits() {
        expectThrows(IllegalArgumentException.class, () -> new ES816HnswBinaryQuantizedVectorsFormat(-1, 20));
        expectThrows(IllegalArgumentException.class, () -> new ES816HnswBinaryQuantizedVectorsFormat(0, 20));
        expectThrows(IllegalArgumentException.class, () -> new ES816HnswBinaryQuantizedVectorsFormat(20, 0));
        expectThrows(IllegalArgumentException.class, () -> new ES816HnswBinaryQuantizedVectorsFormat(20, -1));
        expectThrows(IllegalArgumentException.class, () -> new ES816HnswBinaryQuantizedVectorsFormat(512 + 1, 20));
        expectThrows(IllegalArgumentException.class, () -> new ES816HnswBinaryQuantizedVectorsFormat(20, 3201));
        expectThrows(
            IllegalArgumentException.class,
            () -> new ES816HnswBinaryQuantizedVectorsFormat(20, 100, 1, new SameThreadExecutorService())
        );
    }

    // Ensures that all expected vector similarity functions are translatable in the format.
    public void testVectorSimilarityFuncs() {
        // This does not necessarily have to be all similarity functions, but
        // differences should be considered carefully.
        var expectedValues = Arrays.stream(VectorSimilarityFunction.values()).toList();
        assertEquals(Lucene99HnswVectorsReader.SIMILARITY_FUNCTIONS, expectedValues);
    }
}
