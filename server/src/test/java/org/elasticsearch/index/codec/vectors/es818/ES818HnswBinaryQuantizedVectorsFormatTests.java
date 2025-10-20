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

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.index.codec.vectors.BaseHnswVectorsFormatTestCase;

import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.ExecutorService;

import static java.lang.String.format;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.oneOf;

public class ES818HnswBinaryQuantizedVectorsFormatTests extends BaseHnswVectorsFormatTestCase {

    @Override
    protected KnnVectorsFormat createFormat() {
        return new ES818HnswBinaryQuantizedVectorsFormat();
    }

    @Override
    protected KnnVectorsFormat createFormat(int maxConn, int beamWidth) {
        return new ES818HnswBinaryQuantizedVectorsFormat(maxConn, beamWidth);
    }

    @Override
    protected KnnVectorsFormat createFormat(int maxConn, int beamWidth, int numMergeWorkers, ExecutorService service) {
        return new ES818HnswBinaryQuantizedVectorsFormat(maxConn, beamWidth, numMergeWorkers, service);
    }

    public void testToString() {
        String expected = "ES818HnswBinaryQuantizedVectorsFormat"
            + "(name=ES818HnswBinaryQuantizedVectorsFormat, maxConn=10, beamWidth=20, flatVectorFormat=%s)";
        expected = format(
            Locale.ROOT,
            expected,
            "ES818BinaryQuantizedVectorsFormat(name=ES818BinaryQuantizedVectorsFormat, flatVectorScorer=%s)"
        );
        expected = format(Locale.ROOT, expected, "ES818BinaryFlatVectorsScorer(nonQuantizedDelegate=%s())");

        String defaultScorer = format(Locale.ROOT, expected, "DefaultFlatVectorScorer");
        String memSegScorer = format(Locale.ROOT, expected, "Lucene99MemorySegmentFlatVectorsScorer");

        KnnVectorsFormat format = createFormat(10, 20, 1, null);
        assertThat(format, hasToString(oneOf(defaultScorer, memSegScorer)));
    }

    public void testSingleVectorCase() throws Exception {
        float[] vector = randomVector(random().nextInt(12, 500));
        for (VectorSimilarityFunction similarityFunction : VectorSimilarityFunction.values()) {
            try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
                Document doc = new Document();
                if (similarityFunction == VectorSimilarityFunction.COSINE) {
                    VectorUtil.l2normalize(vector);
                }
                doc.add(new KnnFloatVectorField("f", vector, similarityFunction));
                w.addDocument(doc);
                w.commit();
                try (IndexReader reader = DirectoryReader.open(w)) {
                    LeafReader r = getOnlyLeafReader(reader);
                    FloatVectorValues vectorValues = r.getFloatVectorValues("f");
                    KnnVectorValues.DocIndexIterator docIndexIterator = vectorValues.iterator();
                    assertThat(vectorValues.size(), equalTo(1));
                    while (docIndexIterator.nextDoc() != NO_MORE_DOCS) {
                        assertArrayEquals(vector, vectorValues.vectorValue(docIndexIterator.index()), 0.00001f);
                    }
                    float[] randomVector = randomVector(vector.length);
                    if (similarityFunction == VectorSimilarityFunction.COSINE) {
                        VectorUtil.l2normalize(randomVector);
                    }
                    float trueScore = similarityFunction.compare(vector, randomVector);
                    TopDocs td = r.searchNearestVectors(
                        "f",
                        randomVector,
                        1,
                        AcceptDocs.fromLiveDocs(r.getLiveDocs(), r.maxDoc()),
                        Integer.MAX_VALUE
                    );
                    assertEquals(1, td.totalHits.value());
                    assertThat(td.scoreDocs[0].score, greaterThanOrEqualTo(0f));
                    // When it's the only vector in a segment, the score should be very close to the true score
                    assertEquals(trueScore, td.scoreDocs[0].score, 0.01f);
                }
            }
        }
    }

    public void testSimpleOffHeapSize() throws IOException {
        try (Directory dir = newDirectory()) {
            testSimpleOffHeapSizeImpl(dir, newIndexWriterConfig(), true);
        }
    }

    public void testSimpleOffHeapSizeMMapDir() throws IOException {
        try (Directory dir = newMMapDirectory()) {
            testSimpleOffHeapSizeImpl(dir, newIndexWriterConfig(), true);
        }
    }

    public void testSimpleOffHeapSizeImpl(Directory dir, IndexWriterConfig config, boolean expectVecOffHeap) throws IOException {
        float[] vector = randomVector(random().nextInt(12, 500));
        var matcher = expectVecOffHeap
            ? allOf(
                aMapWithSize(3),
                hasEntry("vex", 1L),
                hasEntry(equalTo("veb"), greaterThan(0L)),
                hasEntry("vec", (long) vector.length * Float.BYTES)
            )
            : allOf(aMapWithSize(2), hasEntry("vex", 1L), hasEntry(equalTo("veb"), greaterThan(0L)));

        testSimpleOffHeapSize(dir, config, vector, matcher);
    }

    static Directory newMMapDirectory() throws IOException {
        Directory dir = new MMapDirectory(createTempDir("ES818BinaryQuantizedVectorsFormatTests"));
        if (random().nextBoolean()) {
            dir = new MockDirectoryWrapper(random(), dir);
        }
        return dir;
    }
}
