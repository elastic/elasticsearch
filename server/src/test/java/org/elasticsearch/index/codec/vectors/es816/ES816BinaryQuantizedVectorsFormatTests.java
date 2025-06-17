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
import org.apache.lucene.codecs.lucene912.Lucene912Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SoftDeletesRetentionMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.CheckJoinIndex;
import org.apache.lucene.search.join.DiversifyingChildrenFloatKnnVectorQuery;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.index.codec.vectors.BQVectorUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static java.lang.String.format;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

public class ES816BinaryQuantizedVectorsFormatTests extends BaseKnnVectorsFormatTestCase {

    static {
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    @Override
    protected Codec getCodec() {
        return new Lucene912Codec() {
            @Override
            public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                return new ES816BinaryQuantizedRWVectorsFormat();
            }
        };
    }

    static String encodeInts(int[] i) {
        return Arrays.toString(i);
    }

    static BitSetProducer parentFilter(IndexReader r) throws IOException {
        // Create a filter that defines "parent" documents in the index
        BitSetProducer parentsFilter = new QueryBitSetProducer(new TermQuery(new Term("docType", "_parent")));
        CheckJoinIndex.check(r, parentsFilter);
        return parentsFilter;
    }

    Document makeParent(int[] children) {
        Document parent = new Document();
        parent.add(newStringField("docType", "_parent", Field.Store.NO));
        parent.add(newStringField("id", encodeInts(children), Field.Store.YES));
        return parent;
    }

    public void testEmptyDiversifiedChildSearch() throws Exception {
        String fieldName = "field";
        int dims = random().nextInt(4, 65);
        float[] vector = randomVector(dims);
        VectorSimilarityFunction similarityFunction = VectorSimilarityFunction.EUCLIDEAN;
        try (Directory d = newDirectory()) {
            IndexWriterConfig iwc = newIndexWriterConfig().setCodec(codec);
            iwc.setMergePolicy(new SoftDeletesRetentionMergePolicy("soft_delete", MatchAllDocsQuery::new, iwc.getMergePolicy()));
            try (IndexWriter w = new IndexWriter(d, iwc)) {
                List<Document> toAdd = new ArrayList<>();
                for (int j = 1; j <= 5; j++) {
                    Document doc = new Document();
                    doc.add(new KnnFloatVectorField(fieldName, vector, similarityFunction));
                    doc.add(newStringField("id", Integer.toString(j), Field.Store.YES));
                    toAdd.add(doc);
                }
                toAdd.add(makeParent(new int[] { 1, 2, 3, 4, 5 }));
                w.addDocuments(toAdd);
                w.addDocuments(List.of(makeParent(new int[] { 6, 7, 8, 9, 10 })));
                w.deleteDocuments(new FieldExistsQuery(fieldName), new TermQuery(new Term("id", encodeInts(new int[] { 1, 2, 3, 4, 5 }))));
                w.flush();
                w.commit();
                w.forceMerge(1);
                try (IndexReader reader = DirectoryReader.open(w)) {
                    IndexSearcher searcher = new IndexSearcher(reader);
                    BitSetProducer parentFilter = parentFilter(searcher.getIndexReader());
                    Query query = new DiversifyingChildrenFloatKnnVectorQuery(fieldName, vector, null, 1, parentFilter);
                    assertTrue(searcher.search(query, 1).scoreDocs.length == 0);
                }
            }

        }
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
                return new ES816BinaryQuantizedVectorsFormat();
            }
        };
        String expectedPattern = "ES816BinaryQuantizedVectorsFormat("
            + "name=ES816BinaryQuantizedVectorsFormat, "
            + "flatVectorScorer=ES816BinaryFlatVectorsScorer(nonQuantizedDelegate=%s()))";
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
                    OffHeapBinarizedVectorValues qvectorValues = ((ES816BinaryQuantizedVectorsReader.BinarizedVectorValues) vectorValues)
                        .getQuantizedVectorValues();
                    float[] centroid = qvectorValues.getCentroid();
                    assertEquals(centroid.length, dims);

                    int descritizedDimension = BQVectorUtils.discretize(dims, 64);
                    BinaryQuantizer quantizer = new BinaryQuantizer(dims, descritizedDimension, similarityFunction);
                    byte[] expectedVector = new byte[BQVectorUtils.discretize(dims, 64) / 8];
                    if (similarityFunction == VectorSimilarityFunction.COSINE) {
                        vectorValues = new ES816BinaryQuantizedVectorsWriter.NormalizedFloatVectorValues(vectorValues);
                    }

                    while (vectorValues.nextDoc() != NO_MORE_DOCS) {
                        float[] corrections = quantizer.quantizeForIndex(vectorValues.vectorValue(), expectedVector, centroid);
                        assertArrayEquals(expectedVector, qvectorValues.vectorValue());
                        assertEquals(corrections.length, qvectorValues.getCorrectiveTerms().length);
                        for (int i = 0; i < corrections.length; i++) {
                            assertEquals(corrections[i], qvectorValues.getCorrectiveTerms()[i], 0.00001f);
                        }
                    }
                }
            }
        }
    }
}
