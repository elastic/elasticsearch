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
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.hamcrest.Matchers.equalTo;

public class RescoreKnnVectorQueryTests extends ESTestCase {

    public static final String FIELD_NAME = "float_vector";

    public void testRescoresTopK() throws Exception {
        int numDocs = randomIntBetween(10, 100);
        testRescoreDocs(numDocs, randomIntBetween(5, numDocs - 1));
    }

    public void testRescoresNoKParameter() throws Exception {
        testRescoreDocs(randomIntBetween(10, 100), null);
    }

    private void testRescoreDocs(int numDocs, Integer k) throws Exception {
        int numDims = randomIntBetween(5, 100);

        if (k == null) {
            k = numDocs;
        }

        try (Directory d = newDirectory()) {
            try (IndexWriter w = new IndexWriter(d, newIndexWriterConfig())) {
                for (int i = 0; i < numDocs; i++) {
                    Document document = new Document();
                    float[] vector = randomVector(numDims);
                    KnnFloatVectorField vectorField = new KnnFloatVectorField(FIELD_NAME, vector);
                    document.add(vectorField);
                    w.addDocument(document);
                }
                w.commit();
                w.forceMerge(1);
            }

            try (IndexReader reader = DirectoryReader.open(d)) {
                float[] queryVector = randomVector(numDims);

                RescoreKnnVectorQuery rescoreKnnVectorQuery = new RescoreKnnVectorQuery(
                    FIELD_NAME,
                    queryVector,
                    VectorSimilarityFunction.COSINE,
                    k,
                    new MatchAllDocsQuery()
                );

                IndexSearcher searcher = newSearcher(reader, true, false);
                TopDocs docs = searcher.search(rescoreKnnVectorQuery, numDocs);
                Map<Integer, Float> rescoredDocs = Arrays.stream(docs.scoreDocs)
                    .collect(Collectors.toMap(scoreDoc -> scoreDoc.doc, scoreDoc -> scoreDoc.score));

                assertThat(rescoredDocs.size(), equalTo(k));

                Collection<Float> rescoredScores = new ArrayList<>(rescoredDocs.values());
                PriorityQueue<Float> topK = new PriorityQueue<>((o1, o2) -> Float.compare(o2, o1));

                for (LeafReaderContext leafReaderContext : reader.leaves()) {
                    FloatVectorValues floatVectorValues = leafReaderContext.reader().getFloatVectorValues(FIELD_NAME);
                    KnnVectorValues.DocIndexIterator iterator = floatVectorValues.iterator();
                    while (iterator.nextDoc() != NO_MORE_DOCS) {
                        float[] vector = floatVectorValues.vectorValue(iterator.index());
                        float score = VectorSimilarityFunction.COSINE.compare(queryVector, vector);
                        topK.add(score);
                        int docId = iterator.docID();
                        if (rescoredDocs.containsKey(docId)) {
                            assertThat(rescoredDocs.get(docId), equalTo(score));
                            rescoredDocs.remove(docId);
                        }
                    }
                }

                assertThat(rescoredDocs.size(), equalTo(0));

                // Check top scoring docs are contained in rescored docs
                for (int i = 0; i < k; i++) {
                    Float topScore = topK.poll();
                    if (rescoredScores.contains(topScore) == false) {
                        fail("Top score " + topScore + " not contained in rescored doc scores " + rescoredScores);
                    }
                }
            }
        }
    }

    private static float[] randomVector(int numDims) {
        float[] vector = new float[numDims];
        for (int j = 0; j < numDims; j++) {
            vector[j] = randomFloatBetween(0, 1, true);
        }
        return vector;
    }

}
