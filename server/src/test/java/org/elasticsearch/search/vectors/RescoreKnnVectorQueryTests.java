/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.hamcrest.Matchers.equalTo;

public class RescoreKnnVectorQueryTests extends ESTestCase {

    public static final String FIELD_NAME = "float_vector";
    private final int numDocs;
    private final VectorProvider vectorProvider;
    private final Integer k;

    public RescoreKnnVectorQueryTests(VectorProvider vectorProvider, boolean useK) {
        this.vectorProvider = vectorProvider;
        this.numDocs = randomIntBetween(10, 100);;
        this.k = useK ? randomIntBetween(1, numDocs - 1) : null;
    }

    public void testRescoreDocs() throws Exception {
        int numDims = randomIntBetween(5, 100);

        Integer adjustedK = k;
        if (k == null) {
            adjustedK = numDocs;
        }

        try (Directory d = newDirectory()) {
            addRandomDocuments(numDocs, d, numDims, vectorProvider);

            try (IndexReader reader = DirectoryReader.open(d)) {

                // Use a RescoreKnnVectorQuery with a match all query, to ensure we get scoring of 1 from the inner query
                // and thus we're rescoring the top k docs.
                VectorData queryVector = vectorProvider.randomVector(numDims);
                RescoreKnnVectorQuery rescoreKnnVectorQuery = vectorProvider.createRescoreQuery(queryVector, adjustedK);

                IndexSearcher searcher = newSearcher(reader, true, false);
                TopDocs docs = searcher.search(rescoreKnnVectorQuery, numDocs);
                Map<Integer, Float> rescoredDocs = Arrays.stream(docs.scoreDocs)
                    .collect(Collectors.toMap(scoreDoc -> scoreDoc.doc, scoreDoc -> scoreDoc.score));

                assertThat(rescoredDocs.size(), equalTo(adjustedK));

                Collection<Float> rescoredScores = new HashSet<>(rescoredDocs.values());

                // Collect all docs sequentially, and score them using the similarity function to get the top K scores
                PriorityQueue<Float> topK = new PriorityQueue<>((o1, o2) -> Float.compare(o2, o1));

                for (LeafReaderContext leafReaderContext : reader.leaves()) {
                    KnnVectorValues vectorValues = vectorProvider.vectorValues(leafReaderContext.reader());
                    KnnVectorValues.DocIndexIterator iterator = vectorValues.iterator();
                    while (iterator.nextDoc() != NO_MORE_DOCS) {
                        VectorData vectorData = vectorProvider.dataVectorForDoc(vectorValues, iterator.docID());
                        float score = vectorProvider.score(queryVector, vectorData);
                        topK.add(score);
                        int docId = iterator.docID();
                        // If the doc has been retrieved from the RescoreKnnVectorQuery, check the score is the same and remove it
                        // to ensure we found them all
                        if (rescoredDocs.containsKey(docId)) {
                            assertThat(rescoredDocs.get(docId), equalTo(score));
                            rescoredDocs.remove(docId);
                        }
                    }
                }

                assertThat(rescoredDocs.size(), equalTo(0));

                // Check top scoring docs are contained in rescored docs
                for (int i = 0; i < adjustedK; i++) {
                    Float topScore = topK.poll();
                    if (rescoredScores.contains(topScore) == false) {
                        fail("Top score " + topScore + " not contained in rescored doc scores " + rescoredScores);
                    }
                }
            }
        }
    }

    private interface VectorProvider {
        VectorData randomVector(int numDimensions);

        RescoreKnnVectorQuery createRescoreQuery(VectorData queryVector, Integer k);

        KnnVectorValues vectorValues(LeafReader leafReader) throws IOException;

        void addVectorField(Document document, VectorData vector);

        VectorData dataVectorForDoc(KnnVectorValues vectorValues, int docId) throws IOException;

        float score(VectorData queryVector, VectorData dataVector);
    }

    private static class FloatVectorProvider implements VectorProvider {
        @Override
        public VectorData randomVector(int numDimensions) {
            float[] vector = new float[numDimensions];
            for (int j = 0; j < numDimensions; j++) {
                vector[j] = randomFloatBetween(0, 1, true);
            }
            return VectorData.fromFloats(vector);
        }

        @Override
        public RescoreKnnVectorQuery createRescoreQuery(VectorData queryVector, Integer k) {
            return new RescoreKnnVectorQuery(
                FIELD_NAME,
                queryVector.floatVector(),
                VectorSimilarityFunction.COSINE,
                k,
                new MatchAllDocsQuery()
            );
        }

        @Override
        public KnnVectorValues vectorValues(LeafReader leafReader) throws IOException {
            return leafReader.getFloatVectorValues(FIELD_NAME);
        }

        @Override
        public void addVectorField(Document document, VectorData vector) {
            KnnFloatVectorField vectorField = new KnnFloatVectorField(FIELD_NAME, vector.floatVector());
            document.add(vectorField);
        }

        @Override
        public VectorData dataVectorForDoc(KnnVectorValues vectorValues, int docId) throws IOException {
            return VectorData.fromFloats(((FloatVectorValues)vectorValues).vectorValue(docId));
        }

        @Override
        public float score(VectorData queryVector, VectorData dataVector) {
            return VectorSimilarityFunction.COSINE.compare(queryVector.floatVector(), dataVector.floatVector());
        }
    }

    private static class ByteVectorProvider implements VectorProvider {
        @Override
        public VectorData randomVector(int numDimensions) {
            byte[] vector = new byte[numDimensions];
            for (int j = 0; j < numDimensions; j++) {
                vector[j] = randomByte();
            }
            return VectorData.fromBytes(vector);
        }

        @Override
        public RescoreKnnVectorQuery createRescoreQuery(VectorData queryVector, Integer k) {
            return new RescoreKnnVectorQuery(
                FIELD_NAME,
                queryVector.byteVector(),
                VectorSimilarityFunction.COSINE,
                k,
                new MatchAllDocsQuery()
            );
        }

        @Override
        public KnnVectorValues vectorValues(LeafReader leafReader) throws IOException {
            return leafReader.getByteVectorValues(FIELD_NAME);
        }

        @Override
        public void addVectorField(Document document, VectorData vector) {
            KnnByteVectorField vectorField = new KnnByteVectorField(FIELD_NAME, vector.byteVector());
            document.add(vectorField);
        }

        @Override
        public VectorData dataVectorForDoc(KnnVectorValues vectorValues, int docId) throws IOException {
            return VectorData.fromBytes(((ByteVectorValues)vectorValues).vectorValue(docId));
        }

        @Override
        public float score(VectorData queryVector, VectorData dataVector) {
            return VectorSimilarityFunction.COSINE.compare(queryVector.byteVector(), dataVector.byteVector());
        }
    }

    private static void addRandomDocuments(int numDocs, Directory d, int numDims, VectorProvider vectorProvider) throws IOException {
        try (IndexWriter w = new IndexWriter(d, newIndexWriterConfig())) {
            for (int i = 0; i < numDocs; i++) {
                Document document = new Document();
                VectorData vector = vectorProvider.randomVector(numDims);
                vectorProvider.addVectorField(document, vector);
                w.addDocument(document);
            }
            w.commit();
            w.forceMerge(1);
        }
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {

        List<Object[]> params = new ArrayList<>();
        params.add(new Object[] {new FloatVectorProvider(), true});
        params.add(new Object[] {new FloatVectorProvider(), false});
        params.add(new Object[] {new ByteVectorProvider(), true});
        params.add(new Object[] {new ByteVectorProvider(), false});

        return params;
    }

//    public void testProfiling() throws Exception {
//        int numDocs = randomIntBetween(10, 100);
//        int numDims = randomIntBetween(5, 100);
//
//        try (Directory d = newDirectory()) {
//            addRandomDocuments(numDocs, d, numDims, vectorProvider);
//
//            try (IndexReader reader = DirectoryReader.open(d)) {
//                float[] queryVector = randomVector(numDims);
//
//                RescoreKnnVectorQuery rescoreKnnVectorQuery = new RescoreKnnVectorQuery(
//                    FIELD_NAME,
//                    queryVector,
//                    VectorSimilarityFunction.COSINE,
//                    randomIntBetween(5, numDocs - 1),
//                    new MatchAllDocsQuery()
//                );
//
//                IndexSearcher searcher = newSearcher(reader, true, false);
//                QueryProfiler queryProfiler = new QueryProfiler();
//                rescoreKnnVectorQuery.profile(queryProfiler);
//            }
//        }
//    }

}
