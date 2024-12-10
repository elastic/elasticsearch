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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.elasticsearch.search.profile.query.QueryProfiler;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
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
import static org.hamcrest.Matchers.greaterThan;

public class RescoreKnnVectorQueryTests extends ESTestCase {

    public static final String FIELD_NAME = "float_vector";
    private final int numDocs;
    private final Integer k;

    public RescoreKnnVectorQueryTests(boolean useK) {
        this.numDocs = randomIntBetween(10, 100);
        this.k = useK ? randomIntBetween(1, numDocs - 1) : null;
    }

    public void testRescoreDocs() throws Exception {
        int numDims = randomIntBetween(5, 100);

        Integer adjustedK = k;
        if (k == null) {
            adjustedK = numDocs;
        }

        try (Directory d = newDirectory()) {
            addRandomDocuments(numDocs, d, numDims);

            try (IndexReader reader = DirectoryReader.open(d)) {

                // Use a RescoreKnnVectorQuery with a match all query, to ensure we get scoring of 1 from the inner query
                // and thus we're rescoring the top k docs.
                float[] queryVector = randomVector(numDims);
                RescoreKnnVectorQuery rescoreKnnVectorQuery = new RescoreKnnVectorQuery(
                    FIELD_NAME,
                    queryVector,
                    VectorSimilarityFunction.COSINE,
                    adjustedK,
                    new MatchAllDocsQuery()
                );

                IndexSearcher searcher = newSearcher(reader, true, false);
                TopDocs docs = searcher.search(rescoreKnnVectorQuery, numDocs);
                Map<Integer, Float> rescoredDocs = Arrays.stream(docs.scoreDocs)
                    .collect(Collectors.toMap(scoreDoc -> scoreDoc.doc, scoreDoc -> scoreDoc.score));

                assertThat(rescoredDocs.size(), equalTo(adjustedK));

                Collection<Float> rescoredScores = new HashSet<>(rescoredDocs.values());

                // Collect all docs sequentially, and score them using the similarity function to get the top K scores
                PriorityQueue<Float> topK = new PriorityQueue<>((o1, o2) -> Float.compare(o2, o1));

                for (LeafReaderContext leafReaderContext : reader.leaves()) {
                    FloatVectorValues vectorValues = leafReaderContext.reader().getFloatVectorValues(FIELD_NAME);
                    KnnVectorValues.DocIndexIterator iterator = vectorValues.iterator();
                    while (iterator.nextDoc() != NO_MORE_DOCS) {
                        float[] vectorData = vectorValues.vectorValue(iterator.docID());
                        float score = VectorSimilarityFunction.COSINE.compare(queryVector, vectorData);
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

    public void testProfiling() throws Exception {
        int numDims = randomIntBetween(5, 100);

        try (Directory d = newDirectory()) {
            addRandomDocuments(numDocs, d, numDims);

            try (IndexReader reader = DirectoryReader.open(d)) {
                float[] queryVector = randomVector(numDims);

                checkProfiling(queryVector, reader, new MatchAllDocsQuery());
                checkProfiling(queryVector, reader, new MockQueryProfilerProvider(randomIntBetween(1, 100)));
            }
        }
    }

    private void checkProfiling(float[] queryVector, IndexReader reader, Query innerQuery) throws IOException {
        RescoreKnnVectorQuery rescoreKnnVectorQuery = new RescoreKnnVectorQuery(
            FIELD_NAME,
            queryVector,
            VectorSimilarityFunction.COSINE,
            k,
            innerQuery
        );
        IndexSearcher searcher = newSearcher(reader, true, false);
        searcher.search(rescoreKnnVectorQuery, numDocs);

        QueryProfiler queryProfiler = new QueryProfiler();
        rescoreKnnVectorQuery.profile(queryProfiler);

        long expectedVectorOpsCount = numDocs;
        if (innerQuery instanceof QueryProfilerProvider queryProfilerProvider) {
            QueryProfiler anotherProfiler = new QueryProfiler();
            queryProfilerProvider.profile(anotherProfiler);
            assertThat(anotherProfiler.getVectorOpsCount(), greaterThan(0L));
            expectedVectorOpsCount += anotherProfiler.getVectorOpsCount();
        }

        assertThat(queryProfiler.getVectorOpsCount(), equalTo(expectedVectorOpsCount));
    }

    private static float[] randomVector(int numDimensions) {
        float[] vector = new float[numDimensions];
        for (int j = 0; j < numDimensions; j++) {
            vector[j] = randomFloatBetween(0, 1, true);
        }
        return vector;
    }

    /**
     * A mock query that is used to test profiling
     */
    private static class MockQueryProfilerProvider extends Query implements QueryProfilerProvider {

        private final long vectorOpsCount;

        private MockQueryProfilerProvider(long vectorOpsCount) {
            this.vectorOpsCount = vectorOpsCount;
        }

        @Override
        public String toString(String field) {
            return "";
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            throw new UnsupportedEncodingException("Should have been rewritten");
        }

        @Override
        public Query rewrite(IndexSearcher indexSearcher) throws IOException {
            return new MatchAllDocsQuery();
        }

        @Override
        public void visit(QueryVisitor visitor) {}

        @Override
        public boolean equals(Object obj) {
            return obj instanceof MockQueryProfilerProvider;
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public void profile(QueryProfiler queryProfiler) {
            queryProfiler.addVectorOpsCount(vectorOpsCount);
        }
    }

    private static void addRandomDocuments(int numDocs, Directory d, int numDims) throws IOException {
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
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<Object[]> params = new ArrayList<>();
        params.add(new Object[] { true });
        params.add(new Object[] { false });

        return params;
    }
}
