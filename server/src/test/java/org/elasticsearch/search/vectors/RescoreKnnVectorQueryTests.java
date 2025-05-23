/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.queries.function.FunctionScoreQuery;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.elasticsearch.index.codec.Elasticsearch92Lucene103Codec;
import org.elasticsearch.index.codec.vectors.ES813Int8FlatVectorFormat;
import org.elasticsearch.index.codec.vectors.ES814HnswScalarQuantizedVectorsFormat;
import org.elasticsearch.index.codec.vectors.es818.ES818BinaryQuantizedVectorsFormat;
import org.elasticsearch.index.codec.vectors.es818.ES818HnswBinaryQuantizedVectorsFormat;
import org.elasticsearch.index.codec.zstd.Zstd814StoredFieldsFormat;
import org.elasticsearch.index.mapper.vectors.VectorSimilarityFloatValueSource;
import org.elasticsearch.search.profile.query.QueryProfiler;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class RescoreKnnVectorQueryTests extends ESTestCase {

    public static final String FIELD_NAME = "float_vector";

    public void testRescoreDocs() throws Exception {
        int numDocs = randomIntBetween(10, 100);
        int numDims = randomIntBetween(5, 100);
        int k = randomIntBetween(1, numDocs - 1);

        try (Directory d = newDirectory()) {
            addRandomDocuments(numDocs, d, numDims);

            try (IndexReader reader = DirectoryReader.open(d)) {

                // Use a RescoreKnnVectorQuery with a match all query, to ensure we get scoring of 1 from the inner query
                // and thus we're rescoring the top k docs.
                float[] queryVector = randomVector(numDims);
                Query innerQuery;
                if (randomBoolean()) {
                    innerQuery = new KnnFloatVectorQuery(FIELD_NAME, queryVector, (int) (k * randomFloatBetween(1.0f, 10.0f, true)));
                } else {
                    innerQuery = new MatchAllDocsQuery();
                }
                RescoreKnnVectorQuery rescoreKnnVectorQuery = new RescoreKnnVectorQuery(
                    FIELD_NAME,
                    queryVector,
                    VectorSimilarityFunction.COSINE,
                    k,
                    innerQuery
                );

                IndexSearcher searcher = newSearcher(reader, true, false);
                TopDocs rescoredDocs = searcher.search(rescoreKnnVectorQuery, numDocs);
                assertThat(rescoredDocs.scoreDocs.length, equalTo(k));

                // Get real scores
                DoubleValuesSource valueSource = new VectorSimilarityFloatValueSource(
                    FIELD_NAME,
                    queryVector,
                    VectorSimilarityFunction.COSINE
                );
                FunctionScoreQuery functionScoreQuery = new FunctionScoreQuery(new MatchAllDocsQuery(), valueSource);
                TopDocs realScoreTopDocs = searcher.search(functionScoreQuery, numDocs);

                int i = 0;
                ScoreDoc[] realScoreDocs = realScoreTopDocs.scoreDocs;
                for (ScoreDoc rescoreDoc : rescoredDocs.scoreDocs) {
                    // There are docs that won't be found in the rescored search, but every doc found must be in the same order
                    // and have the same score
                    while (i < realScoreDocs.length && realScoreDocs[i].doc != rescoreDoc.doc) {
                        i++;
                    }
                    if (i >= realScoreDocs.length) {
                        fail("Rescored doc not found in real score docs");
                    }
                    assertThat("Real score is not the same as rescored score", rescoreDoc.score, equalTo(realScoreDocs[i].score));
                }
            }
        }
    }

    public void testProfiling() throws Exception {
        int numDocs = randomIntBetween(10, 100);
        int numDims = randomIntBetween(5, 100);
        int k = randomIntBetween(1, numDocs - 1);

        try (Directory d = newDirectory()) {
            addRandomDocuments(numDocs, d, numDims);

            try (IndexReader reader = DirectoryReader.open(d)) {
                float[] queryVector = randomVector(numDims);

                checkProfiling(k, numDocs, queryVector, reader, new MatchAllDocsQuery());
                checkProfiling(k, numDocs, queryVector, reader, new MockQueryProfilerProvider(randomIntBetween(1, 100)));
            }
        }
    }

    private void checkProfiling(int k, int numDocs, float[] queryVector, IndexReader reader, Query innerQuery) throws IOException {
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
        IndexWriterConfig iwc = new IndexWriterConfig();
        // Pick codec from quantized vector formats to ensure scores use real scores when using knn rescore
        KnnVectorsFormat format = randomFrom(
            new ES818BinaryQuantizedVectorsFormat(),
            new ES818HnswBinaryQuantizedVectorsFormat(),
            new ES813Int8FlatVectorFormat(),
            new ES813Int8FlatVectorFormat(),
            new ES814HnswScalarQuantizedVectorsFormat()
        );
        iwc.setCodec(new Elasticsearch92Lucene103Codec(randomFrom(Zstd814StoredFieldsFormat.Mode.values())) {
            @Override
            public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                return format;
            }
        });
        try (IndexWriter w = new IndexWriter(d, newIndexWriterConfig())) {
            for (int i = 0; i < numDocs; i++) {
                Document document = new Document();
                float[] vector = randomVector(numDims);
                KnnFloatVectorField vectorField = new KnnFloatVectorField(FIELD_NAME, vector, VectorSimilarityFunction.COSINE);
                document.add(vectorField);
                w.addDocument(document);
                if (randomBoolean() && (i % 10 == 0)) {
                    w.commit();
                }
            }
            w.commit();
        }
    }
}
