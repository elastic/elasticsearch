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
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.queries.function.FunctionScoreQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.FullPrecisionFloatVectorSimilarityValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.elasticsearch.index.codec.Elasticsearch92Lucene103Codec;
import org.elasticsearch.index.codec.vectors.ES813Int8FlatVectorFormat;
import org.elasticsearch.index.codec.vectors.ES814HnswScalarQuantizedVectorsFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.ES920DiskBBQVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93BinaryQuantizedVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93HnswBinaryQuantizedVectorsFormat;
import org.elasticsearch.index.codec.zstd.Zstd814StoredFieldsFormat;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.search.profile.query.QueryProfiler;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.index.codec.vectors.diskbbq.ES920DiskBBQVectorsFormat.DEFAULT_CENTROIDS_PER_PARENT_CLUSTER;
import static org.elasticsearch.index.codec.vectors.diskbbq.ES920DiskBBQVectorsFormat.DEFAULT_VECTORS_PER_CLUSTER;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class RescoreKnnVectorQueryTests extends ESTestCase {

    public static final String FIELD_NAME = "float_vector";

    public void testRescoreDocs() throws Exception {
        int numDocs = randomIntBetween(10, 100);
        int numDims = randomIntBetween(5, 100);
        int k = randomIntBetween(1, numDocs - 1);

        var queryVector = randomVector(numDims);
        List<Query> innerQueries = new ArrayList<>();
        innerQueries.add(new KnnFloatVectorQuery(FIELD_NAME, randomVector(numDims), (int) (k * randomFloatBetween(1.0f, 10.0f, true))));
        innerQueries.add(
            new BooleanQuery.Builder().add(new DenseVectorQuery.Floats(queryVector, FIELD_NAME), BooleanClause.Occur.SHOULD)
                .add(new FieldExistsQuery(FIELD_NAME), BooleanClause.Occur.FILTER)
                .build()
        );
        innerQueries.add(new MatchAllDocsQuery());

        try (Directory d = newDirectory()) {
            addRandomDocuments(numDocs, d, numDims);

            try (IndexReader reader = DirectoryReader.open(d)) {

                for (var innerQuery : innerQueries) {
                    RescoreKnnVectorQuery rescoreKnnVectorQuery = RescoreKnnVectorQuery.fromInnerQuery(
                        FIELD_NAME,
                        queryVector,
                        VectorSimilarityFunction.COSINE,
                        k,
                        k,
                        innerQuery
                    );

                    IndexSearcher searcher = newSearcher(reader, true, false);
                    TopDocs rescoredDocs = searcher.search(rescoreKnnVectorQuery, numDocs);
                    assertThat(rescoredDocs.scoreDocs.length, equalTo(k));

                    // Get real scores
                    DoubleValuesSource valueSource = new FullPrecisionFloatVectorSimilarityValuesSource(
                        queryVector,
                        FIELD_NAME,
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
    }

    public void testRescoreSingleAndBulkEquality() throws Exception {
        int numDocs = randomIntBetween(10, 100);
        int numDims = randomIntBetween(5, 100);
        int k = randomIntBetween(1, numDocs - 1);

        var queryVector = randomVector(numDims);

        List<Query> innerQueries = new ArrayList<>();
        innerQueries.add(new KnnFloatVectorQuery(FIELD_NAME, randomVector(numDims), (int) (k * randomFloatBetween(1.0f, 10.0f, true))));
        innerQueries.add(
            new BooleanQuery.Builder().add(new DenseVectorQuery.Floats(queryVector, FIELD_NAME), BooleanClause.Occur.SHOULD)
                .add(new FieldExistsQuery(FIELD_NAME), BooleanClause.Occur.FILTER)
                .build()
        );
        innerQueries.add(new MatchAllDocsQuery());

        try (Directory d = newDirectory()) {
            addRandomDocuments(numDocs, d, numDims);
            try (DirectoryReader reader = DirectoryReader.open(d)) {
                for (var innerQuery : innerQueries) {
                    RescoreKnnVectorQuery rescoreKnnVectorQuery = RescoreKnnVectorQuery.fromInnerQuery(
                        FIELD_NAME,
                        queryVector,
                        VectorSimilarityFunction.COSINE,
                        k,
                        k,
                        innerQuery
                    );

                    IndexSearcher searcher = newSearcher(reader, true, false);
                    TopDocs rescoredDocs = searcher.search(rescoreKnnVectorQuery, numDocs);
                    assertThat(rescoredDocs.scoreDocs.length, equalTo(k));

                    searcher = newSearcher(new SingleVectorQueryIndexReader(reader), true, false);
                    rescoreKnnVectorQuery = RescoreKnnVectorQuery.fromInnerQuery(
                        FIELD_NAME,
                        queryVector,
                        VectorSimilarityFunction.COSINE,
                        k,
                        k,
                        innerQuery
                    );
                    TopDocs singleRescored = searcher.search(rescoreKnnVectorQuery, numDocs);
                    assertThat(singleRescored.scoreDocs.length, equalTo(k));

                    // Get real scores
                    ScoreDoc[] singleRescoreDocs = singleRescored.scoreDocs;
                    int i = 0;
                    for (ScoreDoc rescoreDoc : rescoredDocs.scoreDocs) {
                        assertThat(rescoreDoc.doc, equalTo(singleRescoreDocs[i].doc));
                        assertThat(rescoreDoc.score, equalTo(singleRescoreDocs[i].score));
                        i++;
                    }
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
        var rescoreKnnVectorQuery = RescoreKnnVectorQuery.fromInnerQuery(
            FIELD_NAME,
            queryVector,
            VectorSimilarityFunction.COSINE,
            k,
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
            new ES920DiskBBQVectorsFormat(
                DEFAULT_VECTORS_PER_CLUSTER,
                DEFAULT_CENTROIDS_PER_PARENT_CLUSTER,
                randomFrom(DenseVectorFieldMapper.ElementType.FLOAT, DenseVectorFieldMapper.ElementType.BFLOAT16),
                randomBoolean()
            ),
            new ES93BinaryQuantizedVectorsFormat(
                randomFrom(DenseVectorFieldMapper.ElementType.FLOAT, DenseVectorFieldMapper.ElementType.BFLOAT16),
                randomBoolean()
            ),
            new ES93HnswBinaryQuantizedVectorsFormat(
                randomFrom(DenseVectorFieldMapper.ElementType.FLOAT, DenseVectorFieldMapper.ElementType.BFLOAT16),
                randomBoolean()
            ),
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

    private static class SingleVectorQueryIndexReader extends FilterDirectoryReader {

        /**
         * Create a new FilterDirectoryReader that filters a passed in DirectoryReader, using the supplied
         * SubReaderWrapper to wrap its subreader.
         *
         * @param in      the DirectoryReader to filter
         */
        SingleVectorQueryIndexReader(DirectoryReader in) throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    return new FilterLeafReader(reader) {
                        @Override
                        public CacheHelper getReaderCacheHelper() {
                            return null;
                        }

                        @Override
                        public CacheHelper getCoreCacheHelper() {
                            return null;
                        }

                        @Override
                        public FloatVectorValues getFloatVectorValues(String field) throws IOException {
                            FloatVectorValues values = super.getFloatVectorValues(field);
                            if (values == null) {
                                return null;
                            }
                            return new RegularFloatVectorValues(values);
                        }
                    };
                }
            });
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new SingleVectorQueryIndexReader(in);
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return null;
        }
    }

    private static final class RegularFloatVectorValues extends FloatVectorValues {

        private final FloatVectorValues in;

        RegularFloatVectorValues(FloatVectorValues in) {
            this.in = in;
        }

        @Override
        public VectorScorer scorer(float[] target) throws IOException {
            return in.scorer(target);
        }

        @Override
        public int ordToDoc(int ord) {
            return in.ordToDoc(ord);
        }

        @Override
        public Bits getAcceptOrds(Bits acceptDocs) {
            return in.getAcceptOrds(acceptDocs);
        }

        @Override
        public DocIndexIterator iterator() {
            return in.iterator();
        }

        @Override
        public float[] vectorValue(int ord) throws IOException {
            return in.vectorValue(ord);
        }

        @Override
        public FloatVectorValues copy() throws IOException {
            return new RegularFloatVectorValues(in.copy());
        }

        @Override
        public int dimension() {
            return in.dimension();
        }

        @Override
        public int size() {
            return in.size();
        }
    }
}
