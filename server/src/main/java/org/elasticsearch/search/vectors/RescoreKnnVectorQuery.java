/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.queries.function.FunctionScoreQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocAndFloatFeatureBuffer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnByteVectorQuery;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.index.codec.vectors.BulkScorableFloatVectorValues;
import org.elasticsearch.index.codec.vectors.BulkScorableVectorValues;
import org.elasticsearch.search.profile.query.QueryProfiler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A Lucene {@link Query} that applies vector-based rescoring to an inner query's results.
 * <p>
 * Depending on the nature of the {@code innerQuery}, this class dynamically selects between two rescoring strategies:
 * <ul>
 *   <li><b>Inline rescoring</b>:
 *   Used when the inner query is already a top-N vector query with {@code rescoreK} results.
 *   The vector similarity is applied inline using a {@link FunctionScoreQuery} without an additional
 *   filtering pass.</li>
 *   <li><b>Late rescoring</b>: Used when the inner query is not a top-N vector query or does not return
 *   {@code rescoreK} results. The top {@code rescoreK} documents are first collected, and then rescoring is applied
 *   separately to select the final top {@code k}.</li>
 * </ul>
 */
public abstract class RescoreKnnVectorQuery extends Query implements QueryProfilerProvider {
    protected final String fieldName;
    protected final float[] floatTarget;
    protected final VectorSimilarityFunction vectorSimilarityFunction;
    protected final int k;
    protected final Query innerQuery;
    protected long vectorOperations = 0;

    private RescoreKnnVectorQuery(
        String fieldName,
        float[] floatTarget,
        VectorSimilarityFunction vectorSimilarityFunction,
        int k,
        Query innerQuery
    ) {
        this.fieldName = fieldName;
        this.floatTarget = floatTarget;
        this.vectorSimilarityFunction = vectorSimilarityFunction;
        this.k = k;
        this.innerQuery = innerQuery;
    }

    /**
     * Selects and returns the appropriate {@link RescoreKnnVectorQuery} strategy based on the nature of the {@code innerQuery}.
     *
     * @param fieldName                 the name of the field containing the vector
     * @param floatTarget              the target vector to compare against
     * @param vectorSimilarityFunction the similarity function to apply
     * @param k                        the number of top documents to return after rescoring
     * @param rescoreK                 the number of top documents to consider for rescoring
     * @param innerQuery               the original Lucene query to rescore
     */
    public static RescoreKnnVectorQuery fromInnerQuery(
        String fieldName,
        float[] floatTarget,
        VectorSimilarityFunction vectorSimilarityFunction,
        int k,
        int rescoreK,
        Query innerQuery
    ) {
        if ((innerQuery instanceof KnnFloatVectorQuery fQuery && fQuery.getK() == rescoreK)
            || (innerQuery instanceof KnnByteVectorQuery bQuery && bQuery.getK() == rescoreK)
            || (innerQuery instanceof AbstractIVFKnnVectorQuery ivfQuery && ivfQuery.k == rescoreK)) {
            // Queries that return only the top `k` results and do not require reduction before re-scoring.
            return new InlineRescoreQuery(fieldName, floatTarget, vectorSimilarityFunction, k, innerQuery);
        }
        return new LateRescoreQuery(fieldName, floatTarget, vectorSimilarityFunction, k, rescoreK, innerQuery);
    }

    public Query innerQuery() {
        return innerQuery;
    }

    public int k() {
        return k;
    }

    @Override
    public void profile(QueryProfiler queryProfiler) {
        if (innerQuery instanceof QueryProfilerProvider queryProfilerProvider) {
            queryProfilerProvider.profile(queryProfiler);
        }

        queryProfiler.addVectorOpsCount(vectorOperations);
    }

    @Override
    public void visit(QueryVisitor visitor) {
        innerQuery.visit(visitor.getSubVisitor(BooleanClause.Occur.MUST, this));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RescoreKnnVectorQuery that = (RescoreKnnVectorQuery) o;
        return Objects.equals(fieldName, that.fieldName)
            && Arrays.equals(floatTarget, that.floatTarget)
            && vectorSimilarityFunction == that.vectorSimilarityFunction
            && Objects.equals(k, that.k)
            && Objects.equals(innerQuery, that.innerQuery);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, Arrays.hashCode(floatTarget), vectorSimilarityFunction, k, innerQuery);
    }

    @Override
    public String toString(String field) {
        return getClass().getSimpleName()
            + "{"
            + "fieldName='"
            + fieldName
            + '\''
            + ", floatTarget="
            + floatTarget[0]
            + "..."
            + ", vectorSimilarityFunction="
            + vectorSimilarityFunction
            + ", k="
            + k
            + ", vectorQuery="
            + innerQuery
            + '}';
    }

    private static class InlineRescoreQuery extends RescoreKnnVectorQuery {
        private InlineRescoreQuery(
            String fieldName,
            float[] floatTarget,
            VectorSimilarityFunction vectorSimilarityFunction,
            int k,
            Query innerQuery
        ) {
            super(fieldName, floatTarget, vectorSimilarityFunction, k, innerQuery);
        }

        @Override
        public Query rewrite(IndexSearcher searcher) throws IOException {
            var rescoreQuery = new DirectRescoreKnnVectorQuery(fieldName, floatTarget, innerQuery);
            var topDocs = searcher.search(rescoreQuery, k);
            vectorOperations = topDocs.totalHits.value();
            return new KnnScoreDocQuery(topDocs.scoreDocs, searcher.getIndexReader());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            return super.equals(o);
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }
    }

    private static class LateRescoreQuery extends RescoreKnnVectorQuery {
        final int rescoreK;

        private LateRescoreQuery(
            String fieldName,
            float[] floatTarget,
            VectorSimilarityFunction vectorSimilarityFunction,
            int k,
            int rescoreK,
            Query innerQuery
        ) {
            super(fieldName, floatTarget, vectorSimilarityFunction, k, innerQuery);
            this.rescoreK = rescoreK;
        }

        @Override
        public Query rewrite(IndexSearcher searcher) throws IOException {
            final TopDocs topDocs;
            // Retrieve top `rescoreK` documents from the inner query
            topDocs = searcher.search(innerQuery, rescoreK);
            vectorOperations = topDocs.totalHits.value();

            // Retrieve top `k` documents from the top `rescoreK` query
            var topDocsQuery = new KnnScoreDocQuery(topDocs.scoreDocs, searcher.getIndexReader());
            var rescoreQuery = new DirectRescoreKnnVectorQuery(fieldName, floatTarget, topDocsQuery);
            var rescoreTopDocs = searcher.search(rescoreQuery.rewrite(searcher), k);
            return new KnnScoreDocQuery(rescoreTopDocs.scoreDocs, searcher.getIndexReader());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            var that = (RescoreKnnVectorQuery.LateRescoreQuery) o;
            return super.equals(o) && that.rescoreK == rescoreK;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), rescoreK);
        }
    }

    private static class DirectRescoreKnnVectorQuery extends Query {
        private final float[] floatTarget;
        private final String fieldName;
        private final Query innerQuery;

        DirectRescoreKnnVectorQuery(String fieldName, float[] floatTarget, Query innerQuery) {
            this.fieldName = fieldName;
            this.floatTarget = floatTarget;
            this.innerQuery = innerQuery;
        }

        @Override
        public String toString(String field) {
            return "DirectRescoreKnnVectorQuery[" + innerQuery + "]";
        }

        @Override
        public Query rewrite(IndexSearcher indexSearcher) throws IOException {
            Query innerRewritten = innerQuery.rewrite(indexSearcher);
            if (innerRewritten.getClass() == MatchNoDocsQuery.class) {
                return new MatchNoDocsQuery();
            }
            assert innerRewritten.getClass() != MatchAllDocsQuery.class;

            List<ScoreDoc> results = new ArrayList<>(10);
            for (var leaf : indexSearcher.getIndexReader().leaves()) {
                var knnVectorValues = leaf.reader().getFloatVectorValues(fieldName);
                if (knnVectorValues == null) {
                    continue;
                }
                if (knnVectorValues.dimension() != floatTarget.length) {
                    throw new IllegalArgumentException(
                        "vector query dimension: " + floatTarget.length + " differs from field dimension: " + knnVectorValues.dimension()
                    );
                }
                var weight = innerRewritten.createWeight(indexSearcher, ScoreMode.COMPLETE_NO_SCORES, 1.0f);
                var scorer = weight.scorer(leaf);
                if (scorer == null) {
                    continue;
                }
                var filterIterator = scorer.iterator();
                if (knnVectorValues instanceof BulkScorableFloatVectorValues rescorableVectorValues) {
                    rescoreBulk(leaf.docBase, rescorableVectorValues, results, filterIterator);
                } else {
                    rescoreIndividually(
                        leaf.docBase,
                        knnVectorValues,
                        leaf.reader().getFieldInfos().fieldInfo(fieldName).getVectorSimilarityFunction(),
                        results,
                        filterIterator
                    );
                }
            }
            // Remove any remaining sentinel values
            ScoreDoc[] arrayResults = results.toArray(new ScoreDoc[0]);
            return new KnnScoreDocQuery(arrayResults, indexSearcher.getIndexReader());
        }

        @Override
        public void visit(QueryVisitor visitor) {
            if (visitor.acceptField(fieldName)) {
                visitor.visitLeaf(this);
            }
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            DirectRescoreKnnVectorQuery that = (DirectRescoreKnnVectorQuery) obj;
            return Objects.equals(innerQuery, that.innerQuery);
        }

        @Override
        public int hashCode() {
            return Objects.hash(innerQuery, getClass());
        }

        private void rescoreBulk(
            int docBase,
            BulkScorableFloatVectorValues rescorableVectorValues,
            List<ScoreDoc> queue,
            DocIdSetIterator filterIterator
        ) throws IOException {
            BulkScorableVectorValues.BulkVectorScorer vectorReScorer = rescorableVectorValues.bulkRescorer(floatTarget);
            var iterator = vectorReScorer.iterator();
            BulkScorableVectorValues.BulkVectorScorer.BulkScorer bulkScorer = vectorReScorer.bulkScore(filterIterator);
            DocAndFloatFeatureBuffer buffer = new DocAndFloatFeatureBuffer();
            while (iterator.docID() != DocIdSetIterator.NO_MORE_DOCS) {
                // iterator already takes live docs into account
                bulkScorer.nextDocsAndScores(64, null, buffer);
                for (int i = 0; i < buffer.size; i++) {
                    float score = buffer.features[i];
                    int doc = buffer.docs[i];
                    queue.add(new ScoreDoc(doc + docBase, score));
                }
            }
        }

        private void rescoreIndividually(
            int docBase,
            FloatVectorValues knnVectorValues,
            VectorSimilarityFunction function,
            List<ScoreDoc> queue,
            DocIdSetIterator filterIterator
        ) throws IOException {
            int doc;
            KnnVectorValues.DocIndexIterator knnVectorIterator = knnVectorValues.iterator();
            var conjunction = ConjunctionUtils.intersectIterators(List.of(knnVectorIterator, filterIterator));
            while ((doc = conjunction.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                assert doc == knnVectorIterator.docID();
                float[] vector = knnVectorValues.vectorValue(knnVectorIterator.index());
                float score = function.compare(floatTarget, vector);
                if (Float.isNaN(score)) {
                    continue;
                }
                queue.add(new ScoreDoc(doc + docBase, score));
            }
        }
    }
}
