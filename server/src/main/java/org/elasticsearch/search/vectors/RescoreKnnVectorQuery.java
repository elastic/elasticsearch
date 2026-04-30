/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.codecs.lucene95.HasIndexSlice;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
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
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.core.CheckedRunnable;
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
    protected final int k;
    protected final Query innerQuery;
    protected long vectorOperations = 0;

    private RescoreKnnVectorQuery(String fieldName, float[] floatTarget, int k, Query innerQuery) {
        this.fieldName = fieldName;
        this.floatTarget = floatTarget;
        this.k = k;
        this.innerQuery = innerQuery;
    }

    /**
     * Selects and returns the appropriate {@link RescoreKnnVectorQuery} strategy based on the nature of the {@code innerQuery}.
     *
     * @param fieldName                the name of the field containing the vector
     * @param floatTarget              the target vector to compare against
     * @param k                        the number of top documents to return after rescoring
     * @param rescoreK                 the number of top documents to consider for rescoring
     * @param innerQuery               the original Lucene query to rescore
     */
    public static RescoreKnnVectorQuery fromInnerQuery(String fieldName, float[] floatTarget, int k, int rescoreK, Query innerQuery) {
        if ((innerQuery instanceof KnnFloatVectorQuery fQuery && fQuery.getK() == rescoreK)
            || (innerQuery instanceof KnnByteVectorQuery bQuery && bQuery.getK() == rescoreK)
            || (innerQuery instanceof AbstractIVFKnnVectorQuery ivfQuery && ivfQuery.k == rescoreK)) {
            // Queries that return only the top `k` results and do not require reduction before re-scoring.
            return new InlineRescoreQuery(fieldName, floatTarget, k, innerQuery);
        }
        return new LateRescoreQuery(fieldName, floatTarget, k, rescoreK, innerQuery);
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
            && Objects.equals(k, that.k)
            && Objects.equals(innerQuery, that.innerQuery);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, Arrays.hashCode(floatTarget), k, innerQuery);
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
            + ", k="
            + k
            + ", vectorQuery="
            + innerQuery
            + '}';
    }

    private static class InlineRescoreQuery extends RescoreKnnVectorQuery {
        private InlineRescoreQuery(String fieldName, float[] floatTarget, int k, Query innerQuery) {
            super(fieldName, floatTarget, k, innerQuery);
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

        private LateRescoreQuery(String fieldName, float[] floatTarget, int k, int rescoreK, Query innerQuery) {
            super(fieldName, floatTarget, k, innerQuery);
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
        private static final int PREFETCH_BUFFER_SIZE = 100;

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
                return Queries.NO_DOCS_INSTANCE;
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
                bulkRescore(leaf.docBase, knnVectorValues, results, filterIterator);
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

        /**
         * Adds rescoring work to {@code buffer}. If {@code buffer} is non-empty after this call,
         * the caller must run the queued {@link CheckedRunnable}s to materialize results into {@code queue}.
         */
        private void bulkRescore(int docBase, FloatVectorValues knnVectorValues, List<ScoreDoc> queue, DocIdSetIterator filterIterator)
            throws IOException {
            int vectorByteSize = knnVectorValues.getVectorByteLength();
            IndexInput input = (knnVectorValues instanceof HasIndexSlice h) ? h.getSlice() : null;

            KnnVectorValues.DocIndexIterator vectorIter = knnVectorValues.iterator();
            DocIdSetIterator conjunction = ConjunctionUtils.intersectIterators(List.of(vectorIter, filterIterator));

            VectorScorer scorer = knnVectorValues.rescorer(floatTarget);
            DocAndFloatFeatureBuffer scoreBuffer = new DocAndFloatFeatureBuffer();
            scoreBuffer.growNoCopy(PREFETCH_BUFFER_SIZE);

            if (input != null) {
                int[] docs = new int[PREFETCH_BUFFER_SIZE];

                // sequentially copy the doc ids into the docs array for bulk rescoring in batches
                int nextDocIdx = 0;
                while ((docs[nextDocIdx++] = conjunction.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {

                    if (nextDocIdx == PREFETCH_BUFFER_SIZE) {
                        // do the bulk rescore
                        var bulk = scorer.bulk(new ArrayDocIdSetIterator(docs, PREFETCH_BUFFER_SIZE));
                        bulk.nextDocsAndScores(docs[PREFETCH_BUFFER_SIZE - 1], null, scoreBuffer);

                        for (int s = 0; s < scoreBuffer.size; s++) {
                            if (!Float.isNaN(scoreBuffer.features[s])) {
                                queue.add(new ScoreDoc(scoreBuffer.docs[s] + docBase, scoreBuffer.features[s]));
                            }
                        }
                        nextDocIdx = 0; // don't need to explicitly clear the arrays, they just get overwritten
                    }

                    input.prefetch((long) vectorIter.index() * vectorByteSize, vectorByteSize);
                }

                if (nextDocIdx > 0) {
                    // rescore the remaining docs
                    var bulk = scorer.bulk(new ArrayDocIdSetIterator(docs, nextDocIdx));
                    bulk.nextDocsAndScores(docs[nextDocIdx - 1], null, scoreBuffer);

                    for (int s = 0; s < scoreBuffer.size; s++) {
                        if (!Float.isNaN(scoreBuffer.features[s])) {
                            queue.add(new ScoreDoc(scoreBuffer.docs[s] + docBase, scoreBuffer.features[s]));
                        }
                    }
                }
            } else {
                // just use the bulk scorer over the whole lot, nothing to prefetch
                var bulk = scorer.bulk(conjunction);
                bulk.nextDocsAndScores(DocIdSetIterator.NO_MORE_DOCS, null, scoreBuffer);

                for (int s = 0; s < scoreBuffer.size; s++) {
                    if (!Float.isNaN(scoreBuffer.features[s])) {
                        queue.add(new ScoreDoc(scoreBuffer.docs[s] + docBase, scoreBuffer.features[s]));
                    }
                }
            }
        }

        private static class ArrayDocIdSetIterator extends DocIdSetIterator {

            private final int[] ids;
            private final int size;
            private int idx = -1;

            private ArrayDocIdSetIterator(int[] ids, int size) {
                this.ids = ids;
                this.size = size;
            }

            @Override
            public int docID() {
                if (idx == -1) {
                    return -1;
                }
                return idx < size ? ids[idx] : NO_MORE_DOCS;
            }

            @Override
            public int nextDoc() {
                if (++idx >= size) {
                    return NO_MORE_DOCS;
                } else {
                    return ids[idx];
                }
            }

            @Override
            public int advance(int target) {
                if (idx >= size) {
                    return NO_MORE_DOCS;
                }

                int pos = Arrays.binarySearch(ids, idx + 1, size, target);
                if (pos < 0) {
                    pos = -pos - 1;
                }
                idx = pos;

                return docID();
            }

            @Override
            public long cost() {
                return size;
            }
        }
    }
}
