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
            || (innerQuery instanceof KnnByteVectorQuery bQuery && bQuery.getK() == rescoreK)) {
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
        private static final int BULK_SCORE_SIZE = 32;

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

        /**
         * A ring buffer that allows doc data to be prefetched, and then actually scored at a later point
         * using a bulk scorer
         */
        private static class PrefetchRing {
            private final int[] docIds;
            private final int[] docBases;
            private final VectorScorer[] scorer;

            /*
             * Only one bulk scorer can be created for a VectorScorer at once,
             * so we need to re-use any created bulk scorer in subsequent batches
             * for the same scorer. Might as well store them here.
             */
            private VectorScorer previousScorer;
            private VectorScorer.Bulk previousBulkScorer;

            private int head;
            private int size;

            PrefetchRing() {
                this.docIds = new int[PREFETCH_BUFFER_SIZE];
                this.docBases = new int[PREFETCH_BUFFER_SIZE];
                this.scorer = new VectorScorer[PREFETCH_BUFFER_SIZE];
            }

            static int ringIdx(int head, int idx) {
                return (head + idx) % PREFETCH_BUFFER_SIZE;
            }

            void advance(int count) {
                int newHead = ringIdx(head, count);

                // clear the scorers so the RingIterator doesn't pick up stale entries
                // if iteration over a single leaf loops all the way round
                if (newHead > head) {
                    Arrays.fill(scorer, head, newHead, null);
                } else {
                    Arrays.fill(scorer, head, PREFETCH_BUFFER_SIZE, null);
                    Arrays.fill(scorer, 0, newHead, null);
                }

                head = newHead;
                size -= count;
            }

            void append(int doc, int docBase, VectorScorer vecScorer) {
                int idx = ringIdx(head, size);
                docIds[idx] = doc;
                docBases[idx] = docBase;
                scorer[idx] = vecScorer;
                size++;
            }
        }

        /**
         * A lazy docID iterator over {@link PrefetchRing} for a specific scorer
         */
        static class RingIterator extends DocIdSetIterator {

            private final PrefetchRing ring;
            private final VectorScorer scorer;
            private final int startIdx;
            private int idx = 0;    // just start on startIdx without needing an initial advance

            RingIterator(PrefetchRing ring, VectorScorer scorer, int startIdx) {
                this.ring = ring;
                this.scorer = scorer;
                this.startIdx = startIdx;
            }

            @Override
            public int docID() {
                if (idx == NO_MORE_DOCS) {
                    return idx;
                }
                return ring.docIds[PrefetchRing.ringIdx(startIdx, idx)];
            }

            @Override
            public int nextDoc() {
                idx++;
                if (ring.scorer[PrefetchRing.ringIdx(startIdx, idx)] != scorer) {
                    idx = NO_MORE_DOCS;
                }
                return docID();
            }

            @Override
            public int advance(int target) throws IOException {
                return slowAdvance(target);
            }

            @Override
            public long cost() {
                return BULK_SCORE_SIZE;
            }
        }

        @Override
        public Query rewrite(IndexSearcher indexSearcher) throws IOException {
            Query innerRewritten = innerQuery.rewrite(indexSearcher);
            if (innerRewritten.getClass() == MatchNoDocsQuery.class) {
                return Queries.NO_DOCS_INSTANCE;
            }
            assert innerRewritten.getClass() != MatchAllDocsQuery.class;

            DocAndFloatFeatureBuffer buffer = new DocAndFloatFeatureBuffer();
            List<ScoreDoc> results = new ArrayList<>(10);

            PrefetchRing ring = new PrefetchRing();

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

                final int vectorByteSize = knnVectorValues.getVectorByteLength();
                final IndexInput input = getIndexSliceOrNull(knnVectorValues);
                KnnVectorValues.DocIndexIterator vectorIter = knnVectorValues.iterator();
                DocIdSetIterator conjunction = ConjunctionUtils.intersectIterators(List.of(vectorIter, filterIterator));
                VectorScorer rescorer = knnVectorValues.rescorer(floatTarget);

                int doc;
                while ((doc = conjunction.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    assert doc == vectorIter.docID();
                    final int ord = vectorIter.index();

                    if (input != null) {
                        input.prefetch((long) ord * vectorByteSize, vectorByteSize);
                    }

                    if (ring.size == PREFETCH_BUFFER_SIZE) {
                        int scored = scoreEntries(ring, buffer, results);
                        ring.advance(scored);
                    }

                    ring.append(doc, leaf.docBase, rescorer);
                }
            }

            while (ring.size > 0) {
                int scored = scoreEntries(ring, buffer, results);
                ring.advance(scored);
            }

            return new KnnScoreDocQuery(results.toArray(ScoreDoc[]::new), indexSearcher.getIndexReader());
        }

        private static IndexInput getIndexSliceOrNull(KnnVectorValues vectorValues) {
            return vectorValues instanceof HasIndexSlice h ? h.getSlice() : null;
        }

        private static int scoreEntries(PrefetchRing ring, DocAndFloatFeatureBuffer buffer, List<ScoreDoc> results) throws IOException {
            int docBase = ring.docBases[ring.head];
            VectorScorer scorer = ring.scorer[ring.head];

            // create the bulk scorer from this scorer if it's not created already
            VectorScorer.Bulk bulkScorer;
            if (ring.previousScorer == scorer) {
                // re-use the bulk scorer from the previous batch
                bulkScorer = ring.previousBulkScorer;
            } else {
                // first use of this scorer - create a new bulk scorer
                RingIterator iterator = new RingIterator(ring, scorer, ring.head);
                scorer.iterator().advance(ring.docIds[ring.head]);
                bulkScorer = scorer.bulk(iterator);

                // and record it for any subsequent batches
                ring.previousScorer = scorer;
                ring.previousBulkScorer = bulkScorer;
            }

            // find up to BULK_SCORE_SIZE docs for this scorer to score
            int count = 0;
            for (; count < BULK_SCORE_SIZE && count < ring.size; count++) {
                int idx = PrefetchRing.ringIdx(ring.head, count);
                if (ring.scorer[idx] != scorer) break;    // scorer has changed - stop there
            }
            assert count > 0;

            int maxDocId = ring.docIds[PrefetchRing.ringIdx(ring.head, count - 1)] + 1; // upTo is EXCLUSIVE
            bulkScorer.nextDocsAndScores(maxDocId, null, buffer);
            assert buffer.size == count;

            for (int d = 0; d < buffer.size; d++) {
                if (!Float.isNaN(buffer.features[d])) {
                    results.add(new ScoreDoc(buffer.docs[d] + docBase, buffer.features[d]));
                }
            }

            // return the number of docs scored
            return count;
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
    }
}
