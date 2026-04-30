/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocAndFloatFeatureBuffer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Exact knn query. Will iterate and score all documents that have the provided dense vector field in
 * the index. An optional filter restricts scoring to documents that also match that query.
 *
 * <p>Scoring uses two complementary bulk paths:
 * <ul>
 *   <li>{@link ScorerSupplier#bulkScorer()} returns a {@link BulkScorer} backed by
 *       {@link VectorScorer#bulk} for the top-level collection path, letting the similarity
 *       computation run in SIMD-friendly batches without per-document dispatch overhead.</li>
 *   <li>{@link ScorerSupplier#get(long)} returns a standard {@link Scorer} for the
 *       explain and conjunction paths where per-document scoring is required.</li>
 * </ul>
 */
public abstract class DenseVectorQuery extends Query {

    protected final String field;
    protected final Query filter;

    public DenseVectorQuery(String field, Query filter) {
        this.field = field;
        this.filter = filter;
    }

    @Override
    public void visit(QueryVisitor queryVisitor) {
        queryVisitor.visitLeaf(this);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        if (filter != null && filter.getClass() != MatchAllDocsQuery.class) {
            BooleanQuery booleanQuery = new BooleanQuery.Builder().add(filter, BooleanClause.Occur.FILTER)
                .add(new FieldExistsQuery(field), BooleanClause.Occur.FILTER)
                .build();
            Query rewritten = searcher.rewrite(booleanQuery);
            return rewritten.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f);
        } else {
            // If the filter is a match all docs query, we can skip it
            return null;
        }
    }

    abstract static class DenseVectorWeight extends Weight {
        private final String field;
        private final float boost;
        private final Weight filterWeight;

        protected DenseVectorWeight(DenseVectorQuery query, float boost, Weight filterWeight) {
            super(query);
            this.field = query.field;
            this.boost = boost;
            this.filterWeight = filterWeight;
        }

        abstract VectorScorer vectorScorer(LeafReaderContext leafReaderContext) throws IOException;

        @Override
        public Explanation explain(LeafReaderContext leafReaderContext, int i) throws IOException {
            if (filterWeight != null) {
                Explanation filterExplanation = filterWeight.explain(leafReaderContext, i);
                if (filterExplanation.isMatch() == false) {
                    return Explanation.noMatch("Document does not match filter", filterExplanation);
                }
            }
            VectorScorer vectorScorer = vectorScorer(leafReaderContext);
            if (vectorScorer == null) {
                return Explanation.noMatch("No vector values found for field: " + field);
            }
            DocIdSetIterator iterator = vectorScorer.iterator();
            iterator.advance(i);
            if (iterator.docID() == i) {
                float score = vectorScorer.score();
                return Explanation.match(score * boost, "found vector with calculated similarity: " + score);
            }
            return Explanation.noMatch("Document not found in vector values for field: " + field);
        }

        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
            VectorScorer vectorScorer = vectorScorer(context);
            if (vectorScorer == null) {
                return null;
            }
            final DocIdSetIterator filterIterator;
            final long cost;
            if (filterWeight != null) {
                ScorerSupplier filterSupplier = filterWeight.scorerSupplier(context);
                if (filterSupplier == null) {
                    return null;
                }
                filterIterator = filterSupplier.get(Long.MAX_VALUE).iterator();
                cost = Math.min(vectorScorer.iterator().cost(), filterIterator.cost());
            } else {
                filterIterator = null;
                cost = vectorScorer.iterator().cost();
            }
            return new ScorerSupplier() {
                @Override
                public Scorer get(long leadCost) throws IOException {
                    DocIdSetIterator iterator = filterIterator == null
                        ? vectorScorer.iterator()
                        : ConjunctionUtils.intersectIterators(List.of(vectorScorer.iterator(), filterIterator));
                    return new DenseVectorScorer(vectorScorer, iterator, boost);
                }

                @Override
                public BulkScorer bulkScorer() throws IOException {
                    return new DenseVectorBulkScorer(vectorScorer, filterIterator, boost, cost);
                }

                @Override
                public long cost() {
                    return cost;
                }
            };
        }

        @Override
        public boolean isCacheable(LeafReaderContext leafReaderContext) {
            return true;
        }
    }

    public static class Floats extends DenseVectorQuery {

        private final float[] query;

        public Floats(float[] query, String field, Query filter) {
            super(field, filter);
            this.query = query;
        }

        public float[] getQuery() {
            return query;
        }

        @Override
        public String toString(String field) {
            return "DenseVectorQuery.Floats";
        }

        @Override
        public Query rewrite(IndexSearcher indexSearcher) throws IOException {
            if (filter == null) return this;
            Query rewritten = indexSearcher.rewrite(filter);
            if (rewritten == filter) {
                return this;
            } else if (rewritten.getClass() == MatchNoDocsQuery.class) {
                return rewritten;
            } else {
                return new Floats(query, field, rewritten);
            }
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            Weight filterWeight = super.createWeight(searcher, scoreMode, boost);
            return new DenseVectorWeight(Floats.this, boost, filterWeight) {
                @Override
                VectorScorer vectorScorer(LeafReaderContext leafReaderContext) throws IOException {
                    FloatVectorValues vectorValues = leafReaderContext.reader().getFloatVectorValues(field);
                    if (vectorValues == null) {
                        return null;
                    }
                    return vectorValues.scorer(query);
                }
            };
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Floats floats = (Floats) o;
            return Objects.equals(field, floats.field) && Objects.deepEquals(query, floats.query) && Objects.equals(filter, floats.filter);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, Arrays.hashCode(query), filter);
        }
    }

    public static class Bytes extends DenseVectorQuery {

        private final byte[] query;

        public Bytes(byte[] query, String field, Query filter) {
            super(field, filter);
            this.query = query;
        }

        @Override
        public String toString(String field) {
            return "DenseVectorQuery.Bytes";
        }

        @Override
        public Query rewrite(IndexSearcher indexSearcher) throws IOException {
            if (filter == null) return this;
            Query rewritten = indexSearcher.rewrite(filter);
            if (rewritten.getClass() == MatchNoDocsQuery.class) {
                return rewritten;
            } else if (rewritten == filter) {
                return this;
            } else {
                return new Bytes(query, field, rewritten);
            }
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            Weight filterWeight = super.createWeight(searcher, scoreMode, boost);
            return new DenseVectorWeight(Bytes.this, boost, filterWeight) {
                @Override
                VectorScorer vectorScorer(LeafReaderContext leafReaderContext) throws IOException {
                    ByteVectorValues vectorValues = leafReaderContext.reader().getByteVectorValues(field);
                    if (vectorValues == null) {
                        return null;
                    }
                    return vectorValues.scorer(query);
                }
            };
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Bytes bytes = (Bytes) o;
            return Objects.equals(field, bytes.field) && Objects.deepEquals(query, bytes.query) && Objects.equals(filter, bytes.filter);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, Arrays.hashCode(query), filter);
        }
    }

    static class DenseVectorScorer extends Scorer {

        private final VectorScorer vectorScorer;
        private final DocIdSetIterator iterator;
        private final float boost;

        DenseVectorScorer(VectorScorer vectorScorer, DocIdSetIterator iterator, float boost) {
            this.vectorScorer = vectorScorer;
            this.iterator = iterator;
            this.boost = boost;
        }

        @Override
        public DocIdSetIterator iterator() {
            return iterator;
        }

        @Override
        public float getMaxScore(int i) throws IOException {
            // TODO: can we optimize this at all?
            return Float.POSITIVE_INFINITY;
        }

        @Override
        public float score() throws IOException {
            assert iterator.docID() != -1;
            return vectorScorer.score() * boost;
        }

        @Override
        public int docID() {
            return iterator.docID();
        }
    }

    private static class DenseVectorBulkScorer extends BulkScorer {
        private final DocAndFloatFeatureBuffer buffer;
        private final VectorScorer.Bulk bulkScorer;
        private final DocIdSetIterator vectorIterator;
        private final float boost;
        private final long cost;
        private float currentScore = 0f;
        private final Scorable scorable = new Scorable() {
            @Override
            public float score() {
                return currentScore;
            }
        };

        DenseVectorBulkScorer(VectorScorer vectorScorer, DocIdSetIterator filterIterator, float boost, long cost) throws IOException {
            this.bulkScorer = vectorScorer.bulk(filterIterator);
            this.vectorIterator = filterIterator == null
                ? vectorScorer.iterator()
                : ConjunctionUtils.intersectIterators(List.of(vectorScorer.iterator(), filterIterator));
            this.buffer = new DocAndFloatFeatureBuffer();
            this.boost = boost;
            this.cost = cost;
        }

        @Override
        public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
            collector.setScorer(scorable);

            if (vectorIterator.docID() < min) {
                vectorIterator.advance(min);
            }

            while (vectorIterator.docID() < max) {
                bulkScorer.nextDocsAndScores(max, acceptDocs, buffer);
                for (int i = 0; i < buffer.size; i++) {
                    int doc = buffer.docs[i];
                    // currentScore is closed over by scorable
                    currentScore = buffer.features[i] * boost;
                    collector.collect(doc);
                }
            }

            return vectorIterator.docID();
        }

        @Override
        public long cost() {
            return cost;
        }
    }

}
