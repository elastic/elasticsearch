/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Objects;

/**
 * Matches every document the wrapped query does not, as a clause that can be required rather than prohibited.
 *
 * <p>Lucene runs a {@code MUST_NOT} clause with {@code ReqExclBulkScorer}, which walks the exclusion's own
 * approximation and confirms it on every document it reaches, wanted by the other clauses or not. That is the right
 * way round for an exclusion backed by an inverted index, whose approximation is a short list of candidates. A
 * {@link ColumnarScanQuery} has no such list: it can reach every document, and confirming one decodes column data.
 * Excluding with it that way decodes most of the column however few documents the other clauses leave.
 *
 * <p>Required, the same exclusion is a two-phase clause that matches everywhere and confirms by asking whether the
 * wrapped query does <em>not</em> match. Lucene's conjunction orders such clauses by {@link TwoPhaseIterator#matchCost}
 * and confirms the later ones only on documents the earlier ones kept, so a cheap, selective clause decides which
 * chunks of the wrapped query's column are ever read.
 *
 * <p>The answer is the one {@code MUST_NOT} gives: a document the wrapped query has no opinion on, such as one without
 * the field, is not excluded, so it matches here. The clause is constant-scoring, as a prohibited clause is.
 *
 * <p>Use {@link #asFilter} to choose between the two forms; wrapping a query that Lucene already excludes well only
 * adds a confirmation per document.
 */
public final class ColumnarNegatedQuery extends Query {

    /**
     * What it costs to find out whether the wrapped query reaches a document, on top of confirming it. An estimate
     * in the unit {@link TwoPhaseIterator#matchCost} uses, which is only ever compared with other clauses' costs.
     */
    private static final float PROBE_COST = 10f;

    private final Query negated;

    public ColumnarNegatedQuery(Query negated) {
        this.negated = Objects.requireNonNull(negated);
    }

    /**
     * The clause to require in place of prohibiting {@code exclusion}, or null when it should stay prohibited.
     *
     * <p>Only an exclusion that reads a column qualifies. That is decided from the query's own tree, through
     * {@link Query#visit}, because by the time a boolean query is built the exclusion is usually wrapped in the
     * single-value checks and filters ES|QL puts around it.
     */
    public static Query asFilter(Query exclusion) {
        return scansColumn(exclusion) ? new ColumnarNegatedQuery(exclusion) : null;
    }

    /** Whether {@code query}, or something it contains, is a {@link ColumnarScanQuery}. */
    static boolean scansColumn(Query query) {
        final boolean[] found = new boolean[1];
        query.visit(new QueryVisitor() {
            @Override
            public void visitLeaf(Query leaf) {
                if (leaf instanceof ColumnarScanQuery) {
                    found[0] = true;
                }
            }
        });
        return found[0];
    }

    /** The query this one negates. */
    public Query negated() {
        return negated;
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        final Query rewritten = negated.rewrite(searcher);
        if (rewritten instanceof MatchNoDocsQuery) {
            return new MatchAllDocsQuery();
        }
        if (rewritten instanceof MatchAllDocsQuery) {
            return new MatchNoDocsQuery("negation of a query matching every document");
        }
        return rewritten == negated ? this : new ColumnarNegatedQuery(rewritten);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        final Weight negatedWeight = negated.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f);
        return new ConstantScoreWeight(this, boost) {
            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                final ScorerSupplier negatedSupplier = negatedWeight.scorerSupplier(context);
                final int maxDoc = context.reader().maxDoc();
                return new ScorerSupplier() {
                    @Override
                    public Scorer get(long leadCost) throws IOException {
                        // No supplier means the wrapped query matches nothing in this segment, so nothing is excluded.
                        final Scorer negatedScorer = negatedSupplier == null ? null : negatedSupplier.get(leadCost);
                        return new ConstantScoreScorer(score(), scoreMode, new Negation(maxDoc, negatedScorer));
                    }

                    @Override
                    public long cost() {
                        return maxDoc;
                    }
                };
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return negatedWeight.isCacheable(ctx);
            }
        };
    }

    /**
     * Every document is a candidate, and a candidate is confirmed when the wrapped query's scorer does not match it.
     */
    private static final class Negation extends TwoPhaseIterator {
        private final TwoPhaseIterator negatedTwoPhase;
        private final DocIdSetIterator negatedApproximation;

        Negation(int maxDoc, Scorer negated) {
            super(new AllDocs(maxDoc));
            this.negatedTwoPhase = negated == null ? null : negated.twoPhaseIterator();
            this.negatedApproximation = negated == null ? null
                : negatedTwoPhase != null ? negatedTwoPhase.approximation()
                : negated.iterator();
        }

        @Override
        public boolean matches() throws IOException {
            if (negatedApproximation == null) {
                return true;
            }
            final int doc = approximation.docID();
            int reached = negatedApproximation.docID();
            if (reached < doc) {
                reached = negatedApproximation.advance(doc);
            }
            if (reached != doc) {
                return true;
            }
            // The wrapped query reaches this document. Without a confirmation, reaching it is matching it.
            return negatedTwoPhase != null && negatedTwoPhase.matches() == false;
        }

        @Override
        public float matchCost() {
            return PROBE_COST + (negatedTwoPhase == null ? 0f : negatedTwoPhase.matchCost());
        }
    }

    /**
     * Every document of the segment. It reports no run end, which keeps a conjunction from treating this clause as
     * matching a whole window: it has to be confirmed document by document.
     */
    private static final class AllDocs extends DocIdSetIterator {
        private final int maxDoc;
        private int doc = -1;

        AllDocs(int maxDoc) {
            this.maxDoc = maxDoc;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() {
            return advance(doc + 1);
        }

        @Override
        public int advance(int target) {
            return doc = target >= maxDoc ? NO_MORE_DOCS : target;
        }

        @Override
        public long cost() {
            return maxDoc;
        }
    }

    @Override
    public void visit(QueryVisitor visitor) {
        negated.visit(visitor.getSubVisitor(BooleanClause.Occur.MUST_NOT, this));
    }

    @Override
    public String toString(String field) {
        return "NOT(" + negated.toString(field) + ")";
    }

    @Override
    public boolean equals(Object other) {
        return sameClassAs(other) && negated.equals(((ColumnarNegatedQuery) other).negated);
    }

    @Override
    public int hashCode() {
        return 31 * classHash() + negated.hashCode();
    }
}
