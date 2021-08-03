package org.elasticsearch.xpack.wildcard.mapper;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.ConjunctionDISI;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.Scorable.ChildScorable;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * A wrapper for two query clauses, one a cheap approximation and the other a 
 * more expensive verification. (The same principle as chainsaw and fine sandpaper,
 * one quickly removes the bulk of what's needed but the other is required for 
 * producing a final finish).
 * Wrapping the clauses in a BooleanQuery as two "musts" rather than using this class 
 * also works but there's an important disadvantage - the caching framework harvests 
 * the internals of BooleanQueries, trying to find expensive clauses to cache. 
 * Because verification clauses (the fine sandpaper) only ever get used in conjunction 
 * with the results from a rough approximation we want to prevent them being used by 
 * the query caching logic on the whole index (the equivalent of turning a large tree 
 * into a food bowl only using fine sandpaper). 
 * Equally, the approximation query provides inconclusive results that need further 
 * treatment, so this clause would never be used in isolation (you don't make a food bowl
 * using just a chainsaw).
 * 
 * This ApproximationAndVerificationQuery class provides
 * an opaque wrapper whose results can be cached as a whole but whose component parts 
 * cannot be visible to the caching framework. 
 */
public final class ApproximationAndVerificationQuery extends Query {

    private final Query approxQuery, verifyQuery;

    /**
     * Create an {@link ApproximationAndVerificationQuery}. 
     * @param approxQuery a query that is quick to run across many documents but is only approximate
     * @param verifyQuery a query used to verify matches (slowly) on individual matches from approxQuery
     */
    public ApproximationAndVerificationQuery(Query approxQuery, Query verifyQuery) {
        this.approxQuery = approxQuery;
        this.verifyQuery = verifyQuery;
    }

    public Query getApproximationQuery() {
        return approxQuery;
    }

    public Query getVerificationQuery() {
        return verifyQuery;
    }

    @Override
    public String toString(String field) {
        return verifyQuery.toString(field);
    }

    @Override
    public boolean equals(Object obj) {
        if (sameClassAs(obj) == false) {
            return false;
        }
        ApproximationAndVerificationQuery that = (ApproximationAndVerificationQuery) obj;
        return approxQuery.equals(that.approxQuery) && verifyQuery.equals(that.verifyQuery);
    }

    @Override
    public int hashCode() {
        int h = classHash();
        h = 31 * h + approxQuery.hashCode();
        h = 31 * h + verifyQuery.hashCode();
        return h;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        Query approxRewrite = approxQuery.rewrite(reader);
        Query verifyRewrite = verifyQuery.rewrite(reader);
        if (approxQuery != approxRewrite || verifyQuery != verifyRewrite) {
            return new ApproximationAndVerificationQuery(approxRewrite, verifyRewrite);
        }
        return this;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        QueryVisitor v = visitor.getSubVisitor(BooleanClause.Occur.MUST, this);
        approxQuery.visit(v);
        verifyQuery.visit(v);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        final Weight approxWeight = approxQuery.createWeight(searcher, scoreMode, boost);
        final Weight verifyWeight = verifyQuery.createWeight(searcher, scoreMode, boost);

        class ApproxAndVerifyWeight extends Weight {

            protected ApproxAndVerifyWeight(Query query) {
                super(query);
            }

            @Override
            public Matches matches(LeafReaderContext context, int doc) throws IOException {
                return verifyWeight.matches(context, doc);
            }

            @Override
            public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                return verifyWeight.explain(context, doc);
            }

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                final ScorerSupplier approxScorerSupplier = approxWeight.scorerSupplier(context);
                final ScorerSupplier verifyScorerSupplier = verifyWeight.scorerSupplier(context);
                if (approxScorerSupplier == null || verifyScorerSupplier == null) {
                    return null;
                }
                return new ScorerSupplier() {

                    @Override
                    public Scorer get(long leadCost) throws IOException {
                        return new ConjunctionScorer(
                            ApproxAndVerifyWeight.this,
                            approxScorerSupplier.get(leadCost),
                            verifyScorerSupplier.get(leadCost)
                        );

                    }

                    @Override
                    public long cost() {
                        return approxScorerSupplier.cost();
                    }

                    /** Scorer for conjunction of the two queries */
                    class ConjunctionScorer extends Scorer {

                        final DocIdSetIterator disi;
                        Scorer verifyScorer;
                        Scorer approxScorer;
                        private DocIdSetIterator approxDisi;
                        private DocIdSetIterator verifyDisi;

                        ConjunctionScorer(Weight weight, Scorer approxScorer, Scorer verifyScorer) throws IOException {
                            super(weight);
                            this.approxDisi = approxScorer.iterator();
                            this.verifyDisi = verifyScorer.iterator();
                            this.disi = ConjunctionDISI.intersectIterators(List.of(approxDisi, verifyDisi));
                            this.approxScorer = approxScorer;
                            this.verifyScorer = verifyScorer;
                        }

                        @Override
                        public TwoPhaseIterator twoPhaseIterator() {
                            return new TwoPhaseIterator(approxDisi) {

                                @Override
                                public boolean matches() throws IOException {

                                    if (verifyDisi.docID() < approxDisi.docID()) {
                                        verifyDisi.advance(approxDisi.docID());
                                    }
                                    return verifyDisi.docID() == approxDisi.docID();

                                }

                                @Override
                                public float matchCost() {
                                    return 1000f;
                                }
                            };
                        }

                        @Override
                        public DocIdSetIterator iterator() {
                            return disi;
                        }

                        @Override
                        public int docID() {
                            return disi.docID();
                        }

                        @Override
                        public float score() throws IOException {
                            return approxScorer.score() + verifyScorer.score();
                        }

                        @Override
                        public float getMaxScore(int upTo) throws IOException {
                            return Float.POSITIVE_INFINITY;
                        }

                        // For profiling purposes
                        @Override
                        public Collection<ChildScorable> getChildren() {
                            return List.of(new ChildScorable(approxScorer, "MUST"), new ChildScorable(verifyScorer, "MUST"));
                        }

                    }

                };

            }

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                ScorerSupplier scorerSupplier = scorerSupplier(context);
                if (scorerSupplier == null) {
                    return null;
                }
                return scorerSupplier.get(Long.MAX_VALUE);
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                // Neither of the contained queries is intended to be cached on their own but their conjunction is.
                return true;
            }

             @Override
             public void extractTerms(Set<Term> terms) {
                 verifyWeight.extractTerms(terms);
                 approxWeight.extractTerms(terms);           
             }

        }

        return new ApproxAndVerifyWeight(this);
    }

}

