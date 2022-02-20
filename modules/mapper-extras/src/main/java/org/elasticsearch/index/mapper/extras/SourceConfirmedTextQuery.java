/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafSimScorer;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.elasticsearch.common.CheckedIntFunction;
import org.elasticsearch.common.lucene.search.MultiPhrasePrefixQuery;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * A variant of {@link TermQuery}, {@link PhraseQuery}, {@link MultiPhraseQuery}
 * and span queries that uses postings for its approximation, but falls back to
 * stored fields or _source whenever term frequencies or positions are needed.
 * This query matches and scores the same way as the wrapped query.
 */
public final class SourceConfirmedTextQuery extends Query {

    /**
     * Create an approximation for the given query. The returned approximation
     * should match a superset of the matches of the provided query.
     */
    public static Query approximate(Query query) {
        if (query instanceof TermQuery) {
            return query;
        } else if (query instanceof PhraseQuery) {
            return approximate((PhraseQuery) query);
        } else if (query instanceof MultiPhraseQuery) {
            return approximate((MultiPhraseQuery) query);
        } else if (query instanceof MultiPhrasePrefixQuery) {
            return approximate((MultiPhrasePrefixQuery) query);
        } else {
            return new MatchAllDocsQuery();
        }
    }

    private static Query approximate(PhraseQuery query) {
        BooleanQuery.Builder approximation = new BooleanQuery.Builder();
        for (Term term : query.getTerms()) {
            approximation.add(new TermQuery(term), Occur.FILTER);
        }
        return approximation.build();
    }

    private static Query approximate(MultiPhraseQuery query) {
        BooleanQuery.Builder approximation = new BooleanQuery.Builder();
        for (Term[] termArray : query.getTermArrays()) {
            BooleanQuery.Builder approximationClause = new BooleanQuery.Builder();
            for (Term term : termArray) {
                approximationClause.add(new TermQuery(term), Occur.SHOULD);
            }
            approximation.add(approximationClause.build(), Occur.FILTER);
        }
        return approximation.build();
    }

    private static Query approximate(MultiPhrasePrefixQuery query) {
        Term[][] terms = query.getTerms();
        if (terms.length == 0) {
            return new MatchNoDocsQuery();
        } else if (terms.length == 1) {
            // Only a prefix, approximate with a prefix query
            BooleanQuery.Builder approximation = new BooleanQuery.Builder();
            for (Term term : terms[0]) {
                approximation.add(new PrefixQuery(term), Occur.FILTER);
            }
            return approximation.build();
        }
        // A combination of a phrase and a prefix query, only use terms of the phrase for the approximation
        BooleanQuery.Builder approximation = new BooleanQuery.Builder();
        for (int i = 0; i < terms.length - 1; ++i) { // ignore the last set of terms, which are prefixes
            Term[] termArray = terms[i];
            BooleanQuery.Builder approximationClause = new BooleanQuery.Builder();
            for (Term term : termArray) {
                approximationClause.add(new TermQuery(term), Occur.SHOULD);
            }
            approximation.add(approximationClause.build(), Occur.FILTER);
        }
        return approximation.build();
    }

    /**
     * Similarity that produces the frequency as a score.
     */
    private static final Similarity FREQ_SIMILARITY = new Similarity() {

        @Override
        public long computeNorm(FieldInvertState state) {
            return 1L;
        }

        public SimScorer scorer(float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
            return new SimScorer() {
                @Override
                public float score(float freq, long norm) {
                    return freq;
                }
            };
        }
    };

    private final Query in;
    private final Function<LeafReaderContext, CheckedIntFunction<List<Object>, IOException>> valueFetcherProvider;
    private final Analyzer indexAnalyzer;

    public SourceConfirmedTextQuery(
        Query in,
        Function<LeafReaderContext, CheckedIntFunction<List<Object>, IOException>> valueFetcherProvider,
        Analyzer indexAnalyzer
    ) {
        this.in = in;
        this.valueFetcherProvider = valueFetcherProvider;
        this.indexAnalyzer = indexAnalyzer;
    }

    public Query getQuery() {
        return in;
    }

    @Override
    public String toString(String field) {
        return in.toString(field);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        SourceConfirmedTextQuery that = (SourceConfirmedTextQuery) obj;
        return Objects.equals(in, that.in)
            && Objects.equals(valueFetcherProvider, that.valueFetcherProvider)
            && Objects.equals(indexAnalyzer, that.indexAnalyzer);
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hash(in, valueFetcherProvider, indexAnalyzer) + classHash();
    }

    @Override
    public void visit(QueryVisitor visitor) {
        in.visit(visitor.getSubVisitor(Occur.MUST, this));
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        Query inRewritten = in.rewrite(reader);
        if (inRewritten != in) {
            return new SourceConfirmedTextQuery(inRewritten, valueFetcherProvider, indexAnalyzer);
        } else if (in instanceof ConstantScoreQuery) {
            Query sub = ((ConstantScoreQuery) in).getQuery();
            return new ConstantScoreQuery(new SourceConfirmedTextQuery(sub, valueFetcherProvider, indexAnalyzer));
        } else if (in instanceof BoostQuery) {
            Query sub = ((BoostQuery) in).getQuery();
            float boost = ((BoostQuery) in).getBoost();
            return new BoostQuery(new SourceConfirmedTextQuery(sub, valueFetcherProvider, indexAnalyzer), boost);
        } else if (in instanceof MatchNoDocsQuery) {
            return in; // e.g. empty phrase query
        }
        return super.rewrite(reader);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        if (scoreMode.needsScores() == false && in instanceof TermQuery) {
            // No need to ever look at the _source for non-scoring term queries
            return in.createWeight(searcher, scoreMode, boost);
        }

        final Set<Term> terms = new HashSet<>();
        in.visit(QueryVisitor.termCollector(terms));
        if (terms.isEmpty()) {
            throw new IllegalStateException("Query " + in + " doesn't have any term");
        }
        final String field = terms.iterator().next().field();
        final CollectionStatistics collectionStatistics = searcher.collectionStatistics(field);
        final SimScorer simScorer;
        final Weight approximationWeight;
        if (collectionStatistics == null) {
            // field does not exist in the index
            simScorer = null;
            approximationWeight = null;
        } else {
            final Map<Term, TermStates> termStates = new HashMap<>();
            final List<TermStatistics> termStats = new ArrayList<>();
            for (Term term : terms) {
                TermStates ts = termStates.computeIfAbsent(term, t -> {
                    try {
                        return TermStates.build(searcher.getTopReaderContext(), t, scoreMode.needsScores());
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
                if (scoreMode.needsScores()) {
                    if (ts.docFreq() > 0) {
                        termStats.add(searcher.termStatistics(term, ts.docFreq(), ts.totalTermFreq()));
                    }
                } else {
                    termStats.add(new TermStatistics(term.bytes(), 1, 1L));
                }
            }
            simScorer = searcher.getSimilarity().scorer(boost, collectionStatistics, termStats.toArray(TermStatistics[]::new));
            approximationWeight = searcher.createWeight(approximate(in), ScoreMode.COMPLETE_NO_SCORES, 1f);
        }
        return new Weight(this) {

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                // Don't cache queries that may perform linear scans
                return false;
            }

            @Override
            public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                RuntimePhraseScorer scorer = scorer(context);
                if (scorer == null) {
                    return Explanation.noMatch("No matching phrase");
                }
                final TwoPhaseIterator twoPhase = scorer.twoPhaseIterator();
                if (twoPhase.approximation().advance(doc) != doc || scorer.twoPhaseIterator().matches() == false) {
                    return Explanation.noMatch("No matching phrase");
                }
                float phraseFreq = scorer.freq();
                Explanation freqExplanation = Explanation.match(phraseFreq, "phraseFreq=" + phraseFreq);
                final LeafSimScorer leafSimScorer = new LeafSimScorer(simScorer, context.reader(), field, scoreMode.needsScores());
                Explanation scoreExplanation = leafSimScorer.explain(doc, freqExplanation);
                return Explanation.match(
                    scoreExplanation.getValue(),
                    "weight(" + getQuery() + " in " + doc + ") [" + searcher.getSimilarity().getClass().getSimpleName() + "], result of:",
                    scoreExplanation
                );
            }

            @Override
            public RuntimePhraseScorer scorer(LeafReaderContext context) throws IOException {
                final Scorer approximationScorer = approximationWeight != null ? approximationWeight.scorer(context) : null;
                if (approximationScorer == null) {
                    return null;
                }
                final DocIdSetIterator approximation = approximationScorer.iterator();
                final LeafSimScorer leafSimScorer = new LeafSimScorer(simScorer, context.reader(), field, scoreMode.needsScores());
                final CheckedIntFunction<List<Object>, IOException> valueFetcher = valueFetcherProvider.apply(context);
                return new RuntimePhraseScorer(this, approximation, leafSimScorer, valueFetcher, field, in);
            }

        };
    }

    private class RuntimePhraseScorer extends Scorer {

        private final LeafSimScorer scorer;
        private final CheckedIntFunction<List<Object>, IOException> valueFetcher;
        private final String field;
        private final Query query;
        private final TwoPhaseIterator twoPhase;

        private int doc = -1;
        private float freq;

        private RuntimePhraseScorer(
            Weight weight,
            DocIdSetIterator approximation,
            LeafSimScorer scorer,
            CheckedIntFunction<List<Object>, IOException> valueFetcher,
            String field,
            Query query
        ) {
            super(weight);
            this.scorer = scorer;
            this.valueFetcher = valueFetcher;
            this.field = field;
            this.query = query;
            twoPhase = new TwoPhaseIterator(approximation) {

                @Override
                public boolean matches() throws IOException {
                    return freq() > 0;
                }

                @Override
                public float matchCost() {
                    // TODO what is a right value?
                    // Defaults to a high-ish value so that it likely runs last.
                    return 10_000f;
                }

            };
        }

        @Override
        public DocIdSetIterator iterator() {
            return TwoPhaseIterator.asDocIdSetIterator(twoPhaseIterator());
        }

        @Override
        public TwoPhaseIterator twoPhaseIterator() {
            return twoPhase;
        }

        @Override
        public float getMaxScore(int upTo) throws IOException {
            return scorer.getSimScorer().score(Float.MAX_VALUE, 1L);
        }

        @Override
        public float score() throws IOException {
            return scorer.score(docID(), freq());
        }

        @Override
        public int docID() {
            return twoPhase.approximation().docID();
        }

        private float freq() throws IOException {
            if (doc != docID()) {
                doc = docID();
                freq = computeFreq();
            }
            return freq;
        }

        private float computeFreq() throws IOException {
            MemoryIndex index = new MemoryIndex();
            index.setSimilarity(FREQ_SIMILARITY);
            List<Object> values = valueFetcher.apply(docID());
            float frequency = 0;
            for (Object value : values) {
                if (value == null) {
                    continue;
                }
                index.addField(field, value.toString(), indexAnalyzer);
                frequency += index.search(query);
                index.reset();
            }
            return frequency;
        }
    }

}
