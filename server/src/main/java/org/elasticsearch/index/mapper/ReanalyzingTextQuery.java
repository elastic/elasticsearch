/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
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
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOFunction;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.common.CheckedIntFunction;
import org.elasticsearch.common.lucene.search.MultiPhrasePrefixQuery;
import org.elasticsearch.common.lucene.search.Queries;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * A variant of {@link TermQuery}, {@link PhraseQuery}, {@link MultiPhraseQuery}
 * and span queries that uses postings for its approximation and analyzes the
 * document's own values again wherever term frequencies or positions are needed.
 * Where those values live is the caller's to say; see {@link FieldValueFetchers}.
 * This query matches and scores the same way as the wrapped query.
 */
public final class ReanalyzingTextQuery extends Query {

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
            return Queries.ALL_DOCS_INSTANCE;
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
            return Queries.NO_DOCS_INSTANCE;
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
     * The terms of a phrase that can be confirmed by walking a document's values rather than indexing them, or null
     * where it cannot: anything but an exact phrase, whose terms sit at consecutive positions, on one field.
     */
    static Term[] walkablePhrase(Query query) {
        if (query instanceof PhraseQuery phrase && phrase.getSlop() == 0) {
            final Term[] terms = phrase.getTerms();
            final int[] positions = phrase.getPositions();
            if (terms.length == 0) {
                return null;
            }
            for (int i = 0; i < positions.length; i++) {
                if (positions[i] != i) {
                    return null;
                }
            }
            return terms;
        }
        return null;
    }

    /**
     * How often {@code terms} occur in order and adjacent across {@code values}, which is the frequency an index of
     * them reports. Positions run on from one value to the next, as that index joins them. {@code countEvery} is
     * false where only the presence of the phrase is asked, and the walk then stops at the first one.
     *
     * <p>A prefix of the phrase can only be continued by the token at the position after the one it ended at, and
     * positions only advance, so one end position per prefix length is all there is to carry. What ends at the
     * position in hand is held apart until that position is done, since several tokens can share one and a prefix
     * starting on one of them must not be offered to the others.
     */
    static int walkPhraseFreq(Term[] terms, String field, Analyzer analyzer, List<Object> values) throws IOException {
        return walkPhraseFreq(terms, field, analyzer, values, true);
    }

    static int walkPhraseFreq(Term[] terms, String field, Analyzer analyzer, List<Object> values, boolean countEvery) throws IOException {
        final BytesRef[] bytes = new BytesRef[terms.length];
        for (int i = 0; i < terms.length; i++) {
            bytes[i] = terms[i].bytes();
        }
        final TokenStreamMatching.PhraseWalker walker = new TokenStreamMatching.PhraseWalker(bytes, countEvery);
        final int gap = analyzer.getPositionIncrementGap(field);
        boolean firstValue = true;
        for (Object value : values) {
            if (value == null) {
                continue;
            }
            if (firstValue) {
                firstValue = false;
            } else {
                // The analyzer's gap sits between two values, as it does when the same values are indexed.
                walker.skip(gap);
            }
            final String text = value instanceof BytesRef valueBytes ? valueBytes.utf8ToString() : value.toString();
            try (TokenStream stream = analyzer.tokenStream(field, text)) {
                stream.reset();
                walker.accept(stream);
                if (countEvery == false && walker.freq() > 0) {
                    return walker.freq();
                }
                stream.end();
            }
        }
        return walker.freq();
    }

    /**
     * How a frequency is counted where nothing is indexed: a clause the document answers counts once, whatever its
     * frequency, so the clauses a query names bound what it can report. {@link #scanMaxFreq} is that bound and has
     * to change with this rule.
     */
    private static final Similarity MATCHED_CLAUSES_SIMILARITY = new Similarity() {

        @Override
        public long computeNorm(FieldInvertState state) {
            return 1L;
        }

        @Override
        public SimScorer scorer(float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
            return new SimScorer() {
                @Override
                public float score(float freq, long norm) {
                    return freq > 0 ? 1f : 0f;
                }
            };
        }
    };

    /** Scores the clauses a document answered, which {@link #MATCHED_CLAUSES_SIMILARITY} counts one each. */
    private static SimScorer matchedClausesScorer(float boost) {
        return new SimScorer() {
            @Override
            public float score(float freq, long norm) {
                return freq * boost;
            }
        };
    }

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
    private final IOFunction<LeafReaderContext, CheckedIntFunction<List<Object>, IOException>> valueFetcherProvider;
    private final Analyzer indexAnalyzer;
    private final boolean scansEveryDocument;

    public ReanalyzingTextQuery(
        Query in,
        IOFunction<LeafReaderContext, CheckedIntFunction<List<Object>, IOException>> valueFetcherProvider,
        Analyzer indexAnalyzer
    ) {
        this(in, valueFetcherProvider, indexAnalyzer, false);
    }

    /**
     * @param scansEveryDocument whether the field indexes no terms, leaving no postings to narrow the documents read
     *                           or to weigh a term
     */
    public ReanalyzingTextQuery(
        Query in,
        IOFunction<LeafReaderContext, CheckedIntFunction<List<Object>, IOException>> valueFetcherProvider,
        Analyzer indexAnalyzer,
        boolean scansEveryDocument
    ) {
        this.in = in;
        this.valueFetcherProvider = valueFetcherProvider;
        this.indexAnalyzer = indexAnalyzer;
        this.scansEveryDocument = scansEveryDocument;
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
        ReanalyzingTextQuery that = (ReanalyzingTextQuery) obj;
        // We intentionally do not compare the value fetcher or analyzer, as they
        // do not typically implement equals() themselves, and the inner
        // Query is sufficient to establish identity.
        return Objects.equals(in, that.in) && scansEveryDocument == that.scansEveryDocument;
    }

    @Override
    public int hashCode() {
        // We intentionally do not hash the value fetcher or analyzer, as they
        // do not typically implement hashCode() themselves, and the inner
        // Query is sufficient to establish identity.
        return 31 * Objects.hash(in, scansEveryDocument) + classHash();
    }

    @Override
    public void visit(QueryVisitor visitor) {
        in.visit(visitor.getSubVisitor(Occur.MUST, this));
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        // A term the index does not hold rewrites away, so where it holds none the query is left as it is.
        Query inRewritten = scansEveryDocument ? in : in.rewrite(searcher);
        if (inRewritten != in) {
            return new ReanalyzingTextQuery(inRewritten, valueFetcherProvider, indexAnalyzer, scansEveryDocument);
        } else if (in instanceof ConstantScoreQuery) {
            Query sub = ((ConstantScoreQuery) in).getQuery();
            return new ConstantScoreQuery(new ReanalyzingTextQuery(sub, valueFetcherProvider, indexAnalyzer, scansEveryDocument));
        } else if (in instanceof BoostQuery) {
            Query sub = ((BoostQuery) in).getQuery();
            float boost = ((BoostQuery) in).getBoost();
            return new BoostQuery(new ReanalyzingTextQuery(sub, valueFetcherProvider, indexAnalyzer, scansEveryDocument), boost);
        } else if (in instanceof MatchNoDocsQuery) {
            return in; // e.g. empty phrase query
        }
        return super.rewrite(searcher);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        if (scoreMode.needsScores() == false && in instanceof TermQuery && scansEveryDocument == false) {
            // No need to ever look at the _source for non-scoring term queries
            return in.createWeight(searcher, scoreMode, boost);
        }
        // We use a LinkedHashSet here to preserve the ordering of terms to ensure that
        // later summing of float scores per term is consistent
        final Set<Term> terms = new LinkedHashSet<>();
        in.visit(QueryVisitor.termCollector(terms));
        final String field;
        if (terms.isEmpty()) {
            // A query over a range of terms - a fuzziness, a prefix - names none of them, only the field it reads.
            field = scansEveryDocument ? fieldOf(in) : null;
            if (field == null) {
                throw new IllegalStateException("Query " + in + " doesn't have any term");
            }
        } else {
            field = terms.iterator().next().field();
        }
        final CollectionStatistics collectionStatistics = searcher.collectionStatistics(field);
        final SimScorer simScorer;
        final Weight approximationWeight;
        if (scansEveryDocument) {
            // Every document is read and nothing weighs a term, so the clauses answered are the whole score.
            simScorer = matchedClausesScorer(boost);
            approximationWeight = searcher.createWeight(Queries.ALL_DOCS_INSTANCE, ScoreMode.COMPLETE_NO_SCORES, 1f);
        } else if (collectionStatistics == null) {
            // field does not exist in the index
            simScorer = null;
            approximationWeight = null;
        } else {
            final Map<Term, TermStates> termStates = new HashMap<>();
            final List<TermStatistics> termStats = new ArrayList<>();
            for (Term term : terms) {
                TermStates ts = termStates.computeIfAbsent(term, t -> {
                    try {
                        return TermStates.build(searcher, t, scoreMode.needsScores());
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
            if (termStats.size() > 0) {
                simScorer = searcher.getSimilarity().scorer(boost, collectionStatistics, termStats.toArray(TermStatistics[]::new));
                approximationWeight = searcher.createWeight(approximate(in), ScoreMode.COMPLETE_NO_SCORES, 1f);
            } else {
                simScorer = null;
                approximationWeight = null;
            }
        }
        final float maxFreq = scansEveryDocument ? scanMaxFreq(in, terms) : Float.MAX_VALUE;
        return new Weight(this) {

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                // Don't cache queries that may perform linear scans
                return false;
            }

            @Override
            public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                NumericDocValues norms = context.reader().getNormValues(field);
                ScorerSupplier scorerSupplier = scorerSupplier(context);
                if (scorerSupplier == null) {
                    return Explanation.noMatch("No matching phrase");
                }
                ReanalyzingScorer scorer = (ReanalyzingScorer) scorerSupplier.get(0);
                if (scorer == null) {
                    return Explanation.noMatch("No matching phrase");
                }
                final TwoPhaseIterator twoPhase = scorer.twoPhaseIterator();
                if (twoPhase.approximation().advance(doc) != doc || scorer.twoPhaseIterator().matches() == false) {
                    return Explanation.noMatch("No matching phrase");
                }
                float phraseFreq = scorer.freq();
                Explanation freqExplanation = Explanation.match(phraseFreq, "phraseFreq=" + phraseFreq);
                assert simScorer != null;
                Explanation scoreExplanation = simScorer.explain(freqExplanation, getNormValue(norms, doc));
                return Explanation.match(
                    scoreExplanation.getValue(),
                    "weight(" + getQuery() + " in " + doc + ") [" + searcher.getSimilarity().getClass().getSimpleName() + "], result of:",
                    scoreExplanation
                );
            }

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                ScorerSupplier approximationSupplier = approximationWeight != null ? approximationWeight.scorerSupplier(context) : null;
                if (approximationSupplier == null) {
                    return null;
                }
                return new ScorerSupplier() {
                    @Override
                    public Scorer get(long leadCost) throws IOException {
                        final Scorer approximationScorer = approximationSupplier.get(leadCost);
                        final DocIdSetIterator approximation = approximationScorer.iterator();
                        final CheckedIntFunction<List<Object>, IOException> valueFetcher = valueFetcherProvider.apply(context);
                        NumericDocValues norms = context.reader().getNormValues(field);
                        return new ReanalyzingScorer(approximation, simScorer, norms, valueFetcher, field, in, maxFreq);
                    }

                    @Override
                    public long cost() {
                        return approximationSupplier.cost();
                    }
                };
            }

            @Override
            public Matches matches(LeafReaderContext context, int doc) throws IOException {
                var terms = context.reader().terms(field);
                if (terms == null && scansEveryDocument == false) {
                    return null;
                }
                // Some highlighters will already have re-indexed the source with positions and offsets,
                // so rather than doing it again we check to see if this data is available on the
                // current context and if so delegate directly to the inner query
                if (terms != null && terms.hasOffsets()) {
                    Weight innerWeight = in.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1);
                    return innerWeight.matches(context, doc);
                }
                ScorerSupplier scorerSupplier = scorerSupplier(context);
                if (scorerSupplier == null) {
                    return null;
                }
                ReanalyzingScorer scorer = (ReanalyzingScorer) scorerSupplier.get(0L);
                if (scorer == null) {
                    return null;
                }
                final TwoPhaseIterator twoPhase = scorer.twoPhaseIterator();
                if (twoPhase.approximation().advance(doc) != doc || scorer.twoPhaseIterator().matches() == false) {
                    return null;
                }
                return scorer.matches();
            }
        };
    }

    /** The field {@code query} reads, whether it names its terms or a range of them. */
    private static String fieldOf(Query query) {
        final String[] found = new String[1];
        query.visit(new QueryVisitor() {
            @Override
            public void consumeTerms(Query query, Term... terms) {
                if (found[0] == null && terms.length > 0) {
                    found[0] = terms[0].field();
                }
            }

            @Override
            public void consumeTermsMatching(Query query, String field, Supplier<ByteRunAutomaton> automaton) {
                if (found[0] == null) {
                    found[0] = field;
                }
            }
        });
        return found[0];
    }

    /**
     * The highest frequency a document can report where nothing is indexed: {@link #MATCHED_CLAUSES_SIMILARITY} holds
     * a clause the document answers to one point, and a phrase is one clause however often the document holds it. The
     * scorer turns the bound into a score through the similarity in hand, so a collector can stop once no document
     * left can beat what it holds. Unbounded where the query names no terms to count, a fuzziness or a prefix.
     */
    private static float scanMaxFreq(Query in, Set<Term> terms) {
        if (walkablePhrase(in) != null) {
            return 1f;
        }
        return terms.isEmpty() ? Float.MAX_VALUE : terms.size();
    }

    private static long getNormValue(NumericDocValues norms, int doc) throws IOException {
        if (norms != null) {
            boolean found = norms.advanceExact(doc);
            assert found;
            return norms.longValue();
        } else {
            return 1L; // default norm
        }
    }

    private class ReanalyzingScorer extends Scorer {
        private final SimScorer scorer;
        private final CheckedIntFunction<List<Object>, IOException> valueFetcher;
        private final String field;
        private final Query query;
        private final TwoPhaseIterator twoPhase;
        private final NumericDocValues norms;

        private final float maxFreq;
        private final MemoryIndexEntry cacheEntry = new MemoryIndexEntry();
        private final Term[] walkablePhrase;
        private int valuesDocID = -1;
        private List<Object> values;

        private int doc = -1;
        private float freq;

        private ReanalyzingScorer(
            DocIdSetIterator approximation,
            SimScorer scorer,
            NumericDocValues norms,
            CheckedIntFunction<List<Object>, IOException> valueFetcher,
            String field,
            Query query,
            float maxFreq
        ) {
            this.scorer = scorer;
            this.norms = norms;
            this.valueFetcher = valueFetcher;
            this.field = field;
            this.query = query;
            this.maxFreq = maxFreq;
            this.walkablePhrase = walkablePhrase(query);
            twoPhase = new TwoPhaseIterator(approximation) {

                @Override
                public boolean matches() throws IOException {
                    return freq() > 0;
                }

                @Override
                public float matchCost() {
                    // Reading the values and analyzing them dominates either way, so both stay high enough to run
                    // last among cheaper checks. The walk compares terms as they come rather than indexing them all.
                    return walkablePhrase != null ? 1_000f : 10_000f;
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
            return scorer.score(maxFreq, 1L);
        }

        @Override
        public float score() throws IOException {
            return scorer.score(freq(), getNormValue(norms, doc));
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

        private MemoryIndex getOrCreateMemoryIndex() throws IOException {
            if (cacheEntry.docID != docID()) {
                cacheEntry.docID = docID();
                // One index per scorer, emptied between documents: the buffers it holds are the point of keeping it.
                if (cacheEntry.memoryIndex == null) {
                    cacheEntry.memoryIndex = new MemoryIndex(true, false);
                } else {
                    cacheEntry.memoryIndex.reset();
                }
                // Each clause the document answers counts once; elsewhere the frequency is what the similarity asks for.
                cacheEntry.memoryIndex.setSimilarity(scansEveryDocument ? MATCHED_CLAUSES_SIMILARITY : FREQ_SIMILARITY);
                for (Object value : values()) {
                    if (value == null) {
                        continue;
                    }
                    String valueStr;
                    if (value instanceof BytesRef valueRef) {
                        valueStr = valueRef.utf8ToString();
                    } else {
                        valueStr = value.toString();
                    }
                    cacheEntry.memoryIndex.addField(field, valueStr, indexAnalyzer);
                }
            }
            return cacheEntry.memoryIndex;
        }

        /** The document's values, read once however many of the paths below ask for them: reading is what costs. */
        private List<Object> values() throws IOException {
            if (valuesDocID != docID()) {
                valuesDocID = docID();
                values = valueFetcher.apply(docID());
            }
            return values;
        }

        private float computeFreq() throws IOException {
            if (scansEveryDocument && walkablePhrase != null) {
                // One clause, so holding the phrase counts once however often the document holds it, and the walk
                // stops at the first one.
                return walkPhraseFreq(walkablePhrase, field, indexAnalyzer, values(), false) > 0 ? 1 : 0;
            }
            if (walkablePhrase != null) {
                return walkPhraseFreq(walkablePhrase, field, indexAnalyzer, values());
            }
            return getOrCreateMemoryIndex().search(query);
        }

        private Matches matches() throws IOException {
            IndexSearcher searcher = getOrCreateMemoryIndex().createSearcher();
            Weight w = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1);
            return w.matches(searcher.getLeafContexts().get(0), 0);
        }
    }

    private static class MemoryIndexEntry {
        private int docID = -1;
        private MemoryIndex memoryIndex;
    }
}
