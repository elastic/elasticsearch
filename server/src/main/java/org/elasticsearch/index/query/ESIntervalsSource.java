package org.elasticsearch.index.query;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.intervals.IntervalIterator;
import org.apache.lucene.search.intervals.Intervals;
import org.apache.lucene.search.intervals.IntervalsSource;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.graph.GraphTokenStreamFiniteStrings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public abstract class ESIntervalsSource extends IntervalsSource {

    public abstract int length();

    public abstract List<ESIntervalsSource> subSources();

    public static abstract class DelegatingIntervalsSource extends ESIntervalsSource {

        protected final IntervalsSource delegate;

        protected DelegatingIntervalsSource(IntervalsSource delegate) {
            this.delegate = delegate;
        }

        @Override
        public final IntervalIterator intervals(String field, LeafReaderContext ctx) throws IOException {
            return delegate.intervals(field, ctx);
        }

        @Override
        public final MatchesIterator matches(String field, LeafReaderContext ctx, int doc) throws IOException {
            return delegate.matches(field, ctx, doc);
        }

        @Override
        public final void extractTerms(String field, Set<Term> terms) {
            delegate.extractTerms(field, terms);
        }

        @Override
        public final String toString() {
            return delegate.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DisjunctionIntervalsSource that = (DisjunctionIntervalsSource) o;
            return Objects.equals(delegate, that.delegate);
        }

        @Override
        public int hashCode() {
            return Objects.hash(delegate);
        }
    }

    public static ESIntervalsSource disjunction(List<ESIntervalsSource> subSources) {
        if (subSources.size() == 0) {
            return NO_INTERVALS;
        }
        if (subSources.size() == 1) {
            return subSources.get(1);
        }
        return new DisjunctionIntervalsSource(subSources);
    }

    private static class DisjunctionIntervalsSource extends DelegatingIntervalsSource {

        private final List<ESIntervalsSource> subSources;

        public DisjunctionIntervalsSource(List<ESIntervalsSource> subSources) {
            super(Intervals.or(subSources.toArray(new IntervalsSource[0])));
            this.subSources = new ArrayList<>(subSources);
        }

        @Override
        public int length() {
            int length = subSources.get(0).length();
            for (int i = 1; i < subSources.size(); i++) {
                if (subSources.get(i).length() != length) {
                    return -1;
                }
            }
            return length;
        }

        @Override
        public List<ESIntervalsSource> subSources() {
            return subSources;
        }
    }

    public static class TermIntervalsSource extends DelegatingIntervalsSource {

        public TermIntervalsSource(BytesRef term) {
            super(Intervals.term(term));
        }

        @Override
        public int length() {
            return 1;
        }

        @Override
        public List<ESIntervalsSource> subSources() {
            return Collections.singletonList(this);
        }
    }

    public static class PhraseIntervalsSource extends DelegatingIntervalsSource {

        final int length;
        final List<ESIntervalsSource> subSources = new ArrayList<>();

        public PhraseIntervalsSource(List<ESIntervalsSource> terms) {
            this(terms.toArray(new ESIntervalsSource[0]));
        }

        public PhraseIntervalsSource(ESIntervalsSource... terms) {
            super(Intervals.phrase(terms));
            subSources.addAll(Arrays.asList(terms));
            int length = 0;
            for (ESIntervalsSource term : terms) {
                if (term.length() > 0) {
                    length += term.length();
                }
                else {
                    this.length = -1;
                    return;
                }
            }
            this.length = length;
        }

        @Override
        public int length() {
            return length;
        }

        @Override
        public List<ESIntervalsSource> subSources() {
            return subSources;
        }
    }

    public static ESIntervalsSource maxwidth(int maxWidth, ESIntervalsSource subSource) {
        if (subSource.length() < 0) {
            return new MaxWidthIntervalsSource(maxWidth, subSource);
        }
        if (subSource.length() > maxWidth) {
            return NO_INTERVALS;
        }
        return subSource;
    }

    public static class MaxWidthIntervalsSource extends DelegatingIntervalsSource {

        final ESIntervalsSource delegate;

        public MaxWidthIntervalsSource(int maxWidth, ESIntervalsSource delegate) {
            super(Intervals.maxwidth(maxWidth, delegate));
            this.delegate = delegate;
        }

        @Override
        public int length() {
            return delegate.length();
        }

        @Override
        public List<ESIntervalsSource> subSources() {
            return delegate.subSources();
        }
    }

    public static List<ESIntervalsSource> analyzeQuery(String field, String query, Analyzer analyzer) throws IOException {
        try (TokenStream ts = analyzer.tokenStream(field, query);
             CachingTokenFilter stream = new CachingTokenFilter(ts)) {

            TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
            PositionIncrementAttribute posIncAtt = stream.addAttribute(PositionIncrementAttribute.class);
            PositionLengthAttribute posLenAtt = stream.addAttribute(PositionLengthAttribute.class);

            if (termAtt == null) {
                return Collections.singletonList(NO_INTERVALS);
            }

            // phase 1: read through the stream and assess the situation:
            // counting the number of tokens/positions and marking if we have any synonyms.

            int numTokens = 0;
            boolean hasSynonyms = false;
            boolean isGraph = false;

            stream.reset();
            while (stream.incrementToken()) {
                numTokens++;
                int positionIncrement = posIncAtt.getPositionIncrement();
                if (positionIncrement == 0) {
                    hasSynonyms = true;
                }
                int positionLength = posLenAtt.getPositionLength();
                if (positionLength > 1) {
                    isGraph = true;
                }
            }

            // phase 2: based on token count, presence of synonyms, and options
            // formulate a single term, boolean, or phrase.

            if (numTokens == 0) {
                return null;
            } else if (numTokens == 1) {
                // single term
                return Collections.singletonList(analyzeTerm(stream));
            } else if (isGraph) {
                // graph
                return analyzeGraph(stream);
            } else {
                // phrase
                if (hasSynonyms) {
                    // phrase with single-term synonyms
                    return analyzeSynonyms(stream);
                } else {
                    // simple phrase
                    return analyzeTerms(stream);
                }
            }
        }
    }

    public static ESIntervalsSource analyzeTerm(TokenStream ts) throws IOException {
        TermToBytesRefAttribute bytesAtt = ts.addAttribute(TermToBytesRefAttribute.class);
        ts.reset();
        ts.incrementToken();
        return new TermIntervalsSource(bytesAtt.getBytesRef());
    }

    public static List<ESIntervalsSource> analyzeTerms(TokenStream ts) throws IOException {
        List<ESIntervalsSource> terms = new ArrayList<>();
        TermToBytesRefAttribute bytesAtt = ts.addAttribute(TermToBytesRefAttribute.class);
        ts.reset();
        while (ts.incrementToken()) {
            BytesRef term = bytesAtt.getBytesRef();
            terms.add(new TermIntervalsSource(BytesRef.deepCopyOf(term)));
        }
        ts.end();
        return terms;
    }

    public static List<ESIntervalsSource> analyzeSynonyms(TokenStream ts) throws IOException {
        List<ESIntervalsSource> terms = new ArrayList<>();
        List<ESIntervalsSource> synonyms = new ArrayList<>();
        TermToBytesRefAttribute bytesAtt = ts.addAttribute(TermToBytesRefAttribute.class);
        PositionIncrementAttribute posAtt = ts.addAttribute(PositionIncrementAttribute.class);
        ts.reset();
        while (ts.incrementToken()) {
            if (posAtt.getPositionIncrement() == 1) {
                if (synonyms.size() == 1) {
                    terms.add(synonyms.get(0));
                }
                if (synonyms.size() > 1) {
                    terms.add(new DisjunctionIntervalsSource(synonyms));
                }
                synonyms.clear();
            }
            synonyms.add(new TermIntervalsSource(bytesAtt.getBytesRef()));
        }
        if (synonyms.size() == 1) {
            terms.add(synonyms.get(0));
        }
        else {
            terms.add(new DisjunctionIntervalsSource(synonyms));
        }
        return terms;
    }

    public static List<ESIntervalsSource> analyzeGraph(TokenStream source) throws IOException {
        source.reset();
        GraphTokenStreamFiniteStrings graph = new GraphTokenStreamFiniteStrings(source);

        List<ESIntervalsSource> clauses = new ArrayList<>();
        int[] articulationPoints = graph.articulationPoints();
        int lastState = 0;
        int maxClauseCount = BooleanQuery.getMaxClauseCount();
        for (int i = 0; i <= articulationPoints.length; i++) {
            int start = lastState;
            int end = -1;
            if (i < articulationPoints.length) {
                end = articulationPoints[i];
            }
            lastState = end;
            if (graph.hasSidePath(start)) {
                List<ESIntervalsSource> paths = new ArrayList<>();
                Iterator<TokenStream> it = graph.getFiniteStrings(start, end);
                while (it.hasNext()) {
                    TokenStream ts = it.next();
                    ESIntervalsSource phrase = new PhraseIntervalsSource(analyzeTerms(ts));
                    if (paths.size() >= maxClauseCount) {
                        throw new BooleanQuery.TooManyClauses();
                    }
                    paths.add(phrase);
                }
                if (paths.size() > 0) {
                    clauses.add(new DisjunctionIntervalsSource(paths));
                }
            } else {
                Iterator<TokenStream> it = graph.getFiniteStrings(start, end);
                TokenStream ts = it.next();
                clauses.addAll(analyzeTerms(ts));
                assert it.hasNext() == false;
            }
        }
        return clauses;
    }

    public static final ESIntervalsSource NO_INTERVALS = new ESIntervalsSource() {

        @Override
        public int length() {
            return 0;
        }

        @Override
        public List<ESIntervalsSource> subSources() {
            return Collections.emptyList();
        }

        @Override
        public IntervalIterator intervals(String field, LeafReaderContext ctx) {
            return new IntervalIterator() {
                @Override
                public int start() {
                    return NO_MORE_INTERVALS;
                }

                @Override
                public int end() {
                    return NO_MORE_INTERVALS;
                }

                @Override
                public int nextInterval() {
                    return NO_MORE_INTERVALS;
                }

                @Override
                public float matchCost() {
                    return 0;
                }

                @Override
                public int docID() {
                    return NO_MORE_DOCS;
                }

                @Override
                public int nextDoc() {
                    return NO_MORE_DOCS;
                }

                @Override
                public int advance(int target) {
                    return NO_MORE_DOCS;
                }

                @Override
                public long cost() {
                    return 0;
                }
            };
        }

        @Override
        public MatchesIterator matches(String field, LeafReaderContext ctx, int doc) {
            return null;
        }

        @Override
        public void extractTerms(String field, Set<Term> terms) {

        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public boolean equals(Object other) {
            return other == this;
        }

        @Override
        public String toString() {
            return "no_match";
        }
    };

}
