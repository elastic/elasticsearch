/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.queries.intervals.IntervalIterator;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.graph.GraphTokenStreamFiniteStrings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Constructs an IntervalsSource based on analyzed text
 */
public class IntervalBuilder {

    private final String field;
    private final Analyzer analyzer;

    public IntervalBuilder(String field, Analyzer analyzer) {
        this.field = field;
        this.analyzer = analyzer;
    }

    public IntervalsSource analyzeText(String query, int maxGaps, boolean ordered) throws IOException {
        try (TokenStream ts = analyzer.tokenStream(field, query);
             CachingTokenFilter stream = new CachingTokenFilter(ts)) {
            return analyzeText(stream, maxGaps, ordered);
        }
    }

    protected IntervalsSource analyzeText(CachingTokenFilter stream, int maxGaps, boolean ordered) throws IOException {

        TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
        PositionIncrementAttribute posIncAtt = stream.addAttribute(PositionIncrementAttribute.class);
        PositionLengthAttribute posLenAtt = stream.addAttribute(PositionLengthAttribute.class);

        if (termAtt == null) {
            return NO_INTERVALS;
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
            return NO_INTERVALS;
        } else if (numTokens == 1) {
            // single term
            return analyzeTerm(stream);
        } else if (isGraph) {
            // graph
            return combineSources(analyzeGraph(stream), maxGaps, ordered);
        } else {
            // phrase
            if (hasSynonyms) {
                // phrase with single-term synonyms
                return analyzeSynonyms(stream, maxGaps, ordered);
            } else {
                // simple phrase
                return combineSources(analyzeTerms(stream), maxGaps, ordered);
            }
        }

    }

    protected IntervalsSource analyzeTerm(TokenStream ts) throws IOException {
        TermToBytesRefAttribute bytesAtt = ts.addAttribute(TermToBytesRefAttribute.class);
        ts.reset();
        ts.incrementToken();
        return Intervals.term(BytesRef.deepCopyOf(bytesAtt.getBytesRef()));
    }

    protected static IntervalsSource combineSources(List<IntervalsSource> sources, int maxGaps, boolean ordered) {
        if (sources.size() == 0) {
            return NO_INTERVALS;
        }
        if (sources.size() == 1) {
            return sources.get(0);
        }
        IntervalsSource[] sourcesArray = sources.toArray(new IntervalsSource[0]);
        if (maxGaps == 0 && ordered) {
            return Intervals.phrase(sourcesArray);
        }
        IntervalsSource inner = ordered ? Intervals.ordered(sourcesArray) : Intervals.unordered(sourcesArray);
        if (maxGaps == -1) {
            return inner;
        }
        return Intervals.maxgaps(maxGaps, inner);
    }

    protected List<IntervalsSource> analyzeTerms(TokenStream ts) throws IOException {
        List<IntervalsSource> terms = new ArrayList<>();
        TermToBytesRefAttribute bytesAtt = ts.addAttribute(TermToBytesRefAttribute.class);
        PositionIncrementAttribute posAtt = ts.addAttribute(PositionIncrementAttribute.class);
        ts.reset();
        while (ts.incrementToken()) {
            BytesRef term = bytesAtt.getBytesRef();
            int precedingSpaces = posAtt.getPositionIncrement() - 1;
            terms.add(extend(Intervals.term(BytesRef.deepCopyOf(term)), precedingSpaces));
        }
        ts.end();
        return terms;
    }

    public static IntervalsSource extend(IntervalsSource source, int precedingSpaces) {
        if (precedingSpaces == 0) {
            return source;
        }
        return Intervals.extend(source, precedingSpaces, 0);
    }

    protected IntervalsSource analyzeSynonyms(TokenStream ts, int maxGaps, boolean ordered) throws IOException {
        List<IntervalsSource> terms = new ArrayList<>();
        List<IntervalsSource> synonyms = new ArrayList<>();
        TermToBytesRefAttribute bytesAtt = ts.addAttribute(TermToBytesRefAttribute.class);
        PositionIncrementAttribute posAtt = ts.addAttribute(PositionIncrementAttribute.class);
        ts.reset();
        int spaces = 0;
        while (ts.incrementToken()) {
            int posInc = posAtt.getPositionIncrement();
            if (posInc > 0) {
                if (synonyms.size() == 1) {
                    terms.add(extend(synonyms.get(0), spaces));
                }
                else if (synonyms.size() > 1) {
                    terms.add(extend(Intervals.or(synonyms.toArray(new IntervalsSource[0])), spaces));
                }
                synonyms.clear();
                spaces = posInc - 1;
            }
            synonyms.add(Intervals.term(BytesRef.deepCopyOf(bytesAtt.getBytesRef())));
        }
        if (synonyms.size() == 1) {
            terms.add(extend(synonyms.get(0), spaces));
        }
        else {
            terms.add(extend(Intervals.or(synonyms.toArray(new IntervalsSource[0])), spaces));
        }
        return combineSources(terms, maxGaps, ordered);
    }

    protected List<IntervalsSource> analyzeGraph(TokenStream source) throws IOException {
        source.reset();
        GraphTokenStreamFiniteStrings graph = new GraphTokenStreamFiniteStrings(source);

        List<IntervalsSource> clauses = new ArrayList<>();
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
                List<IntervalsSource> paths = new ArrayList<>();
                Iterator<TokenStream> it = graph.getFiniteStrings(start, end);
                while (it.hasNext()) {
                    TokenStream ts = it.next();
                    IntervalsSource phrase = combineSources(analyzeTerms(ts), 0, true);
                    if (paths.size() >= maxClauseCount) {
                        throw new BooleanQuery.TooManyClauses();
                    }
                    paths.add(phrase);
                }
                if (paths.size() > 0) {
                    clauses.add(Intervals.or(paths.toArray(new IntervalsSource[0])));
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

    static final IntervalsSource NO_INTERVALS = new IntervalsSource() {

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
                public int gaps() {
                    throw new UnsupportedOperationException();
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
        public void visit(String field, QueryVisitor visitor) {}

        @Override
        public int minExtent() {
            return 0;
        }

        @Override
        public Collection<IntervalsSource> pullUpDisjunctions() {
            return Collections.emptyList();
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
