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
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Matching the tokens of a value against the terms a query asks about, for the searches that read a document's values
 * rather than an index: a {@code text} field without positions or without terms at all, and the row-by-row searches of
 * ES|QL. One rule each, so the sources cannot answer the same question differently.
 */
public final class TokenStreamMatching {

    private TokenStreamMatching() {}

    /** Decides whether one value's tokens answer a query. The stream is reset; implementations consume it. */
    public interface Matcher {
        boolean matches(TokenStream stream) throws IOException;
    }

    /** Matches a value holding any of its terms, the semantics of a match query. */
    public record AnyTerm(Collection<BytesRef> queryTerms) implements Matcher {
        @Override
        public boolean matches(TokenStream stream) throws IOException {
            return containsAnyTerm(stream, queryTerms);
        }
    }

    /** Matches a value holding its terms in order at consecutive positions, the semantics of a phrase. */
    public record Phrase(List<BytesRef> queryTerms) implements Matcher {
        @Override
        public boolean matches(TokenStream stream) throws IOException {
            final PhraseWalker walker = new PhraseWalker(queryTerms.toArray(BytesRef[]::new));
            walker.accept(stream);
            return walker.freq() > 0;
        }
    }

    /**
     * The terms a query string analyzes into, in the order they appear, with their position increments discarded.
     */
    public static List<BytesRef> analyzeTerms(Analyzer analyzer, String field, String query) throws IOException {
        final List<BytesRef> terms = new ArrayList<>();
        try (TokenStream stream = analyzer.tokenStream(field, query)) {
            stream.reset();
            final TermToBytesRefAttribute term = stream.addAttribute(TermToBytesRefAttribute.class);
            while (stream.incrementToken()) {
                terms.add(BytesRef.deepCopyOf(term.getBytesRef()));
            }
            stream.end();
        }
        return terms;
    }

    /** The same, with how often a query repeats each term, which is what weighs it where no index does. */
    public static Map<BytesRef, Integer> analyzeTermsWithCounts(Analyzer analyzer, String field, String query) throws IOException {
        final Map<BytesRef, Integer> terms = new HashMap<>();
        try (TokenStream stream = analyzer.tokenStream(field, query)) {
            stream.reset();
            final TermToBytesRefAttribute term = stream.addAttribute(TermToBytesRefAttribute.class);
            while (stream.incrementToken()) {
                terms.merge(BytesRef.deepCopyOf(term.getBytesRef()), 1, Integer::sum);
            }
            stream.end();
        }
        return terms;
    }

    /**
     * Whether {@code stream} holds any of {@code terms}, the semantics of a match query. The stream is consumed and
     * left at its end; the caller resets and closes it.
     */
    public static boolean containsAnyTerm(TokenStream stream, Collection<BytesRef> terms) throws IOException {
        final TermToBytesRefAttribute term = stream.addAttribute(TermToBytesRefAttribute.class);
        while (stream.incrementToken()) {
            if (terms.contains(term.getBytesRef())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Counts the phrases a document holds as its values are fed in, one stream at a time. Where a document holds
     * several values, the gap its analyzer leaves between them is fed too, so a phrase spans them exactly as far as
     * an index of the same values would let it.
     */
    public static class PhraseWalker {

        private final BytesRef[] terms;
        /** The position a run of the first n + 1 terms ended at, before the position in hand and at it. */
        private final int[] endedBefore;
        private final int[] endedHere;

        private int freq;
        private int position = -1;
        private int positionInHand = -1;

        public PhraseWalker(BytesRef[] terms) {
            this.terms = terms;
            this.endedBefore = new int[terms.length];
            this.endedHere = new int[terms.length];
            Arrays.fill(endedBefore, Integer.MIN_VALUE);
            Arrays.fill(endedHere, Integer.MIN_VALUE);
        }

        /** Advances past a gap between two values, which no phrase can cross unless the gap is one position. */
        public void skip(int gap) {
            position += gap;
        }

        /** Feeds one value's tokens. The stream is consumed and left at its end; the caller resets and closes it. */
        public void accept(TokenStream stream) throws IOException {
            final TermToBytesRefAttribute term = stream.addAttribute(TermToBytesRefAttribute.class);
            final PositionIncrementAttribute increment = stream.addAttribute(PositionIncrementAttribute.class);
            while (stream.incrementToken()) {
                position += increment.getPositionIncrement();
                if (position != positionInHand) {
                    // Tokens sharing a position each extend what ended before it, so what ends here is held back
                    // until the position does.
                    for (int length = 0; length < terms.length; length++) {
                        if (endedHere[length] != Integer.MIN_VALUE) {
                            endedBefore[length] = endedHere[length];
                            endedHere[length] = Integer.MIN_VALUE;
                        }
                    }
                    positionInHand = position;
                }
                final BytesRef token = term.getBytesRef();
                if (terms[0].equals(token)) {
                    if (terms.length == 1) {
                        freq++;
                    } else {
                        endedHere[0] = position;
                    }
                }
                for (int length = 1; length < terms.length; length++) {
                    if (endedBefore[length - 1] == position - 1 && terms[length].equals(token)) {
                        if (length == terms.length - 1) {
                            freq++;
                        } else {
                            endedHere[length] = position;
                        }
                    }
                }
            }
        }

        /** How many phrases the values fed so far hold. */
        public int freq() {
            return freq;
        }
    }
}
