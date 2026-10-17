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

    /**
     * The text of one value a field holds, as the fetchers of those values spell it. A value read from a binary
     * column or from field data arrives as a string, and one read from a stored field as the bytes that were
     * stored, whose own {@code toString} spells them in hex rather than as text.
     */
    public static String textOf(Object value) {
        return value instanceof BytesRef bytes ? bytes.utf8ToString() : value.toString();
    }

    /** Decides whether one value's tokens answer a query. The stream is reset; implementations consume it. */
    public interface Matcher {
        boolean matches(TokenStream stream) throws IOException;

        /**
         * What a value must spell for this matcher to match it, which a caller reads a value's bytes by before
         * analyzing it. A matcher that leaves its terms unsaid answers {@link CandidateValues#ANY} and its values
         * are analyzed as they were.
         */
        default CandidateValues candidates() {
            return CandidateValues.ANY;
        }
    }

    /** Matches a value holding any of its terms, the semantics of a match query. */
    public record AnyTerm(Collection<BytesRef> queryTerms) implements Matcher {
        @Override
        public boolean matches(TokenStream stream) throws IOException {
            return containsAnyTerm(stream, queryTerms);
        }

        @Override
        public CandidateValues candidates() {
            return CandidateValues.anyOf(queryTerms);
        }
    }

    /** Matches a value holding its terms in order at consecutive positions, the semantics of a phrase. */
    public record Phrase(List<BytesRef> queryTerms) implements Matcher {
        @Override
        public boolean matches(TokenStream stream) throws IOException {
            final PhraseWalker walker = new PhraseWalker(queryTerms.toArray(BytesRef[]::new), false);
            walker.accept(stream);
            return walker.freq() > 0;
        }

        @Override
        public CandidateValues candidates() {
            // A phrase sits inside one value, so every term of it is spelled by the value holding the phrase.
            return CandidateValues.allOf(queryTerms);
        }
    }

    /**
     * Whether the terms {@code query} analyzes into sit one to a position, from the first, which is what
     * {@link #analyzeTerms} leaves behind when it discards their increments. A phrase of those is answered by
     * walking a value's tokens for them in order and adjacent, as {@link Phrase} does.
     *
     * <p>A dropped token leaves a gap between two of them, and two terms at one position leave a choice between
     * them, and a phrase holding either needs the positions themselves: it is answered by a query over an index of
     * the value instead. This is the test {@link ReanalyzingTextQuery#walkablePhrase} makes of the phrase a field
     * built, so a query is answered the same way whether its values come from a column or from a row.
     */
    public static boolean termsSitOneToAPosition(Analyzer analyzer, String field, String query) throws IOException {
        try (TokenStream stream = analyzer.tokenStream(field, query)) {
            final PositionIncrementAttribute increment = stream.addAttribute(PositionIncrementAttribute.class);
            stream.reset();
            while (stream.incrementToken()) {
                if (increment.getPositionIncrement() != 1) {
                    return false;
                }
            }
            stream.end();
        }
        return true;
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
     * Whether {@code stream} holds any of {@code terms}. The stream is consumed and left at its end; the caller
     * resets and closes it.
     */
    private static boolean containsAnyTerm(TokenStream stream, Collection<BytesRef> terms) throws IOException {
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
    /**
     * How many of a query's clauses a value's tokens answer, where every clause is answered by any one of the terms
     * it names. A clause answered counts once however often a value holds one of them, which is what a field
     * indexing no terms scores, so the walk stops as soon as every clause has been answered. Positions are not
     * read: no clause of this shape asks about them.
     */
    public static class ClauseWalker {

        /** The clauses a term answers, held by the query rather than built per document. */
        private final Map<BytesRef, int[]> clausesOfTerm;
        private final boolean[] answered;
        private int answeredCount;

        /**
         * @param clausesOfTerm which clauses each term answers, as {@link #clausesOfTerms} builds it
         * @param clauses how many clauses there are, which is what every one of them answered means
         */
        public ClauseWalker(Map<BytesRef, int[]> clausesOfTerm, int clauses) {
            this.clausesOfTerm = clausesOfTerm;
            this.answered = new boolean[clauses];
        }

        /** Which clauses each term answers, where {@code termsOfClause} holds the terms of every clause in order. */
        public static Map<BytesRef, int[]> clausesOfTerms(List<List<BytesRef>> termsOfClause) {
            final Map<BytesRef, List<Integer>> clausesByTerm = new HashMap<>();
            for (int clause = 0; clause < termsOfClause.size(); clause++) {
                for (BytesRef term : termsOfClause.get(clause)) {
                    final List<Integer> clauses = clausesByTerm.computeIfAbsent(term, t -> new ArrayList<>());
                    // A term naming the same clause twice answers it once, and the clause is already there.
                    if (clauses.isEmpty() || clauses.get(clauses.size() - 1) != clause) {
                        clauses.add(clause);
                    }
                }
            }
            final Map<BytesRef, int[]> clausesOfTerm = new HashMap<>(clausesByTerm.size());
            clausesByTerm.forEach((term, clauses) -> {
                final int[] of = new int[clauses.size()];
                for (int i = 0; i < of.length; i++) {
                    of[i] = clauses.get(i);
                }
                clausesOfTerm.put(term, of);
            });
            return clausesOfTerm;
        }

        /**
         * Feeds one value's tokens. The stream is consumed, to its end or to the token that answers the last clause
         * outstanding; the caller resets and closes it.
         */
        public void accept(TokenStream stream) throws IOException {
            final TermToBytesRefAttribute term = stream.addAttribute(TermToBytesRefAttribute.class);
            while (stream.incrementToken()) {
                final int[] clauses = clausesOfTerm.get(term.getBytesRef());
                if (clauses == null) {
                    continue;
                }
                for (int clause : clauses) {
                    if (answered[clause] == false) {
                        answered[clause] = true;
                        answeredCount++;
                    }
                }
                if (answeredCount == answered.length) {
                    return;
                }
            }
        }

        /** Whether the {@code clause}th clause was answered by any value fed so far. */
        public boolean answered(int clause) {
            return answered[clause];
        }

        /** Whether every clause has been answered, which is where a walk has nothing left to look for. */
        public boolean answeredEvery() {
            return answeredCount == answered.length;
        }
    }

    public static class PhraseWalker {

        private final BytesRef[] terms;
        /** Whether every phrase is counted, or the walk stops at the first one because only its presence is asked. */
        private final boolean countEvery;
        /** The position a run of the first n + 1 terms ended at, before the position in hand and at it. */
        private final int[] endedBefore;
        private final int[] endedHere;
        /**
         * How many phrases a run ending at that position stands for, which is how often the phrase's first term was
         * left at the position it started on.
         */
        private final int[] standsForBefore;
        private final int[] standsForHere;

        private int freq;
        private int position = -1;
        private int positionInHand = -1;
        /** The position the last phrase counted ended at, which no other token ending there counts again. */
        private int countedAt = Integer.MIN_VALUE;

        public PhraseWalker(BytesRef[] terms, boolean countEvery) {
            this.terms = terms;
            this.countEvery = countEvery;
            this.endedBefore = new int[terms.length];
            this.endedHere = new int[terms.length];
            this.standsForBefore = new int[terms.length];
            this.standsForHere = new int[terms.length];
            Arrays.fill(endedBefore, Integer.MIN_VALUE);
            Arrays.fill(endedHere, Integer.MIN_VALUE);
        }

        /** Advances past a gap between two values, which no phrase can cross unless the gap is one position. */
        public void skip(int gap) {
            position += gap;
        }

        /**
         * Feeds one value's tokens. The stream is consumed, to its end or to the phrase the walk stops at; the caller
         * resets and closes it.
         */
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
                            standsForBefore[length] = standsForHere[length];
                            endedHere[length] = Integer.MIN_VALUE;
                        }
                    }
                    positionInHand = position;
                }
                final BytesRef token = term.getBytesRef();
                if (terms[0].equals(token)) {
                    if (terms.length == 1) {
                        freq++;
                        if (countEvery == false) {
                            return;
                        }
                    } else if (endedHere[0] == position) {
                        // An analyzer can leave the same term at one position more than once, and an index counts a
                        // phrase starting on each of them.
                        standsForHere[0]++;
                    } else {
                        endedHere[0] = position;
                        standsForHere[0] = 1;
                    }
                }
                for (int length = 1; length < terms.length; length++) {
                    if (endedBefore[length - 1] == position - 1 && terms[length].equals(token)) {
                        if (length == terms.length - 1) {
                            // Only the first term repeating multiplies the phrase; a later one repeating at the
                            // position the phrase ends on is the same phrase ending there.
                            if (position != countedAt) {
                                countedAt = position;
                                freq += standsForBefore[length - 1];
                            }
                            if (countEvery == false) {
                                return;
                            }
                        } else if (endedHere[length] != position) {
                            endedHere[length] = position;
                            standsForHere[length] = standsForBefore[length - 1];
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
