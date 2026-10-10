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
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.tests.analysis.CannedTokenStream;
import org.apache.lucene.tests.analysis.Token;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

/**
 * Whether a query's terms sit one to a position, which decides how a phrase over a field's values is answered: by
 * walking the values for terms in order and adjacent, or by a query over an index of them.
 */
public class TokenStreamMatchingPositionsTests extends ESTestCase {

    public void testTermsOneToAPosition() throws IOException {
        assertTrue(oneToAPosition(new StandardAnalyzer(), "brown fox"));
        assertTrue(oneToAPosition(new WhitespaceAnalyzer(), "brown fox"));
        assertTrue("one term is one position", oneToAPosition(new StandardAnalyzer(), "brown"));
        assertTrue("nothing to space out", oneToAPosition(new StandardAnalyzer(), ""));
    }

    /** A dropped token leaves the terms on either side of it two positions apart. */
    public void testADroppedTokenLeavesAGap() throws IOException {
        final Analyzer stop = new StopAnalyzer(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET);
        assertFalse(oneToAPosition(stop, "brown the fox"));
        assertFalse("a dropped first token spaces out the rest from it", oneToAPosition(stop, "the brown fox"));
        // The same analyzer answers for a query it drops nothing from.
        assertTrue(oneToAPosition(stop, "brown fox"));
    }

    /** A value read from a stored field arrives as bytes, whose own {@code toString} spells them in hex. */
    public void testTheTextOfAValue() {
        assertEquals("hello world", TokenStreamMatching.textOf("hello world"));
        assertEquals("hello world", TokenStreamMatching.textOf(new BytesRef("hello world")));
        assertEquals("caf\u00e9", TokenStreamMatching.textOf(new BytesRef("caf\u00e9")));
    }

    /**
     * Several tokens can end a phrase at one position, which a {@code keyword_repeat} filter and a stemmer leave
     * together. An index counts the phrase that ends there once, and so does the walk.
     */
    public void testAPhraseEndingTwiceAtOnePositionIsCountedOnce() throws IOException {
        final Token a = new Token("a", 0, 1);
        final Token first = new Token("b", 2, 3);
        final Token second = new Token("b", 2, 3);
        second.setPositionIncrement(0);
        assertEquals(1, phraseFreq(new String[] { "a", "b" }, a, first, second));
    }

    /** Two phrases ending at different positions are two, which is what a repeat of the phrase leaves. */
    public void testAPhraseEndingAtTwoPositionsIsCountedTwice() throws IOException {
        assertEquals(
            2,
            phraseFreq(new String[] { "a", "b" }, new Token("a", 0, 1), new Token("b", 2, 3), new Token("a", 4, 5), new Token("b", 6, 7))
        );
    }

    /** One term is counted by the token, as an index counts a term's frequency. */
    public void testOneTermIsCountedByTheToken() throws IOException {
        final Token first = new Token("b", 0, 1);
        final Token second = new Token("b", 0, 1);
        second.setPositionIncrement(0);
        assertEquals(2, phraseFreq(new String[] { "b" }, first, second));
    }

    private static int phraseFreq(String[] terms, Token... tokens) throws IOException {
        final BytesRef[] asBytes = new BytesRef[terms.length];
        for (int i = 0; i < terms.length; i++) {
            asBytes[i] = new BytesRef(terms[i]);
        }
        final TokenStreamMatching.PhraseWalker walker = new TokenStreamMatching.PhraseWalker(asBytes, true);
        try (CannedTokenStream stream = new CannedTokenStream(tokens)) {
            stream.reset();
            walker.accept(stream);
        }
        return walker.freq();
    }

    private static boolean oneToAPosition(Analyzer analyzer, String query) throws IOException {
        return TokenStreamMatching.termsSitOneToAPosition(analyzer, "field", query);
    }
}
