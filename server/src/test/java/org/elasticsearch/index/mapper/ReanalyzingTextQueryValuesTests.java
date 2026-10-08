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
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.miscellaneous.KeywordRepeatFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

/**
 * How a query over a field's values reads one of those values, and how often it counts a phrase it holds.
 */
public class ReanalyzingTextQueryValuesTests extends ESTestCase {

    /** A value read from a stored field arrives as bytes, whose own {@code toString} spells them in hex. */
    public void testTheTextOfAValue() {
        assertEquals("hello world", ReanalyzingTextQuery.textOf("hello world"));
        assertEquals("hello world", ReanalyzingTextQuery.textOf(new BytesRef("hello world")));
        assertEquals("café", ReanalyzingTextQuery.textOf(new BytesRef("café")));
    }

    /** The walk reads a value whichever way it arrives, so a phrase is found in bytes as it is in a string. */
    public void testAPhraseIsFoundInAValueHeldAsBytes() throws IOException {
        final Term[] phrase = { new Term("body", "quick"), new Term("body", "brown") };
        assertEquals(1, walk(phrase, List.of("the quick brown fox")));
        assertEquals("the same value, held as bytes", 1, walk(phrase, List.of(new BytesRef("the quick brown fox"))));
        assertEquals(0, walk(phrase, List.of(new BytesRef("brown quick"))));
    }

    /** A phrase the value holds twice is two, and one ending twice at a position is one, as an index counts them. */
    public void testHowOftenAPhraseIsCounted() throws IOException {
        final Term[] phrase = { new Term("body", "quick"), new Term("body", "brown") };
        assertEquals(2, walk(phrase, List.of("quick brown and quick brown")));
        assertEquals(1, walk(phrase, List.of("quick brown")));
    }

    /**
     * An analyzer that keeps a token and its stem leaves two of them at one position, and where the stem is the
     * token they are the same. A phrase ending on it ended once, which is what an index counts.
     */
    public void testAPhraseEndingTwiceAtOnePositionIsCountedOnce() throws IOException {
        final Analyzer keepsTheStemBesideTheToken = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                final Tokenizer source = new StandardTokenizer();
                return new TokenStreamComponents(source, new PorterStemFilter(new KeywordRepeatFilter(source)));
            }
        };
        // "cat" stems to itself, so the filter leaves cat@1 twice behind the x@0 before it.
        final Term[] phrase = { new Term("body", "x"), new Term("body", "cat") };
        assertEquals(1, ReanalyzingTextQuery.walkPhraseFreq(phrase, "body", keepsTheStemBesideTheToken, List.of("x cat")));
    }

    private static int walk(Term[] phrase, List<Object> values) throws IOException {
        return ReanalyzingTextQuery.walkPhraseFreq(phrase, "body", new StandardAnalyzer(), values);
    }
}
