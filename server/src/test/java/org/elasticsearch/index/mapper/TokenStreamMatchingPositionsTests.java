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

    private static boolean oneToAPosition(Analyzer analyzer, String query) throws IOException {
        return TokenStreamMatching.termsSitOneToAPosition(analyzer, "field", query);
    }
}
