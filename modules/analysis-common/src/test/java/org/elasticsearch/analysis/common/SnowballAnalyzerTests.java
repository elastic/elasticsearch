/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.elasticsearch.test.ESTokenStreamTestCase;

import static org.apache.lucene.tests.analysis.BaseTokenStreamTestCase.assertAnalyzesTo;

public class SnowballAnalyzerTests extends ESTokenStreamTestCase {

    public void testEnglish() throws Exception {
        Analyzer a = new SnowballAnalyzer("English");
        assertAnalyzesTo(a, "he abhorred accents", new String[] { "he", "abhor", "accent" });
    }

    public void testStopwords() throws Exception {
        Analyzer a = new SnowballAnalyzer("English", EnglishAnalyzer.ENGLISH_STOP_WORDS_SET);
        assertAnalyzesTo(a, "the quick brown fox jumped", new String[] { "quick", "brown", "fox", "jump" });
    }

    /**
     * Test turkish lowercasing
     */
    public void testTurkish() throws Exception {
        Analyzer a = new SnowballAnalyzer("Turkish");

        assertAnalyzesTo(a, "ağacı", new String[] { "ağaç" });
        assertAnalyzesTo(a, "AĞACI", new String[] { "ağaç" });
    }

    public void testReusableTokenStream() throws Exception {
        Analyzer a = new SnowballAnalyzer("English");
        assertAnalyzesTo(a, "he abhorred accents", new String[] { "he", "abhor", "accent" });
        assertAnalyzesTo(a, "she abhorred him", new String[] { "she", "abhor", "him" });
    }
}
