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
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Reading a value for a term compares the ASCII letters without their case, which is sound because lowering a
 * character outside ASCII leaves it outside ASCII — save for two of them, whose lower case is an ASCII letter.
 *
 * <p>That is an assumption about Unicode, so it is checked here rather than believed: every character whose case
 * changes at all is walked, under every analyzer a value is read for, and the read is held to the one thing it
 * promises — a value it rules out holds none of the tokens it was read for.
 */
public class CandidateValuesUnicodeTests extends ESTestCase {

    private static final String FIELD = "body";

    private static List<Analyzer> readableAnalyzers() {
        return List.of(
            new StandardAnalyzer(),
            new WhitespaceAnalyzer(),
            new SimpleAnalyzer(),
            new StopAnalyzer(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET),
            new KeywordAnalyzer()
        );
    }

    /** The two characters the rule makes an exception of, and the whole of Unicode holds no others. */
    public void testOnlyTwoCharactersLowerCaseIntoAscii() {
        final List<String> found = new ArrayList<>();
        for (int codePoint = 128; codePoint <= Character.MAX_CODE_POINT; codePoint++) {
            if (Character.isDefined(codePoint) && Character.toLowerCase(codePoint) < 128) {
                found.add(String.format(Locale.ROOT, "U+%04X", codePoint));
            }
        }
        assertEquals("the read makes an exception of these and no others", List.of("U+0130", "U+212A"), found);
    }

    /**
     * For every character whose case changes, a value holding it is read for each token that value actually
     * leaves. A read that ruled such a value out would lose a document the analysis answers.
     */
    public void testAValueIsNeverRuledOutForATokenItLeaves() throws IOException {
        int read = 0;
        for (int codePoint = 128; codePoint <= Character.MAX_CODE_POINT; codePoint++) {
            if (Character.isDefined(codePoint) == false || Character.toLowerCase(codePoint) == codePoint) {
                continue;
            }
            final String character = new String(Character.toChars(codePoint));
            for (String text : List.of(
                character,
                "a" + character + "b",
                // Where the character carries the letter of a word, which is what the exceptions do.
                "zeppel" + character + "n",
                "quick " + character + " brown"
            )) {
                for (Analyzer analyzer : readableAnalyzers()) {
                    for (BytesRef token : TokenStreamMatching.analyzeTerms(analyzer, FIELD, text)) {
                        final CandidateValues candidates = CandidateValues.anyOf(List.of(token));
                        if (candidates.reads() == false) {
                            continue;
                        }
                        read++;
                        assertTrue(
                            "U+"
                                + Integer.toHexString(codePoint).toUpperCase(Locale.ROOT)
                                + " in ["
                                + text
                                + "] leaves token ["
                                + token.utf8ToString()
                                + "] under "
                                + analyzer.getClass().getSimpleName()
                                + ", and the read ruled the value out",
                            candidates.mayHold(new BytesRef(text))
                        );
                    }
                }
            }
        }
        assertThat("nothing was read for, so nothing was proved", read, org.hamcrest.Matchers.greaterThan(10000));
    }

    /** The exceptions themselves, spelled out: each carries an ASCII letter into a token. */
    public void testTheTwoExceptionsCarryALetterIntoAToken() throws IOException {
        final Analyzer standard = new StandardAnalyzer();
        for (String[] each : new String[][] { { "zeppelİn", "zeppelin" }, { "Kelvin", "kelvin" } }) {
            final String text = each[0];
            final String expected = each[1];
            assertEquals(
                "the analyzer carries the character into an ASCII token",
                List.of(new BytesRef(expected)),
                TokenStreamMatching.analyzeTerms(standard, FIELD, text)
            );
            final CandidateValues candidates = CandidateValues.anyOf(List.of(new BytesRef(expected)));
            assertTrue("and the value spells none of it, so the read must keep it", candidates.mayHold(new BytesRef(text)));
        }
    }

    /** A value outside ASCII is read like any other, which is the point of the rule. */
    public void testAValueOutsideAsciiIsStillRuledOut() {
        final CandidateValues candidates = CandidateValues.anyOf(List.of(new BytesRef("zeppelin")));
        assertFalse(
            "a curly quote says nothing about the tokens a value leaves",
            candidates.mayHold(new BytesRef("a value with ‘curly quotes’ and an en–dash"))
        );
        assertFalse(candidates.mayHold(new BytesRef("café naïve Über")));
        assertTrue(candidates.mayHold(new BytesRef("a café named zeppelin")));
    }
}
