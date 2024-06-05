/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.elasticsearch.test.ESTestCase;

public class CharMatcherTests extends ESTestCase {

    public void testLetter() {
        assertTrue(CharMatcher.Basic.LETTER.isTokenChar('a')); // category Ll
        assertTrue(CharMatcher.Basic.LETTER.isTokenChar('é')); // category Ll
        assertTrue(CharMatcher.Basic.LETTER.isTokenChar('A')); // category Lu
        assertTrue(CharMatcher.Basic.LETTER.isTokenChar('Å')); // category Lu
        assertTrue(CharMatcher.Basic.LETTER.isTokenChar('ʰ')); // category Lm
        assertTrue(CharMatcher.Basic.LETTER.isTokenChar('ª')); // category Lo
        assertTrue(CharMatcher.Basic.LETTER.isTokenChar('ǅ')); // category Lt
        assertFalse(CharMatcher.Basic.LETTER.isTokenChar(' '));
        assertFalse(CharMatcher.Basic.LETTER.isTokenChar('0'));
        assertFalse(CharMatcher.Basic.LETTER.isTokenChar('!'));
    }

    public void testSpace() {
        assertTrue(CharMatcher.Basic.WHITESPACE.isTokenChar(' '));
        assertTrue(CharMatcher.Basic.WHITESPACE.isTokenChar('\t'));
        assertFalse(CharMatcher.Basic.WHITESPACE.isTokenChar('\u00A0')); // nbsp
    }

    public void testNumber() {
        assertTrue(CharMatcher.Basic.DIGIT.isTokenChar('1'));
        assertTrue(CharMatcher.Basic.DIGIT.isTokenChar('١')); // ARABIC-INDIC DIGIT ONE
        assertFalse(CharMatcher.Basic.DIGIT.isTokenChar(','));
        assertFalse(CharMatcher.Basic.DIGIT.isTokenChar('a'));
    }

    public void testSymbol() {
        assertTrue(CharMatcher.Basic.SYMBOL.isTokenChar('$')); // category Sc
        assertTrue(CharMatcher.Basic.SYMBOL.isTokenChar('+')); // category Sm
        assertTrue(CharMatcher.Basic.SYMBOL.isTokenChar('`')); // category Sm
        assertTrue(CharMatcher.Basic.SYMBOL.isTokenChar('^')); // category Sk
        assertTrue(CharMatcher.Basic.SYMBOL.isTokenChar('¦')); // category Sc
        assertFalse(CharMatcher.Basic.SYMBOL.isTokenChar(' '));
    }

    public void testPunctuation() {
        assertTrue(CharMatcher.Basic.PUNCTUATION.isTokenChar('(')); // category Ps
        assertTrue(CharMatcher.Basic.PUNCTUATION.isTokenChar(')')); // category Pe
        assertTrue(CharMatcher.Basic.PUNCTUATION.isTokenChar('_')); // category Pc
        assertTrue(CharMatcher.Basic.PUNCTUATION.isTokenChar('!')); // category Po
        assertTrue(CharMatcher.Basic.PUNCTUATION.isTokenChar('-')); // category Pd
        assertTrue(CharMatcher.Basic.PUNCTUATION.isTokenChar('«')); // category Pi
        assertTrue(CharMatcher.Basic.PUNCTUATION.isTokenChar('»')); // category Pf
        assertFalse(CharMatcher.Basic.PUNCTUATION.isTokenChar(' '));
    }
}
