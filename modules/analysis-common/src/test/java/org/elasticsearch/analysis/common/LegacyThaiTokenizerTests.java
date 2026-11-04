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
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.th.ThaiTokenizer;
import org.elasticsearch.test.ESTokenStreamTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Supplier;

import static org.apache.lucene.tests.analysis.BaseTokenStreamTestCase.assertAnalyzesTo;
import static org.apache.lucene.tests.analysis.BaseTokenStreamTestCase.checkRandomData;

/**
 * {@link LegacyThaiTokenizer} must cut the segmenting buffer only where Lucene 10.5's {@code SegmentingTokenizerBase} did (line
 * terminators), and otherwise tokenize exactly like {@link ThaiTokenizer}.
 */
public class LegacyThaiTokenizerTests extends ESTokenStreamTestCase {

    public void testSafeEndIsLineTerminatorsOnly() {
        LegacyThaiTokenizer tokenizer = new LegacyThaiTokenizer();
        for (char ch : new char[] { '\r', '\n', '\u0085', '\u2028', '\u2029' }) {
            assertTrue(Integer.toHexString(ch), tokenizer.isSafeEnd(ch));
        }
        // Whitespace that Lucene 10.6 also treats as a safe end.
        for (char ch : new char[] { ' ', '\t', '\u000B', '\u000C', '\u001C', '\u2000', '\u3000' }) {
            assertTrue(Integer.toHexString(ch), Character.isWhitespace(ch));
            assertFalse(Integer.toHexString(ch), tokenizer.isSafeEnd(ch));
        }
        assertFalse(tokenizer.isSafeEnd('ก'));
    }

    /**
     * A Thai word straddling the 1024-char buffer after a space but with no newline: the legacy tokenizer slices it the way
     * Lucene 10.5 did, Lucene's current tokenizer does not.
     */
    public void testLongBufferWithoutNewlines() throws IOException {
        ThaiAnalyzerBwcTests.assertPreLucene106ThaiTokenizer(analyzer(LegacyThaiTokenizer::new));
        ThaiAnalyzerBwcTests.assertLucene106ThaiTokenizer(analyzer(ThaiTokenizer::new));
    }

    /** With a newline before the straddling word both rules cut at the same place, so the two tokenizers agree. */
    public void testLongBufferWithNewlineMatchesCurrent() throws IOException {
        String text = "ก".repeat(1021) + "\nแมว";
        String[] expected = new String[512];
        Arrays.fill(expected, 0, 510, "กก");
        expected[510] = "ก";
        expected[511] = "แมว";
        assertAnalyzesTo(analyzer(LegacyThaiTokenizer::new), text, expected);
        assertAnalyzesTo(analyzer(ThaiTokenizer::new), text, expected);
    }

    /** Short text never reaches a buffer cut, so the two tokenizers agree. */
    public void testShortTextMatchesCurrent() throws IOException {
        String text = "การที่ได้ต้องแสดงว่างานดี ภาษาไทย 1234";
        String[] expected = new String[] { "การ", "ที่", "ได้", "ต้อง", "แสดง", "ว่า", "งาน", "ดี", "ภาษา", "ไทย", "1234" };
        assertAnalyzesTo(analyzer(LegacyThaiTokenizer::new), text, expected);
        assertAnalyzesTo(analyzer(ThaiTokenizer::new), text, expected);
    }

    public void testRandomStrings() throws IOException {
        try (Analyzer analyzer = analyzer(LegacyThaiTokenizer::new)) {
            checkRandomData(random(), analyzer, 200);
        }
    }

    private static Analyzer analyzer(Supplier<Tokenizer> tokenizer) {
        return new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                return new TokenStreamComponents(tokenizer.get());
            }
        };
    }
}
