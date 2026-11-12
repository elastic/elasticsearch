/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.elasticsearch.test.ESTokenStreamTestCase;

/**
 * {@link ThaiLucene105Tokenizer} must cut the segmenting buffer only where Lucene 10.5's {@code SegmentingTokenizerBase} did (line
 * terminators). {@link ThaiAnalyzerBwcTests} covers the resulting tokens.
 */
public class ThaiLucene105TokenizerTests extends ESTokenStreamTestCase {

    public void testSafeEndIsLineTerminatorsOnly() {
        ThaiLucene105Tokenizer tokenizer = new ThaiLucene105Tokenizer();
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

}
