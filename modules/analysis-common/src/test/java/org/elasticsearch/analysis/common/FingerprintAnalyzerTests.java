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
import org.apache.lucene.analysis.CharArraySet;
import org.elasticsearch.test.ESTokenStreamTestCase;

import static org.apache.lucene.tests.analysis.BaseTokenStreamTestCase.assertAnalyzesTo;

public class FingerprintAnalyzerTests extends ESTokenStreamTestCase {

    public void testFingerprint() throws Exception {
        Analyzer a = new FingerprintAnalyzer(CharArraySet.EMPTY_SET, ' ', 255);
        assertAnalyzesTo(a, "foo bar@baz Baz $ foo foo FOO. FoO", new String[] { "bar baz foo" });
    }

    public void testReusableTokenStream() throws Exception {
        Analyzer a = new FingerprintAnalyzer(CharArraySet.EMPTY_SET, ' ', 255);
        assertAnalyzesTo(a, "foo bar baz Baz foo foo FOO. FoO", new String[] { "bar baz foo" });
        assertAnalyzesTo(a, "xyz XYZ abc 123.2 abc", new String[] { "123.2 abc xyz" });
    }

    public void testAsciifolding() throws Exception {
        Analyzer a = new FingerprintAnalyzer(CharArraySet.EMPTY_SET, ' ', 255);
        assertAnalyzesTo(a, "gödel escher bach", new String[] { "bach escher godel" });

        assertAnalyzesTo(a, "gödel godel escher bach", new String[] { "bach escher godel" });
    }

    public void testLimit() throws Exception {
        Analyzer a = new FingerprintAnalyzer(CharArraySet.EMPTY_SET, ' ', 3);
        assertAnalyzesTo(a, "e d c b a", new String[] {});

        assertAnalyzesTo(a, "b a", new String[] { "a b" });
    }

}
