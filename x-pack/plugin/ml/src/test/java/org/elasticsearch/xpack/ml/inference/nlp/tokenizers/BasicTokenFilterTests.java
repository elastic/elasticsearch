/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;

import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Some test cases taken from
 * https://github.com/huggingface/transformers/blob/ba8c4d0ac04acfcdbdeaed954f698d6d5ec3e532/tests/test_tokenization_bert.py
 */
public class BasicTokenFilterTests extends BaseTokenStreamTestCase {

    public void testNeverSplit_GivenNoLowerCase() throws IOException {
        Analyzer analyzer = basicAnalyzerFromSettings(false, false, List.of("[UNK]"));
        assertAnalyzesToNoCharFilter(analyzer, "1 (return) [ Patois ", new String[] { "1", "(", "return", ")", "[", "Patois" });
        assertAnalyzesToNoCharFilter(analyzer, "Hello [UNK].", new String[] { "Hello", "[UNK]", "." });
        assertAnalyzesToNoCharFilter(analyzer, "Hello-[UNK]", new String[] { "Hello", "-", "[UNK]" });
        assertAnalyzesToNoCharFilter(
            analyzer,
            " \tHeLLo!how  \n Are yoU? [UNK]",
            new String[] { "HeLLo", "!", "how", "Are", "yoU", "?", "[UNK]" }
        );
        assertAnalyzesToNoCharFilter(analyzer, "Hello [UNK]?", new String[] { "Hello", "[UNK]", "?" });
        assertAnalyzesToNoCharFilter(analyzer, "Hello [UNK]!!", new String[] { "Hello", "[UNK]", "!", "!" });
        assertAnalyzesToNoCharFilter(analyzer, "Hello~[UNK][UNK]", new String[] { "Hello", "~", "[UNK]", "[UNK]" });
        assertAnalyzesToNoCharFilter(analyzer, "Hello-[unk]", new String[] { "Hello", "-", "[", "unk", "]" });
    }

    public void testNeverSplit_GivenLowerCase() throws IOException {
        Analyzer analyzer = basicAnalyzerFromSettings(false, false, List.of("[UNK]"));
        assertAnalyzesToNoCharFilter(
            analyzer,
            " \tHeLLo!how  \n Are yoU? [UNK]",
            new String[] { "HeLLo", "!", "how", "Are", "yoU", "?", "[UNK]" }
        );
        assertAnalyzesToNoCharFilter(analyzer, "Hello [UNK].", new String[] { "Hello", "[UNK]", "." });
        assertAnalyzesToNoCharFilter(analyzer, "Hello [UNK]?", new String[] { "Hello", "[UNK]", "?" });
        assertAnalyzesToNoCharFilter(analyzer, "Hello [UNK]!!", new String[] { "Hello", "[UNK]", "!", "!" });
        assertAnalyzesToNoCharFilter(analyzer, "Hello-[UNK]", new String[] { "Hello", "-", "[UNK]" });
        assertAnalyzesToNoCharFilter(analyzer, "Hello~[UNK][UNK]", new String[] { "Hello", "~", "[UNK]", "[UNK]" });
        assertAnalyzesToNoCharFilter(analyzer, "Hello-[unk]", new String[] { "Hello", "-", "[", "unk", "]" });
    }

    public void testSplitCJK() throws Exception {
        Analyzer analyzer = basicAnalyzerFromSettings(true, false, List.of("[UNK]"));
        assertAnalyzesToNoCharFilter(analyzer, "hello ah\u535A\u63A8zz", new String[] { "hello", "ah", "\u535A", "\u63A8", "zz" });
        assertAnalyzesToNoCharFilter(analyzer, "hello world", new String[] { "hello", "world" });
    }

    public void testStripAccents() throws Exception {
        Analyzer analyzer = basicAnalyzerFromSettings(true, true, List.of("[UNK]"));
        assertAnalyzesToNoCharFilter(analyzer, "HäLLo how are you", new String[] { "HaLLo", "how", "are", "you" });
        assertAnalyzesToNoCharFilter(analyzer, "ÎÎÎÏÎ½ÎÎÎÎ±Î¿Ï", new String[] { "IIIII½IIII±I", "¿", "I" });
    }

    private static void assertAnalyzesToNoCharFilter(Analyzer a, String input, String[] output) throws IOException {
        assertTokenStreamContents(a.tokenStream("dummy", input), output, null, null, null, null, null, input.length());
        checkResetException(a, input);
        // We don't allow the random char filter because our offsets aren't corrected appropriately due to "never_split"
        // If we could figure out a way to pass "never_split" through whichever passed char_filter there was, then it would work
        checkAnalysisConsistency(random(), a, false, input);
    }

    public void testIsPunctuation() {
        assertTrue(BasicTokenFilter.isPunctuationMark('-'));
        assertTrue(BasicTokenFilter.isPunctuationMark('$'));
        assertTrue(BasicTokenFilter.isPunctuationMark('`'));
        assertTrue(BasicTokenFilter.isPunctuationMark('.'));
        assertFalse(BasicTokenFilter.isPunctuationMark(' '));
        assertFalse(BasicTokenFilter.isPunctuationMark('A'));
        assertTrue(BasicTokenFilter.isPunctuationMark('['));
    }

    public static Analyzer basicAnalyzerFromSettings(boolean isTokenizeCjkChars, boolean isStripAccents, List<String> neverSplit) {
        return new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer t = new WhitespaceTokenizer();
                try {
                    return new TokenStreamComponents(t, BasicTokenFilter.build(isTokenizeCjkChars, isStripAccents, neverSplit, t));
                } catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                }
            }

            @Override
            protected Reader initReader(String fieldName, Reader reader) {
                return new ControlCharFilter(reader);
            }
        };
    }

    public static List<DelimitedToken> basicTokenize(Analyzer analyzer, String input) throws IOException {
        try (TokenStream test = analyzer.tokenStream("test", input)) {
            test.reset();
            CharTermAttribute term = test.addAttribute(CharTermAttribute.class);
            OffsetAttribute offsetAttribute = test.addAttribute(OffsetAttribute.class);
            List<DelimitedToken> tokens = new ArrayList<>();
            while (test.incrementToken()) {
                tokens.add(new DelimitedToken(term.toString(), offsetAttribute.startOffset(), offsetAttribute.endOffset()));
            }
            test.end();
            return tokens;
        }
    }

    public static List<DelimitedToken> basicTokenize(
        boolean isTokenizeCjkChars,
        boolean isStripAccents,
        List<String> neverSplit,
        String input
    ) throws IOException {
        try (Analyzer analyzer = basicAnalyzerFromSettings(isTokenizeCjkChars, isStripAccents, neverSplit)) {
            return basicTokenize(analyzer, input);
        }
    }

}
