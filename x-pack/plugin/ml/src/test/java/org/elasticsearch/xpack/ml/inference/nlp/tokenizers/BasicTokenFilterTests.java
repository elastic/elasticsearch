/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;

/**
 * Some test cases taken from
 * https://github.com/huggingface/transformers/blob/ba8c4d0ac04acfcdbdeaed954f698d6d5ec3e532/tests/test_tokenization_bert.py
 */
public class BasicTokenFilterTests extends ESTestCase {

    public void testNeverSplit_GivenNoLowerCase() throws IOException {
        try (Analyzer analyzer = basicAnalyzerFromSettings(false, false, false, List.of("[UNK]"))) {
            var tokens = basicTokenize(analyzer, "1 (return) [ Patois ");
            assertThat(tokenStrings(tokens), contains("1", "(", "return", ")", "[", "Patois"));

            tokens = basicTokenize(analyzer, " \tHeLLo!how  \n Are yoU? [UNK]");
            assertThat(tokenStrings(tokens), contains("HeLLo", "!", "how", "Are", "yoU", "?", "[UNK]"));

            tokens = basicTokenize(analyzer, "Hello [UNK].");
            assertThat(tokenStrings(tokens), contains("Hello", "[UNK]", "."));

            tokens = basicTokenize(analyzer, "Hello [UNK]?");
            assertThat(tokenStrings(tokens), contains("Hello", "[UNK]", "?"));

            tokens = basicTokenize(analyzer, "Hello [UNK]!!");
            assertThat(tokenStrings(tokens), contains("Hello", "[UNK]", "!", "!"));

            tokens = basicTokenize(analyzer, "Hello-[UNK]");
            assertThat(tokenStrings(tokens), contains("Hello", "-", "[UNK]"));
            tokens = basicTokenize(analyzer, "Hello~[UNK][UNK]");
            assertThat(tokenStrings(tokens), contains("Hello", "~", "[UNK]", "[UNK]"));
            tokens = basicTokenize(analyzer, "Hello-[unk]");
            assertThat(tokenStrings(tokens), contains("Hello", "-", "[", "unk", "]"));
        }
    }

    public void testNeverSplit_GivenLowerCase() throws IOException {
        try (Analyzer analyzer = basicAnalyzerFromSettings(true, false, false, List.of("[UNK]"))) {
            var tokens = basicTokenize(analyzer, " \tHeLLo!how  \n Are yoU? [UNK]");
            assertThat(tokenStrings(tokens), contains("HeLLo", "!", "how", "Are", "yoU", "?", "[UNK]"));

            tokens = basicTokenize(analyzer, "Hello [UNK].");
            assertThat(tokenStrings(tokens), contains("Hello", "[UNK]", "."));

            tokens = basicTokenize(analyzer, "Hello [UNK]?");
            assertThat(tokenStrings(tokens), contains("Hello", "[UNK]", "?"));

            tokens = basicTokenize(analyzer, "Hello [UNK]!!");
            assertThat(tokenStrings(tokens), contains("Hello", "[UNK]", "!", "!"));

            tokens = basicTokenize(analyzer, "Hello-[UNK]");
            assertThat(tokenStrings(tokens), contains("Hello", "-", "[UNK]"));
            tokens = basicTokenize(analyzer, "Hello~[UNK][UNK]");
            assertThat(tokenStrings(tokens), contains("Hello", "~", "[UNK]", "[UNK]"));
            tokens = basicTokenize(analyzer, "Hello-[unk]");
            assertThat(tokenStrings(tokens), contains("Hello", "-", "[", "unk", "]"));
        }
    }

    public void testSplitCJK() throws Exception {
        try (Analyzer analyzer = basicAnalyzerFromSettings(true, true, false, List.of("[UNK]"))) {
            var tokens = basicTokenize(analyzer, "hello ah\u535A\u63A8zz");
            assertThat(tokenStrings(tokens), contains("hello", "ah", "\u535A", "\u63A8", "zz"));
        }
    }

    public void testSplitCJKWhenNoneExist() throws Exception {
        try (Analyzer analyzer = basicAnalyzerFromSettings(true, true, false, List.of("[UNK]"))) {
            var tokens = basicTokenize(analyzer, "hello world");
            assertThat(tokenStrings(tokens), contains("hello", "world"));
        }
    }

    public void testStripAccents() throws Exception {
        try (Analyzer analyzer = basicAnalyzerFromSettings(true, true, true, List.of("[UNK]"))) {
            var tokens = basicTokenize(analyzer, "HäLLo how are you");
            assertThat(tokenStrings(tokens), contains("HaLLo", "how", "are", "you"));
        }
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

    private List<String> tokenStrings(List<DelimitedToken> tokens) {
        return tokens.stream().map(DelimitedToken::charSequence).map(CharSequence::toString).collect(Collectors.toList());
    }

    public static Analyzer basicAnalyzerFromSettings(
        boolean isLowerCase,
        boolean isTokenizeCjkChars,
        boolean isStripAccents,
        List<String> neverSplit
    ) {
        return new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer t = new MockTokenizer(MockTokenizer.WHITESPACE, false);
                try {
                    return new TokenStreamComponents(
                        t,
                        BasicTokenFilter.buildFromSettings(isLowerCase, isTokenizeCjkChars, isStripAccents, neverSplit, t)
                    );
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
        boolean isLowerCase,
        boolean isTokenizeCjkChars,
        boolean isStripAccents,
        List<String> neverSplit,
        String input
    ) throws IOException {
        try (Analyzer analyzer = basicAnalyzerFromSettings(isLowerCase, isTokenizeCjkChars, isStripAccents, neverSplit)) {
            return basicTokenize(analyzer, input);
        }
    }

}
