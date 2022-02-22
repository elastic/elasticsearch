/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

public class WordPieceTokenFilterTests extends BaseTokenStreamTestCase {

    public static final String UNKNOWN_TOKEN = "[UNK]";

    public void testTokenize() throws IOException {
        List<String> vocab = List.of(UNKNOWN_TOKEN, "[CLS]", "[SEP]", "want", "##want", "##ed", "wa", "un", "runn", "##ing");
        TestNLPAnalyzer analyzer = new TestNLPAnalyzer(vocab, UNKNOWN_TOKEN, 512);

        assertAnalyzesTo(analyzer, "", new String[0]);
        assertAnalyzesTo(analyzer, "unwanted", new String[] { "un", "##want", "##ed" }, new int[] { 1, 0, 0 });
        assertAnalyzesTo(analyzer, "running", new String[] { "runn", "##ing" }, new int[] { 1, 0 });
        assertAnalyzesTo(analyzer, "unwantedX", new String[] { "[UNK]" }, new int[] { 1 });
    }

    public void testMaxCharLength() throws IOException {
        List<String> vocab = List.of(UNKNOWN_TOKEN, "[CLS]", "[SEP]", "want", "##want", "##ed", "wa", "un", "runn", "##ing", "become");
        TestNLPAnalyzer analyzer = new TestNLPAnalyzer(vocab, UNKNOWN_TOKEN, 4);

        assertAnalyzesTo(analyzer, "become", new String[] { UNKNOWN_TOKEN }, new int[] { 1 });
    }

    static class TestNLPAnalyzer extends Analyzer {
        private final List<String> dictionary;
        private final String unknownToken;
        private final int maxTokenSize;

        TestNLPAnalyzer(List<String> dictionary, String unknownToken, int maxTokenSize) {
            this.dictionary = dictionary;
            this.unknownToken = unknownToken;
            this.maxTokenSize = maxTokenSize;
        }

        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
            try {
                WhitespaceTokenizer tokenizer = new WhitespaceTokenizer(512);
                WordPieceTokenFilter filter = WordPieceTokenFilter.build(
                    false,
                    false,
                    false,
                    List.of(),
                    dictionary,
                    unknownToken,
                    maxTokenSize,
                    tokenizer
                );
                return new TokenStreamComponents(tokenizer, filter);
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
        }
    }
}
