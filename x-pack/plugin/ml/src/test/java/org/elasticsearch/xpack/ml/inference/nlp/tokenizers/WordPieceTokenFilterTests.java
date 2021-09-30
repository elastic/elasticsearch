/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;

public class WordPieceTokenFilterTests extends ESTestCase {

    public static final String UNKNOWN_TOKEN = "[UNK]";

    public void testTokenize() throws IOException {
        List<String> vocab = List.of(UNKNOWN_TOKEN, "[CLS]", "[SEP]", "want", "##want", "##ed", "wa", "un", "runn", "##ing");

        var tokenIds = buildAndTokenize(vocab, UNKNOWN_TOKEN, "", 512);
        assertThat(tokenIds, empty());

        tokenIds = buildAndTokenize(vocab, UNKNOWN_TOKEN, "unwanted", 512);
        assertThat(
            tokenIds.stream().map(WordPieceTokenFilter.WordPieceToken::toString).collect(Collectors.toList()),
            contains("un", "##want", "##ed")
        );

        tokenIds = buildAndTokenize(vocab, UNKNOWN_TOKEN, "running", 512);
        assertThat(
            tokenIds.stream().map(WordPieceTokenFilter.WordPieceToken::toString).collect(Collectors.toList()),
            contains("runn", "##ing")
        );

        tokenIds = buildAndTokenize(vocab, UNKNOWN_TOKEN, "unwantedX", 512);
        assertThat(
            tokenIds.stream().map(WordPieceTokenFilter.WordPieceToken::toString).collect(Collectors.toList()),
            contains("un", "##want", "##ed", "[UNK]")
        );
    }

    public void testMaxCharLength() throws IOException {
        List<String> vocab = List.of("Some", "words", "will", "become", "UNK");

        var tokenIds = buildAndTokenize(vocab, "UNK", "running", 4);
        assertThat(tokenIds.stream().map(WordPieceTokenFilter.WordPieceToken::toString).collect(Collectors.toList()), contains("UNK"));
    }

    static List<WordPieceTokenFilter.WordPieceToken> buildAndTokenize(
        List<String> dictionary,
        String unknownToken,
        String input,
        int maxTokenSize
    ) throws IOException {
        TestNLPAnalyzer analyzer = new TestNLPAnalyzer(dictionary, unknownToken, maxTokenSize);
        TokenStream test = analyzer.tokenStream("test", input);
        test.reset();
        while (test.incrementToken()) {
        }
        return analyzer.filter.getTokenizedValues();
    }

    static class TestNLPAnalyzer extends Analyzer {
        private WordPieceTokenFilter filter = null;
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
                filter = WordPieceTokenFilter.buildFromSettings(
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
