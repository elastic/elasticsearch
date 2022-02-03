/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;

import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.util.List;

public class WordPieceAnalyzer extends Analyzer {
    private final List<String> vocabulary;
    private final List<String> neverSplit;
    private final boolean doLowerCase;
    private final boolean doTokenizeCjKChars;
    private final boolean doStripAccents;
    private WordPieceTokenFilter innerTokenFilter;
    private final String unknownToken;

    public WordPieceAnalyzer(
        List<String> vocabulary,
        List<String> neverSplit,
        boolean doLowerCase,
        boolean doTokenizeCjKChars,
        boolean doStripAccents,
        String unknownToken
    ) {
        this.vocabulary = vocabulary;
        this.neverSplit = neverSplit;
        this.doLowerCase = doLowerCase;
        this.doTokenizeCjKChars = doTokenizeCjKChars;
        this.doStripAccents = doStripAccents;
        this.unknownToken = unknownToken;
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        try {
            WhitespaceTokenizer tokenizer = new WhitespaceTokenizer(512);
            innerTokenFilter = WordPieceTokenFilter.build(
                doLowerCase,
                doTokenizeCjKChars,
                doStripAccents,
                neverSplit,
                vocabulary,
                unknownToken,
                100,
                tokenizer
            );
            return new TokenStreamComponents(tokenizer, innerTokenFilter);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    public List<WordPieceTokenFilter.WordPieceToken> getTokens() {
        if (innerTokenFilter != null) {
            return innerTokenFilter.getTokenizedValues();
        } else {
            return List.of();
        }
    }

    @Override
    protected Reader initReader(String fieldName, Reader reader) {
        return new ControlCharFilter(reader);
    }

    @Override
    protected Reader initReaderForNormalization(String fieldName, Reader reader) {
        return new ControlCharFilter(reader);
    }
}
