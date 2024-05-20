/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.elasticsearch.xpack.core.ml.inference.trainedmodel.Tokenization;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;

public class BertJapaneseTokenizer extends BertTokenizer {
    protected BertJapaneseTokenizer(
        List<String> originalVocab,
        SortedMap<String, Integer> vocab,
        boolean doLowerCase,
        boolean doTokenizeCjKChars,
        boolean doStripAccents,
        boolean withSpecialTokens,
        int maxSequenceLength,
        Set<String> neverSplit
    ) {
        super(originalVocab, vocab, doLowerCase, doTokenizeCjKChars, doStripAccents, withSpecialTokens, maxSequenceLength, neverSplit);
    }

    @Override
    protected WordPieceAnalyzer createWordPieceAnalyzer(
        List<String> vocabulary,
        List<String> neverSplit,
        boolean doLowerCase,
        boolean doTokenizeCjKChars,
        boolean doStripAccents,
        String unknownToken
    ) {
        return new JapaneseWordPieceAnalyzer(vocabulary, new ArrayList<>(neverSplit), doLowerCase, doStripAccents, unknownToken);
    }

    public static Builder builder(List<String> vocab, Tokenization tokenization) {
        return new JapaneseBuilder(vocab, tokenization);
    }

    public static class JapaneseBuilder extends BertTokenizer.Builder {

        protected JapaneseBuilder(List<String> vocab, Tokenization tokenization) {
            super(vocab, tokenization);
        }

        @Override
        public BertTokenizer build() {
            // if not set strip accents defaults to the value of doLowerCase
            if (doStripAccents == null) {
                doStripAccents = doLowerCase;
            }

            if (neverSplit == null) {
                neverSplit = Collections.emptySet();
            }

            return new BertJapaneseTokenizer(
                originalVocab,
                vocab,
                doLowerCase,
                doTokenizeCjKChars,
                doStripAccents,
                withSpecialTokens,
                maxSequenceLength,
                neverSplit
            );
        }
    }
}
