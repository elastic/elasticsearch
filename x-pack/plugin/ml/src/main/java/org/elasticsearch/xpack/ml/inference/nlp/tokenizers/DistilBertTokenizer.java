/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TokenizationParams;
import org.elasticsearch.xpack.ml.inference.nlp.DistilBertRequestBuilder;
import org.elasticsearch.xpack.ml.inference.nlp.NlpTask;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;

/**
 * Specialization of the BERT Tokenizer for distilbert inference
 */
public class DistilBertTokenizer extends BertTokenizer {

    private DistilBertTokenizer(List<String> originalVocab,
                                SortedMap<String, Integer> vocab,
                                boolean doLowerCase,
                                boolean doTokenizeCjKChars,
                                boolean doStripAccents,
                                boolean withSpecialTokens,
                                int maxSequenceLength,
                                Set<String> neverSplit) {
        super(originalVocab, vocab, doLowerCase, doTokenizeCjKChars, doStripAccents, withSpecialTokens, maxSequenceLength, neverSplit);
    }

    @Override
    public NlpTask.RequestBuilder requestBuilder(NlpConfig config) {
        return new DistilBertRequestBuilder(this);
    }

    public static Builder builder(List<String> vocab, TokenizationParams tokenizationParams) {
        return new Builder(vocab, tokenizationParams);
    }

    public static class Builder extends BertTokenizer.Builder {
        private Builder(List<String> vocab, TokenizationParams tokenizationParams) {
            super(vocab, tokenizationParams);
        }

        public DistilBertTokenizer build() {
            // if not set strip accents defaults to the value of doLowerCase
            if (doStripAccents == null) {
                doStripAccents = doLowerCase;
            }

            if (neverSplit == null) {
                neverSplit = Collections.emptySet();
            }

            return new DistilBertTokenizer(
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
