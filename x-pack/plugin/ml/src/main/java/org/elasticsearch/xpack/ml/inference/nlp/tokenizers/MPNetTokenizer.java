/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.Tokenization;
import org.elasticsearch.xpack.ml.inference.nlp.MPNetRequestBuilder;
import org.elasticsearch.xpack.ml.inference.nlp.NlpTask;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.IntStream;

/**
 * Performs basic tokenization and normalization of input text
 * then tokenizes with the WordPiece algorithm using the given
 * vocabulary.
 */
public class MPNetTokenizer extends BertTokenizer {

    public static final String UNKNOWN_TOKEN = "[UNK]";
    public static final String SEPARATOR_TOKEN = "</s>";
    public static final String PAD_TOKEN = "<pad>";
    public static final String CLASS_TOKEN = "<s>";
    public static final String MASK_TOKEN = "<mask>";
    private static final Set<String> NEVER_SPLIT = Set.of(MASK_TOKEN);

    protected MPNetTokenizer(
        List<String> originalVocab,
        SortedMap<String, Integer> vocab,
        boolean doLowerCase,
        boolean doTokenizeCjKChars,
        boolean doStripAccents,
        boolean withSpecialTokens,
        int maxSequenceLength,
        Function<NlpTokenizer, NlpTask.RequestBuilder> requestBuilderFactory,
        Set<String> neverSplit
    ) {
        super(
            originalVocab,
            vocab,
            doLowerCase,
            doTokenizeCjKChars,
            doStripAccents,
            withSpecialTokens,
            maxSequenceLength,
            requestBuilderFactory,
            Sets.union(neverSplit, NEVER_SPLIT),
            SEPARATOR_TOKEN,
            CLASS_TOKEN,
            PAD_TOKEN,
            MASK_TOKEN,
            UNKNOWN_TOKEN
        );
    }

    @Override
    protected int getNumExtraTokensForSeqPair() {
        return 4;
    }

    @Override
    protected BertTokenizationBuilder bertTokenizationBuilder() {
        return new MPNetTokenizationBuilder();
    }

    protected class MPNetTokenizationBuilder extends BertTokenizationBuilder {

        @Override
        BertTokenizationBuilder addTokens(List<Integer> wordPieceTokenIds, List<Integer> tokenPositionMap) {
            if (numSeq > 0 && withSpecialTokens) {
                tokenIds.add(IntStream.of(sepTokenId, sepTokenId));
                tokenMap.add(IntStream.of(SPECIAL_TOKEN_POSITION, SPECIAL_TOKEN_POSITION));
            }
            tokenIds.add(wordPieceTokenIds.stream().mapToInt(Integer::valueOf));
            tokenMap.add(tokenPositionMap.stream().mapToInt(Integer::valueOf));
            numSeq++;
            return this;
        }

    }

    public static Builder mpBuilder(List<String> vocab, Tokenization tokenization) {
        return new Builder(vocab, tokenization);
    }

    public static class Builder {

        protected final List<String> originalVocab;
        protected final SortedMap<String, Integer> vocab;
        protected boolean doLowerCase = false;
        protected boolean doTokenizeCjKChars = true;
        protected boolean withSpecialTokens = true;
        protected int maxSequenceLength;
        protected Boolean doStripAccents = null;
        protected Set<String> neverSplit;
        protected Function<NlpTokenizer, NlpTask.RequestBuilder> requestBuilderFactory = MPNetRequestBuilder::new;

        protected Builder(List<String> vocab, Tokenization tokenization) {
            this.originalVocab = vocab;
            this.vocab = buildSortedVocab(vocab);
            this.doLowerCase = tokenization.doLowerCase();
            this.withSpecialTokens = tokenization.withSpecialTokens();
            this.maxSequenceLength = tokenization.maxSequenceLength();
        }

        private static SortedMap<String, Integer> buildSortedVocab(List<String> vocab) {
            SortedMap<String, Integer> sortedVocab = new TreeMap<>();
            for (int i = 0; i < vocab.size(); i++) {
                sortedVocab.put(vocab.get(i), i);
            }
            return sortedVocab;
        }

        public Builder setDoLowerCase(boolean doLowerCase) {
            this.doLowerCase = doLowerCase;
            return this;
        }

        public Builder setDoTokenizeCjKChars(boolean doTokenizeCjKChars) {
            this.doTokenizeCjKChars = doTokenizeCjKChars;
            return this;
        }

        public Builder setDoStripAccents(Boolean doStripAccents) {
            this.doStripAccents = doStripAccents;
            return this;
        }

        public Builder setNeverSplit(Set<String> neverSplit) {
            this.neverSplit = neverSplit;
            return this;
        }

        public Builder setMaxSequenceLength(int maxSequenceLength) {
            this.maxSequenceLength = maxSequenceLength;
            return this;
        }

        /**
         * Include CLS and SEP tokens
         * @param withSpecialTokens if true include CLS and SEP tokens
         * @return this
         */
        public Builder setWithSpecialTokens(boolean withSpecialTokens) {
            this.withSpecialTokens = withSpecialTokens;
            return this;
        }

        public Builder setRequestBuilderFactory(Function<NlpTokenizer, NlpTask.RequestBuilder> requestBuilderFactory) {
            this.requestBuilderFactory = requestBuilderFactory;
            return this;
        }

        public MPNetTokenizer build() {
            // if not set strip accents defaults to the value of doLowerCase
            if (doStripAccents == null) {
                doStripAccents = doLowerCase;
            }

            if (neverSplit == null) {
                neverSplit = Collections.emptySet();
            }

            return new MPNetTokenizer(
                originalVocab,
                vocab,
                doLowerCase,
                doTokenizeCjKChars,
                doStripAccents,
                withSpecialTokens,
                maxSequenceLength,
                requestBuilderFactory,
                neverSplit
            );
        }
    }
}
