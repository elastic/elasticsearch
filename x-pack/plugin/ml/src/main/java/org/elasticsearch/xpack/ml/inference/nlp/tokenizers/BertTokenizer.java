/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.Tokenization;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.inference.nlp.BertRequestBuilder;
import org.elasticsearch.xpack.ml.inference.nlp.NlpTask;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;

/**
 * Performs basic tokenization and normalization of input text
 * then tokenizes with the WordPiece algorithm using the given
 * vocabulary.
 * <p>
 * Derived from
 * https://github.com/huggingface/transformers/blob/ba8c4d0ac04acfcdbdeaed954f698d6d5ec3e532/src/transformers/tokenization_bert.py
 */
public class BertTokenizer implements NlpTokenizer {

    public static final String UNKNOWN_TOKEN = "[UNK]";
    public static final String SEPARATOR_TOKEN = "[SEP]";
    public static final String PAD_TOKEN = "[PAD]";
    public static final String CLASS_TOKEN = "[CLS]";
    public static final String MASK_TOKEN = "[MASK]";

    public static final int SPECIAL_TOKEN_POSITION = -1;

    public static final int DEFAULT_MAX_INPUT_CHARS_PER_WORD = 100;

    private final Set<String> NEVER_SPLIT =  Set.of(MASK_TOKEN);

    private final WordPieceTokenizer wordPieceTokenizer;
    private final List<String> originalVocab;
    // TODO Not sure this needs to be a sorted map
    private final SortedMap<String, Integer> vocab;
    private final boolean doLowerCase;
    private final boolean doTokenizeCjKChars;
    private final boolean doStripAccents;
    private final boolean withSpecialTokens;
    private final Set<String> neverSplit;
    private final int maxSequenceLength;
    private final NlpTask.RequestBuilder requestBuilder;

    protected BertTokenizer(List<String> originalVocab,
                            SortedMap<String, Integer> vocab,
                            boolean doLowerCase,
                            boolean doTokenizeCjKChars,
                            boolean doStripAccents,
                            boolean withSpecialTokens,
                            int maxSequenceLength,
                            Function<BertTokenizer, NlpTask.RequestBuilder> requestBuilderFactory,
                            Set<String> neverSplit) {
        wordPieceTokenizer = new WordPieceTokenizer(vocab, UNKNOWN_TOKEN, DEFAULT_MAX_INPUT_CHARS_PER_WORD);
        this.originalVocab = originalVocab;
        this.vocab = vocab;
        this.doLowerCase = doLowerCase;
        this.doTokenizeCjKChars = doTokenizeCjKChars;
        this.doStripAccents = doStripAccents;
        this.withSpecialTokens = withSpecialTokens;
        this.neverSplit = Sets.union(neverSplit, NEVER_SPLIT);
        this.maxSequenceLength = maxSequenceLength;
        this.requestBuilder = requestBuilderFactory.apply(this);
    }

    /**
     * Tokenize the list of inputs according to the basic tokenization
     * options then perform Word Piece tokenization with the given vocabulary.
     *
     * The result is the Word Piece tokens, a map of the Word Piece
     * token position to the position of the token in the source for
     * each input string grouped into a {@link Tokenization}.
     *
     * @param text Text to tokenize
     * @return A {@link Tokenization}
     */
    @Override
    public TokenizationResult tokenize(List<String> text) {
        TokenizationResult tokenization = new TokenizationResult(originalVocab);

        for (String input: text) {
            addTokenization(tokenization, input);
        }
        return tokenization;
    }


    private void addTokenization(TokenizationResult tokenization, String text) {
        BasicTokenizer basicTokenizer = new BasicTokenizer(doLowerCase, doTokenizeCjKChars, doStripAccents, neverSplit);

        List<String> delineatedTokens = basicTokenizer.tokenize(text);
        List<WordPieceTokenizer.TokenAndId> wordPieceTokens = new ArrayList<>();
        List<Integer> tokenPositionMap = new ArrayList<>();
        if (withSpecialTokens) {
            // insert the first token to simplify the loop counter logic later
            tokenPositionMap.add(SPECIAL_TOKEN_POSITION);
        }

        for (int sourceIndex = 0; sourceIndex < delineatedTokens.size(); sourceIndex++) {
            String token = delineatedTokens.get(sourceIndex);
            if (neverSplit.contains(token)) {
                wordPieceTokens.add(new WordPieceTokenizer.TokenAndId(token, vocab.getOrDefault(token, vocab.get(UNKNOWN_TOKEN))));
                tokenPositionMap.add(sourceIndex);
            } else {
                List<WordPieceTokenizer.TokenAndId> tokens = wordPieceTokenizer.tokenize(token);
                for (int tokenCount = 0; tokenCount < tokens.size(); tokenCount++) {
                    tokenPositionMap.add(sourceIndex);
                }
                wordPieceTokens.addAll(tokens);
            }
        }

        int numTokens = withSpecialTokens ? wordPieceTokens.size() + 2 : wordPieceTokens.size();
        List<String> tokens = new ArrayList<>(numTokens);
        int [] tokenIds = new int[numTokens];
        int [] tokenMap = new int[numTokens];

        if (withSpecialTokens) {
            tokens.add(CLASS_TOKEN);
            tokenIds[0] = vocab.get(CLASS_TOKEN);
            tokenMap[0] = SPECIAL_TOKEN_POSITION;
        }

        int i = withSpecialTokens ? 1 : 0;
        for (WordPieceTokenizer.TokenAndId tokenAndId : wordPieceTokens) {
            tokens.add(tokenAndId.getToken());
            tokenIds[i] = tokenAndId.getId();
            tokenMap[i] = tokenPositionMap.get(i);
            i++;
        }

        if (withSpecialTokens) {
            tokens.add(SEPARATOR_TOKEN);
            tokenIds[i] = vocab.get(SEPARATOR_TOKEN);
            tokenMap[i] = SPECIAL_TOKEN_POSITION;
        }

        if (tokenIds.length > maxSequenceLength) {
            throw ExceptionsHelper.badRequestException(
                "Input too large. The tokenized input length [{}] exceeds the maximum sequence length [{}]",
                tokenIds.length,
                maxSequenceLength
            );
        }

        tokenization.addTokenization(text, tokens, tokenIds, tokenMap);
    }

    @Override
    public OptionalInt getPadToken() {
        Integer pad = vocab.get(PAD_TOKEN);
        if (pad != null) {
            return OptionalInt.of(pad);
        } else {
            return OptionalInt.empty();
        }
    }

    @Override
    public NlpTask.RequestBuilder requestBuilder() {
        return requestBuilder;
    }

    public int getMaxSequenceLength() {
        return maxSequenceLength;
    }

    public static Builder builder(List<String> vocab, Tokenization tokenization) {
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
        protected Function<BertTokenizer, NlpTask.RequestBuilder> requestBuilderFactory = BertRequestBuilder::new;

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

        public Builder setRequestBuilderFactory(Function<BertTokenizer, NlpTask.RequestBuilder> requestBuilderFactory) {
            this.requestBuilderFactory = requestBuilderFactory;
            return this;
        }

        public BertTokenizer build() {
            // if not set strip accents defaults to the value of doLowerCase
            if (doStripAccents == null) {
                doStripAccents = doLowerCase;
            }

            if (neverSplit == null) {
                neverSplit = Collections.emptySet();
            }

            return new BertTokenizer(
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
