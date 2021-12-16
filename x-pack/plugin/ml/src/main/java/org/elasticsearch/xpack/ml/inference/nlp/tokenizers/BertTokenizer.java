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

    private final Set<String> NEVER_SPLIT = Set.of(MASK_TOKEN);

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

    protected BertTokenizer(
        List<String> originalVocab,
        SortedMap<String, Integer> vocab,
        boolean doLowerCase,
        boolean doTokenizeCjKChars,
        boolean doStripAccents,
        boolean withSpecialTokens,
        int maxSequenceLength,
        Function<BertTokenizer, NlpTask.RequestBuilder> requestBuilderFactory,
        Set<String> neverSplit
    ) {
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
        if (vocab.containsKey(UNKNOWN_TOKEN) == false) {
            throw ExceptionsHelper.conflictStatusException("stored vocabulary is missing required [{}] token", UNKNOWN_TOKEN);
        }
        if (vocab.containsKey(PAD_TOKEN) == false) {
            throw ExceptionsHelper.conflictStatusException("stored vocabulary is missing required [{}] token", PAD_TOKEN);
        }

        if (withSpecialTokens) {
            Set<String> missingSpecialTokens = Sets.difference(Set.of(SEPARATOR_TOKEN, CLASS_TOKEN), vocab.keySet());
            if (missingSpecialTokens.isEmpty() == false) {
                throw ExceptionsHelper.conflictStatusException("stored vocabulary is missing required {} token(s)", missingSpecialTokens);
            }
        }
    }

    @Override
    public OptionalInt getPadTokenId() {
        Integer pad = vocab.get(PAD_TOKEN);
        if (pad != null) {
            return OptionalInt.of(pad);
        } else {
            return OptionalInt.empty();
        }
    }

    @Override
    public OptionalInt getMaskTokenId() {
        Integer pad = vocab.get(MASK_TOKEN);
        if (pad != null) {
            return OptionalInt.of(pad);
        } else {
            return OptionalInt.empty();
        }
    }

    @Override
    public String getMaskToken() {
        return MASK_TOKEN;
    }

    @Override
    public TokenizationResult buildTokenizationResult(List<TokenizationResult.Tokenization> tokenizations) {
        TokenizationResult tokenizationResult = new TokenizationResult(originalVocab);
        for (TokenizationResult.Tokenization tokenization : tokenizations) {
            tokenizationResult.addTokenization(tokenization);
        }
        return tokenizationResult;
    }

    /**
     * Tokenize the input according to the basic tokenization
     * options then perform Word Piece tokenization with the given vocabulary.
     *
     * The result is the Word Piece tokens, a map of the Word Piece
     * token position to the position of the token in the source for
     * each input string grouped into a {@link Tokenization}.
     *
     * @param seq Text to tokenize
     * @return A {@link Tokenization}
     */
    @Override
    public TokenizationResult.Tokenization tokenize(String seq, Tokenization.Truncate truncate) {
        var innerResult = innerTokenize(seq);
        List<Integer> wordPieceTokenIds = innerResult.wordPieceTokenIds;
        List<Integer> tokenPositionMap = innerResult.tokenPositionMap;
        int numTokens = withSpecialTokens ? wordPieceTokenIds.size() + 2 : wordPieceTokenIds.size();
        boolean isTruncated = false;

        if (numTokens > maxSequenceLength) {
            switch (truncate) {
                case FIRST:
                case SECOND:
                    isTruncated = true;
                    wordPieceTokenIds = wordPieceTokenIds.subList(0, withSpecialTokens ? maxSequenceLength - 2 : maxSequenceLength);
                    break;
                case NONE:
                    throw ExceptionsHelper.badRequestException(
                        "Input too large. The tokenized input length [{}] exceeds the maximum sequence length [{}]",
                        numTokens,
                        maxSequenceLength
                    );
            }
            numTokens = maxSequenceLength;
        }

        int[] tokenIds = new int[numTokens];
        int[] tokenMap = new int[numTokens];

        if (withSpecialTokens) {
            tokenIds[0] = vocab.get(CLASS_TOKEN);
            tokenMap[0] = SPECIAL_TOKEN_POSITION;
        }

        int i = withSpecialTokens ? 1 : 0;
        final int decrementHandler = withSpecialTokens ? 1 : 0;
        for (var tokenId : wordPieceTokenIds) {
            tokenIds[i] = tokenId;
            tokenMap[i] = tokenPositionMap.get(i - decrementHandler);
            i++;
        }

        if (withSpecialTokens) {
            tokenIds[i] = vocab.get(SEPARATOR_TOKEN);
            tokenMap[i] = SPECIAL_TOKEN_POSITION;
        }

        return new TokenizationResult.Tokenization(seq, innerResult.tokens, isTruncated, tokenIds, tokenMap);
    }

    @Override
    public TokenizationResult.Tokenization tokenize(String seq1, String seq2, Tokenization.Truncate truncate) {
        var innerResultSeq1 = innerTokenize(seq1);
        List<Integer> wordPieceTokenIdsSeq1 = innerResultSeq1.wordPieceTokenIds;
        List<Integer> tokenPositionMapSeq1 = innerResultSeq1.tokenPositionMap;
        var innerResultSeq2 = innerTokenize(seq2);
        List<Integer> wordPieceTokenIdsSeq2 = innerResultSeq2.wordPieceTokenIds;
        List<Integer> tokenPositionMapSeq2 = innerResultSeq2.tokenPositionMap;
        if (withSpecialTokens == false) {
            throw new IllegalArgumentException("Unable to do sequence pair tokenization without special tokens");
        }
        // [CLS] seq1 [SEP] seq2 [SEP]
        int numTokens = wordPieceTokenIdsSeq1.size() + wordPieceTokenIdsSeq2.size() + 3;

        boolean isTruncated = false;
        if (numTokens > maxSequenceLength) {
            switch (truncate) {
                case FIRST:
                    isTruncated = true;
                    if (wordPieceTokenIdsSeq2.size() > maxSequenceLength - 3) {
                        throw ExceptionsHelper.badRequestException(
                            "Attempting truncation [{}] but input is too large for the second sequence. "
                                + "The tokenized input length [{}] exceeds the maximum sequence length [{}], "
                                + "when taking special tokens into account",
                            truncate.toString(),
                            wordPieceTokenIdsSeq2.size(),
                            maxSequenceLength - 3
                        );
                    }
                    wordPieceTokenIdsSeq1 = wordPieceTokenIdsSeq1.subList(0, maxSequenceLength - 3 - wordPieceTokenIdsSeq2.size());
                    break;
                case SECOND:
                    isTruncated = true;
                    if (wordPieceTokenIdsSeq1.size() > maxSequenceLength - 3) {
                        throw ExceptionsHelper.badRequestException(
                            "Attempting truncation [{}] but input is too large for the first sequence. "
                                + "The tokenized input length [{}] exceeds the maximum sequence length [{}], "
                                + "when taking special tokens into account",
                            truncate.toString(),
                            wordPieceTokenIdsSeq1.size(),
                            maxSequenceLength - 3
                        );
                    }
                    wordPieceTokenIdsSeq2 = wordPieceTokenIdsSeq2.subList(0, maxSequenceLength - 3 - wordPieceTokenIdsSeq1.size());
                    break;
                case NONE:
                    throw ExceptionsHelper.badRequestException(
                        "Input too large. The tokenized input length [{}] exceeds the maximum sequence length [{}]",
                        numTokens,
                        maxSequenceLength
                    );
            }
            numTokens = maxSequenceLength;
        }
        int[] tokenIds = new int[numTokens];
        int[] tokenMap = new int[numTokens];

        tokenIds[0] = vocab.get(CLASS_TOKEN);
        tokenMap[0] = SPECIAL_TOKEN_POSITION;

        int i = 1;
        for (var tokenId : wordPieceTokenIdsSeq1) {
            tokenIds[i] = tokenId;
            tokenMap[i] = tokenPositionMapSeq1.get(i - 1);
            i++;
        }
        tokenIds[i] = vocab.get(SEPARATOR_TOKEN);
        tokenMap[i] = SPECIAL_TOKEN_POSITION;
        ++i;

        int j = 0;
        for (var tokenId : wordPieceTokenIdsSeq2) {
            tokenIds[i] = tokenId;
            tokenMap[i] = tokenPositionMapSeq2.get(j);
            i++;
            j++;
        }

        tokenIds[i] = vocab.get(SEPARATOR_TOKEN);
        tokenMap[i] = SPECIAL_TOKEN_POSITION;

        List<DelimitedToken> tokens = new ArrayList<>(innerResultSeq1.tokens);
        tokens.addAll(innerResultSeq2.tokens);
        return new TokenizationResult.Tokenization(seq1 + seq2, tokens, isTruncated, tokenIds, tokenMap);
    }

    private InnerTokenization innerTokenize(String seq) {
        BasicTokenizer basicTokenizer = new BasicTokenizer(doLowerCase, doTokenizeCjKChars, doStripAccents, neverSplit);
        var tokenSequences = basicTokenizer.tokenize(seq);
        List<Integer> wordPieceTokens = new ArrayList<>();
        List<Integer> tokenPositionMap = new ArrayList<>();

        for (int sourceIndex = 0; sourceIndex < tokenSequences.size(); sourceIndex++) {
            String token = tokenSequences.get(sourceIndex).getToken();
            if (neverSplit.contains(token)) {
                wordPieceTokens.add(vocab.getOrDefault(token, vocab.get(UNKNOWN_TOKEN)));
                tokenPositionMap.add(sourceIndex);
            } else {
                List<Integer> tokens = wordPieceTokenizer.tokenize(tokenSequences.get(sourceIndex));
                for (int tokenCount = 0; tokenCount < tokens.size(); tokenCount++) {
                    tokenPositionMap.add(sourceIndex);
                }
                wordPieceTokens.addAll(tokens);
            }
        }

        return new InnerTokenization(tokenSequences, wordPieceTokens, tokenPositionMap);
    }

    private static class InnerTokenization {
        List<DelimitedToken> tokens;
        List<Integer> wordPieceTokenIds;
        List<Integer> tokenPositionMap;

        InnerTokenization(List<DelimitedToken> tokens, List<Integer> wordPieceTokenIds, List<Integer> tokenPositionMap) {
            this.tokens = tokens;
            this.wordPieceTokenIds = wordPieceTokenIds;
            this.tokenPositionMap = tokenPositionMap;
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
