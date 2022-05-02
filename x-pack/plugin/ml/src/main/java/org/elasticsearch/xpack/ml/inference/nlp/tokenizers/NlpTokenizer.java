/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.elasticsearch.core.Releasable;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.BertTokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.MPNetTokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RobertaTokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.Tokenization;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.inference.nlp.NlpTask;
import org.elasticsearch.xpack.ml.inference.nlp.Vocabulary;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig.TOKENIZATION;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig.VOCABULARY;

/**
 * Base tokenization class for NLP models
 */
public abstract class NlpTokenizer implements Releasable {
    abstract int clsTokenId();

    abstract int sepTokenId();

    abstract int maxSequenceLength();

    abstract boolean isWithSpecialTokens();

    abstract int getNumExtraTokensForSeqPair();

    public abstract TokenizationResult buildTokenizationResult(List<TokenizationResult.Tokens> tokenizations);

    /**
     * Tokenize the input according to the basic tokenization
     * options then perform the configured tokenization with the given vocabulary.
     *
     * The result is the tokens ids, a map of the
     * token position to the position of the token in the source for
     * each input string grouped into a {@link Tokenization}.
     *
     * @param seq Text to tokenize
     * @return A list of {@link Tokenization}
     */
    public List<TokenizationResult.Tokens> tokenize(String seq, Tokenization.Truncate truncate, int span, int sequenceId) {
        var innerResult = innerTokenize(seq);
        List<? extends DelimitedToken.Encoded> tokenIds = innerResult.tokens();
        List<Integer> tokenPositionMap = innerResult.tokenPositionMap();
        int numTokens = isWithSpecialTokens() ? tokenIds.size() + 2 : tokenIds.size();
        boolean isTruncated = false;

        if (numTokens > maxSequenceLength()) {
            switch (truncate) {
                case FIRST, SECOND -> {
                    isTruncated = true;
                    tokenIds = tokenIds.subList(0, isWithSpecialTokens() ? maxSequenceLength() - 2 : maxSequenceLength());
                    tokenPositionMap = tokenPositionMap.subList(0, isWithSpecialTokens() ? maxSequenceLength() - 2 : maxSequenceLength());
                }
                case NONE -> {
                    if (span == -1) {
                        throw ExceptionsHelper.badRequestException(
                            "Input too large. The tokenized input length [{}] exceeds the maximum sequence length [{}]",
                            numTokens,
                            maxSequenceLength()
                        );
                    }
                }
            }
        }

        if (numTokens <= maxSequenceLength() || span == -1) {
            return List.of(
                createTokensBuilder(clsTokenId(), sepTokenId(), isWithSpecialTokens()).addSequence(
                    tokenIds.stream().map(DelimitedToken.Encoded::getEncoding).collect(Collectors.toList()),
                    tokenPositionMap
                ).build(seq, isTruncated, innerResult.tokens, -1, sequenceId)
            );
        }

        List<TokenizationResult.Tokens> toReturn = new ArrayList<>();
        int splitEndPos = 0;
        int splitStartPos = 0;
        int spanPrev = -1;
        while (splitEndPos < tokenIds.size()) {
            splitEndPos = Math.min(
                splitStartPos + (isWithSpecialTokens() ? maxSequenceLength() - 2 : maxSequenceLength()),
                tokenIds.size()
            );
            // Make sure we do not end on a word
            if (splitEndPos != tokenIds.size()) {
                while (Objects.equals(tokenPositionMap.get(splitEndPos), tokenPositionMap.get(splitEndPos - 1))) {
                    splitEndPos--;
                }
            }

            toReturn.add(
                createTokensBuilder(clsTokenId(), sepTokenId(), isWithSpecialTokens()).addSequence(
                    tokenIds.subList(splitStartPos, splitEndPos)
                        .stream()
                        .map(DelimitedToken.Encoded::getEncoding)
                        .collect(Collectors.toList()),
                    tokenPositionMap.subList(splitStartPos, splitEndPos)
                ).build(seq, false, innerResult.tokens, spanPrev, sequenceId)
            );
            spanPrev = span;
            int prevSplitStart = splitStartPos;
            splitStartPos = splitEndPos - span;
            // try to back up our split so that it starts at the first whole word
            if (splitStartPos < tokenIds.size()) {
                while (splitStartPos > (prevSplitStart + 1)
                    && Objects.equals(tokenPositionMap.get(splitStartPos), tokenPositionMap.get(splitStartPos - 1))) {
                    splitStartPos--;
                    spanPrev++;
                }
            }
        }
        return toReturn;
    }

    /**
     * Tokenize the sequence pair
     * @param seq1 The first sequence in the pair
     * @param seq2 The second sequence
     * @param truncate truncate settings
     * @param sequenceId The unique id for this tokenization request
     * @return tokenization result for the sequence pair
     */
    public TokenizationResult.Tokens tokenize(String seq1, String seq2, Tokenization.Truncate truncate, int sequenceId) {
        return tokenize(seq1, innerTokenize(seq1), seq2, truncate, sequenceId);
    }

    /**
     * The same as {@link NlpTokenizer#tokenize(String, String, Tokenization.Truncate, int)} but allows for tokenizing the first sequence
     * only once. Useful for zero shot classification.
     * @param seq1 The first sequence
     * @param innerResultSeq1 The tokenization of the first sequence
     * @param seq2 The second sequence in the pair
     * @param truncate truncate settings
     * @param sequenceId The unique id for this tokenization request
     * @return tokenization result for the sequence pair
     */
    public TokenizationResult.Tokens tokenize(
        String seq1,
        InnerTokenization innerResultSeq1,
        String seq2,
        Tokenization.Truncate truncate,
        int sequenceId
    ) {
        List<? extends DelimitedToken.Encoded> tokenIdsSeq1 = innerResultSeq1.tokens;
        List<Integer> tokenPositionMapSeq1 = innerResultSeq1.tokenPositionMap;
        var innerResultSeq2 = innerTokenize(seq2);
        List<? extends DelimitedToken.Encoded> tokenIdsSeq2 = innerResultSeq2.tokens;
        List<Integer> tokenPositionMapSeq2 = innerResultSeq2.tokenPositionMap;
        if (isWithSpecialTokens() == false) {
            throw new IllegalArgumentException("Unable to do sequence pair tokenization without special tokens");
        }
        int extraTokens = getNumExtraTokensForSeqPair();
        int numTokens = tokenIdsSeq1.size() + tokenIdsSeq2.size() + extraTokens;

        boolean isTruncated = false;
        if (numTokens > maxSequenceLength()) {
            switch (truncate) {
                case FIRST -> {
                    isTruncated = true;
                    if (tokenIdsSeq2.size() > maxSequenceLength() - extraTokens) {
                        throw ExceptionsHelper.badRequestException(
                            "Attempting truncation [{}] but input is too large for the second sequence. "
                                + "The tokenized input length [{}] exceeds the maximum sequence length [{}], "
                                + "when taking special tokens into account",
                            truncate.toString(),
                            tokenIdsSeq2.size(),
                            maxSequenceLength() - extraTokens
                        );
                    }
                    tokenIdsSeq1 = tokenIdsSeq1.subList(0, maxSequenceLength() - extraTokens - tokenIdsSeq2.size());
                    tokenPositionMapSeq1 = tokenPositionMapSeq1.subList(0, maxSequenceLength() - extraTokens - tokenIdsSeq2.size());
                }
                case SECOND -> {
                    isTruncated = true;
                    if (tokenIdsSeq1.size() > maxSequenceLength() - extraTokens) {
                        throw ExceptionsHelper.badRequestException(
                            "Attempting truncation [{}] but input is too large for the first sequence. "
                                + "The tokenized input length [{}] exceeds the maximum sequence length [{}], "
                                + "when taking special tokens into account",
                            truncate.toString(),
                            tokenIdsSeq1.size(),
                            maxSequenceLength() - extraTokens
                        );
                    }
                    tokenIdsSeq2 = tokenIdsSeq2.subList(0, maxSequenceLength() - extraTokens - tokenIdsSeq1.size());
                    tokenPositionMapSeq2 = tokenPositionMapSeq2.subList(0, maxSequenceLength() - extraTokens - tokenIdsSeq1.size());
                }
                case NONE -> throw ExceptionsHelper.badRequestException(
                    "Input too large. The tokenized input length [{}] exceeds the maximum sequence length [{}]",
                    numTokens,
                    maxSequenceLength()
                );
            }
        }
        List<DelimitedToken.Encoded> tokens = new ArrayList<>(innerResultSeq1.tokens);
        tokens.addAll(innerResultSeq2.tokens);
        return createTokensBuilder(clsTokenId(), sepTokenId(), isWithSpecialTokens()).addSequencePair(
            tokenIdsSeq1.stream().map(DelimitedToken.Encoded::getEncoding).collect(Collectors.toList()),
            tokenPositionMapSeq1,
            tokenIdsSeq2.stream().map(DelimitedToken.Encoded::getEncoding).collect(Collectors.toList()),
            tokenPositionMapSeq2
        ).build(seq1 + seq2, isTruncated, tokens, -1, sequenceId);
    }

    public abstract NlpTask.RequestBuilder requestBuilder();

    public abstract OptionalInt getPadTokenId();

    public abstract String getPadToken();

    public abstract OptionalInt getMaskTokenId();

    public abstract String getMaskToken();

    public int getSpan() {
        return -1;
    }

    abstract TokenizationResult.TokensBuilder createTokensBuilder(int clsTokenId, int sepTokenId, boolean withSpecialTokens);

    public abstract InnerTokenization innerTokenize(String seq);

    public static NlpTokenizer build(Vocabulary vocabulary, Tokenization params) {
        ExceptionsHelper.requireNonNull(params, TOKENIZATION);
        ExceptionsHelper.requireNonNull(vocabulary, VOCABULARY);
        if (params instanceof BertTokenization) {
            return BertTokenizer.builder(vocabulary.get(), params).build();
        }
        if (params instanceof MPNetTokenization) {
            return MPNetTokenizer.mpBuilder(vocabulary.get(), params).build();
        }
        if (params instanceof RobertaTokenization robertaTokenization) {
            return RobertaTokenizer.builder(vocabulary.get(), vocabulary.merges(), robertaTokenization).build();
        }
        throw new IllegalArgumentException("unknown tokenization type [" + params.getName() + "]");
    }

    public record InnerTokenization(List<? extends DelimitedToken.Encoded> tokens, List<Integer> tokenPositionMap) {}
}
