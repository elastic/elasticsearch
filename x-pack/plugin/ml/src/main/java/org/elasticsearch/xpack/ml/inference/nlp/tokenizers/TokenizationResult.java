/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.Tokenization;
import org.elasticsearch.xpack.ml.inference.nlp.NlpTask;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class TokenizationResult {
    public static final int SPECIAL_TOKEN_POSITION = -1;

    private final List<String> vocab;
    private final List<Tokens> tokens;
    private final int maxLength;
    private final int padTokenId;

    protected TokenizationResult(List<String> vocab, List<Tokens> tokenizations, int padTokenId) {
        this.vocab = vocab;
        this.tokens = tokenizations;
        this.padTokenId = padTokenId;
        int max = 0;
        Set<Integer> sequenceIds = new HashSet<>();
        for (Tokens tokenization : tokenizations) {
            max = Math.max(tokenization.tokenIds.length, max);
            if (sequenceIds.contains(tokenization.sequenceId()) && tokenization.spanPrev == -1) {
                throw new IllegalArgumentException("cannot window a sequence without a configured span");
            }
            sequenceIds.add(tokenization.sequenceId);
        }
        this.maxLength = max;
    }

    public Map<Integer, List<Tokens>> getTokensBySequenceId() {
        return tokens.stream().collect(Collectors.groupingBy(Tokens::sequenceId));
    }

    List<Tokens> getTokens() {
        return tokens;
    }

    public String getFromVocab(int tokenId) {
        return vocab.get(tokenId);
    }

    public String decode(String token) {
        return token;
    }

    public Tokens getTokenization(int tokenizationIndex) {
        return tokens.get(tokenizationIndex);
    }

    public boolean anyTruncated() {
        return tokens.stream().anyMatch(Tokens::truncated);
    }

    public boolean isEmpty() {
        return this.tokens.isEmpty() || this.tokens.stream().allMatch(t -> t.tokenIds.length == 0);
    }

    public abstract NlpTask.Request buildRequest(String requestId, Tokenization.Truncate t) throws IOException;

    protected void writePaddedTokens(String fieldName, XContentBuilder builder) throws IOException {
        builder.startArray(fieldName);
        for (var inputTokens : tokens) {
            builder.startArray();

            // Note, cannot write the array directly as the internal builder code writes start/end array values
            for (int t : inputTokens.tokenIds) {
                builder.value(t);
            }
            for (int i = inputTokens.tokenIds.length; i < maxLength; i++) {
                builder.value(padTokenId);
            }
            builder.endArray();
        }
        builder.endArray();
    }

    protected void writeAttentionMask(String fieldName, XContentBuilder builder) throws IOException {
        builder.startArray(fieldName);
        for (var inputTokens : tokens) {
            builder.startArray();
            // Note, cannot write the array directly as the internal builder code writes start/end array values
            for (int ignored : inputTokens.tokenIds) {
                builder.value(1);
            }
            for (int i = inputTokens.tokenIds.length; i < maxLength; i++) {
                builder.value(padTokenId);
            }
            builder.endArray();
        }
        builder.endArray();
    }

    protected void writeTokenTypeIds(String fieldName, XContentBuilder builder) throws IOException {
        builder.startArray(fieldName);
        for (int i = 0; i < tokens.size(); i++) {
            builder.startArray();
            for (int j = 0; j < maxLength; j++) {
                builder.value(0);
            }
            builder.endArray();
        }
        builder.endArray();
    }

    protected void writePositionIds(String fieldName, XContentBuilder builder) throws IOException {
        builder.startArray(fieldName);
        for (int i = 0; i < tokens.size(); i++) {
            builder.startArray();
            for (int j = 0; j < maxLength; j++) {
                builder.value(j);
            }
            builder.endArray();
        }
        builder.endArray();
    }

    /**
     * Tokenization of a sequence
     */
    public record Tokens(
        String input,
        List<? extends DelimitedToken> tokens,
        boolean truncated,
        int[] tokenIds,
        int[] tokenMap,
        int spanPrev,
        int sequenceId
    ) {
        /**
         *
         * @param input The whole sequence input
         * @param tokens The delimited tokens (includes original text offsets)
         * @param truncated Was this tokenization truncated
         * @param tokenIds The token ids
         * @param tokenMap The token positions
         * @param spanPrev How many of the previous sub-sequence does this tokenization include
         * @param sequenceId A unique sequence ID to allow sub-sequence reconstitution
         */
        public Tokens {
            assert tokenIds.length == tokenMap.length;
            if (spanPrev != -1 && truncated) {
                throw new IllegalArgumentException("should not truncate when windowing is enabled");
            }
        }

        public OptionalInt getTokenIndex(int token) {
            return IntStream.range(0, tokenIds.length).filter(tokenIndex -> token == tokenIds[tokenIndex]).findFirst();
        }
    }

    interface TokensBuilder {
        /**
         * Adds tokens to the token builder
         * @param tokenIds Token ids without special tokens added
         * @param tokenMap Token map without considering special tokens
         * @return The builder object
         */
        TokensBuilder addSequence(List<Integer> tokenIds, List<Integer> tokenMap);

        /**
         * Adds an encoded sequence pair to the token builder
         * @param tokenId1s Sequence 1 ids
         * @param tokenMap1 Sequence 1 token mappings
         * @param tokenId2s Sequence 2 ids
         * @param tokenMap2 Sequence 2 token map
         * @return The builder object
         */
        TokensBuilder addSequencePair(List<Integer> tokenId1s, List<Integer> tokenMap1, List<Integer> tokenId2s, List<Integer> tokenMap2);

        /**
         * Builds the token object
         * @param input the original sequence input, may be a simple concatenation of a sequence pair
         * @param truncated Was this truncated when tokenized
         * @param allTokens All the tokens with their values and offsets
         * @param spanPrev how many tokens from the previous subsequence are in this one. Only relevant when windowing
         * @param seqId the sequence id, unique per tokenized sequence, useful for windowing
         * @return A new Tokens object
         */
        Tokens build(String input, boolean truncated, List<? extends DelimitedToken> allTokens, int spanPrev, int seqId);
    }
}
