/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * this file was contributed to by a Generative AI model
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.Tokenization;
import org.elasticsearch.xpack.ml.inference.nlp.NlpTask;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class DebertaTokenizationResult extends TokenizationResult {
    static final String REQUEST_ID = "request_id";
    static final String TOKENS = "tokens";
    static final String ARG1 = "arg_1";
    static final String ARG2 = "arg_2";

    private static final Logger logger = LogManager.getLogger(DebertaTokenizationResult.class);

    protected DebertaTokenizationResult(List<String> vocab, List<Tokens> tokenizations, int padTokenId) {
        super(vocab, tokenizations, padTokenId);
    }

    @Override
    public NlpTask.Request buildRequest(String requestId, Tokenization.Truncate t) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.field(REQUEST_ID, requestId);
        writePaddedTokens(TOKENS, builder);
        writeAttentionMask(ARG1, builder);
        writeTokenTypeIds(ARG2, builder);
        builder.endObject();

        // BytesReference.bytes closes the builder
        BytesReference jsonRequest = BytesReference.bytes(builder);
        return new NlpTask.Request(this, jsonRequest);
    }

    static class DebertaTokensBuilder implements TokenizationResult.TokensBuilder {
        private final int clsTokenId;
        private final int sepTokenId;
        private final boolean withSpecialTokens;
        protected final Stream.Builder<IntStream> tokenIds;
        protected final Stream.Builder<IntStream> tokenMap;
        protected int seqPairOffset = 0;

        DebertaTokensBuilder(int clsTokenId, int sepTokenId, boolean withSpecialTokens) {
            this.clsTokenId = clsTokenId;
            this.sepTokenId = sepTokenId;
            this.withSpecialTokens = withSpecialTokens;
            this.tokenIds = Stream.builder();
            this.tokenMap = Stream.builder();
        }

        @Override
        public TokensBuilder addSequence(List<Integer> tokenIds, List<Integer> tokenMap) {
            // DeBERTa-v2 single sequence: [CLS] X [SEP]
            if (withSpecialTokens) {
                this.tokenIds.add(IntStream.of(clsTokenId));
                this.tokenMap.add(IntStream.of(SPECIAL_TOKEN_POSITION));
            }
            this.tokenIds.add(tokenIds.stream().mapToInt(Integer::valueOf));
            this.tokenMap.add(tokenMap.stream().mapToInt(Integer::valueOf));
            if (withSpecialTokens) {
                this.tokenIds.add(IntStream.of(sepTokenId));
                this.tokenMap.add(IntStream.of(SPECIAL_TOKEN_POSITION));
            }
            return this;
        }

        @Override
        public TokensBuilder addSequencePair(
            List<Integer> tokenId1s,
            List<Integer> tokenMap1,
            List<Integer> tokenId2s,
            List<Integer> tokenMap2
        ) {
            if (tokenId1s.isEmpty() || tokenId2s.isEmpty()) {
                throw new IllegalArgumentException("Both sequences must have at least one token");
            }

            // DeBERTa-v2 pair of sequences: [CLS] A [SEP] B [SEP]
            if (withSpecialTokens) {
                tokenIds.add(IntStream.of(clsTokenId));
                tokenMap.add(IntStream.of(SPECIAL_TOKEN_POSITION));
            }
            tokenIds.add(tokenId1s.stream().mapToInt(Integer::valueOf));
            tokenMap.add(tokenMap1.stream().mapToInt(Integer::valueOf));
            int previouslyFinalMap = tokenMap1.get(tokenMap1.size() - 1);
            if (withSpecialTokens) {
                tokenIds.add(IntStream.of(sepTokenId));
                tokenMap.add(IntStream.of(SPECIAL_TOKEN_POSITION));
            }
            tokenIds.add(tokenId2s.stream().mapToInt(Integer::valueOf));
            tokenMap.add(tokenMap2.stream().mapToInt(i -> i + previouslyFinalMap));
            if (withSpecialTokens) {
                tokenIds.add(IntStream.of(sepTokenId));
                tokenMap.add(IntStream.of(SPECIAL_TOKEN_POSITION));
            }
            seqPairOffset = withSpecialTokens ? tokenId1s.size() + 2 : tokenId1s.size();
            return this;
        }

        @Override
        public Tokens build(
            List<String> input,
            boolean truncated,
            List<List<? extends DelimitedToken>> allTokens,
            int spanPrev,
            int seqId
        ) {
            return new Tokens(
                input,
                allTokens,
                truncated,
                tokenIds.build().flatMapToInt(Function.identity()).toArray(),
                tokenMap.build().flatMapToInt(Function.identity()).toArray(),
                spanPrev,
                seqId,
                seqPairOffset
            );
        }

        @Override
        public Tokens build(String input, boolean truncated, List<? extends DelimitedToken> allTokens, int spanPrev, int seqId) {
            return TokensBuilder.super.build(input, truncated, allTokens, spanPrev, seqId);
        }
    }
}
