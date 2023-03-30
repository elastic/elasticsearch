/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.Tokenization;
import org.elasticsearch.xpack.ml.inference.nlp.NlpTask;

import java.io.IOException;
import java.util.List;
import java.util.stream.IntStream;

public class MPNetTokenizationResult extends TokenizationResult {

    static final String REQUEST_ID = "request_id";
    static final String TOKENS = "tokens";
    static final String ARG1 = "arg_1";

    public MPNetTokenizationResult(List<String> vocab, List<Tokens> tokenizations, int padTokenId) {
        super(vocab, tokenizations, padTokenId);
    }

    @Override
    public NlpTask.Request buildRequest(String requestId, Tokenization.Truncate t) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.field(REQUEST_ID, requestId);
        writePaddedTokens(TOKENS, builder);
        writeAttentionMask(ARG1, builder);
        builder.endObject();

        // BytesReference.bytes closes the builder
        BytesReference jsonRequest = BytesReference.bytes(builder);
        return new NlpTask.Request(this, jsonRequest);
    }

    static class MPNetTokensBuilder extends BertTokenizationResult.BertTokensBuilder {

        MPNetTokensBuilder(boolean withSpecialTokens, int clsTokenId, int sepTokenId) {
            super(withSpecialTokens, clsTokenId, sepTokenId);
        }

        @Override
        public TokensBuilder addSequencePair(
            List<Integer> tokenId1s,
            List<Integer> tokenMap1,
            List<Integer> tokenId2s,
            List<Integer> tokenMap2
        ) {
            if (withSpecialTokens) {
                tokenIds.add(IntStream.of(clsTokenId));
                tokenMap.add(IntStream.of(SPECIAL_TOKEN_POSITION));
            }
            tokenIds.add(tokenId1s.stream().mapToInt(Integer::valueOf));
            tokenMap.add(tokenMap1.stream().mapToInt(Integer::valueOf));
            int previouslyFinalMap = tokenMap1.get(tokenMap1.size() - 1);
            // MPNet adds two `</s>` betwee sequence pairs
            if (withSpecialTokens) {
                tokenIds.add(IntStream.of(sepTokenId, sepTokenId));
                tokenMap.add(IntStream.of(SPECIAL_TOKEN_POSITION, SPECIAL_TOKEN_POSITION));
            }
            seqPairOffset = withSpecialTokens ? tokenId1s.size() + 3 : tokenId1s.size();
            tokenIds.add(tokenId2s.stream().mapToInt(Integer::valueOf));
            tokenMap.add(tokenMap2.stream().mapToInt(i -> i + previouslyFinalMap));
            if (withSpecialTokens) {
                tokenIds.add(IntStream.of(sepTokenId));
                tokenMap.add(IntStream.of(SPECIAL_TOKEN_POSITION));
            }
            return this;
        }
    }
}
