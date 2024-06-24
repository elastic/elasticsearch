/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.elasticsearch.xpack.core.ml.inference.trainedmodel.Tokenization;
import org.elasticsearch.xpack.ml.inference.nlp.NlpTask;

import java.io.IOException;
import java.util.List;

public class DebertaTokenizationResult extends TokenizationResult {

    protected DebertaTokenizationResult(List<String> vocab, List<Tokens> tokenizations, int padTokenId) {
        super(vocab, tokenizations, padTokenId);
    }

    @Override
    public NlpTask.Request buildRequest(String requestId, Tokenization.Truncate t) throws IOException {
        return null;
    }

    static class DebertaTokensBuilder implements TokenizationResult.TokensBuilder {
        private int clsTokenId;
        private int sepTokenId;
        private boolean withSpecialTokens;

        DebertaTokensBuilder(int clsTokenId, int sepTokenId, boolean withSpecialTokens) {
            this.clsTokenId = clsTokenId;
            this.sepTokenId = sepTokenId;
            this.withSpecialTokens = withSpecialTokens;
        }

        @Override
        public TokensBuilder addSequence(List<Integer> tokenIds, List<Integer> tokenMap) {
            return null; // TODO: Implement
        }

        @Override
        public TokensBuilder addSequencePair(
            List<Integer> tokenId1s,
            List<Integer> tokenMap1,
            List<Integer> tokenId2s,
            List<Integer> tokenMap2
        ) {
            return null; // TODO
        }

        @Override
        public Tokens build(
            List<String> input,
            boolean truncated,
            List<List<? extends DelimitedToken>> allTokens,
            int spanPrev,
            int seqId
        ) {
            return null; // TODO
        }

        @Override
        public Tokens build(String input, boolean truncated, List<? extends DelimitedToken> allTokens, int spanPrev, int seqId) {
            return TokensBuilder.super.build(input, truncated, allTokens, spanPrev, seqId);
        }
    }
}
