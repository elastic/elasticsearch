/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import java.util.List;

import static org.elasticsearch.xpack.ml.inference.nlp.tokenizers.UnigramTokenizer.PREFIX;

public class XLMRobertaTokenizationResult extends RobertaTokenizationResult {

    protected XLMRobertaTokenizationResult(List<String> vocab, List<Tokens> tokenizations, int padTokenId) {
        super(vocab, tokenizations, padTokenId);
    }

    @Override
    public String decode(String token) {
        if (token.startsWith(PREFIX)) {
            return token.substring(PREFIX.length());
        }
        return token;
    }

    static class XLMRobertaTokensBuilder extends RobertaTokensBuilder {
        XLMRobertaTokensBuilder(boolean withSpecialTokens, int clsTokenId, int sepTokenId) {
            super(withSpecialTokens, clsTokenId, sepTokenId);
        }
    }
}
