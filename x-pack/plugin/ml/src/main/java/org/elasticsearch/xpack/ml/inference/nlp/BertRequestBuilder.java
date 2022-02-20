/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.Tokenization;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.NlpTokenizer;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.TokenizationResult;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class BertRequestBuilder implements NlpTask.RequestBuilder {

    static final String REQUEST_ID = "request_id";
    static final String TOKENS = "tokens";
    static final String ARG1 = "arg_1";
    static final String ARG2 = "arg_2";
    static final String ARG3 = "arg_3";

    private final NlpTokenizer tokenizer;

    public BertRequestBuilder(NlpTokenizer tokenizer) {
        this.tokenizer = tokenizer;
    }

    @Override
    public NlpTask.Request buildRequest(List<String> inputs, String requestId, Tokenization.Truncate truncate) throws IOException {
        if (tokenizer.getPadTokenId().isEmpty()) {
            throw new IllegalStateException("The input tokenizer does not have a " + tokenizer.getPadToken() + " token in its vocabulary");
        }

        TokenizationResult tokenization = tokenizer.buildTokenizationResult(
            inputs.stream().map(s -> tokenizer.tokenize(s, truncate)).collect(Collectors.toList())
        );
        return buildRequest(tokenization, requestId);
    }

    @Override
    public NlpTask.Request buildRequest(TokenizationResult tokenization, String requestId) throws IOException {
        if (tokenizer.getPadTokenId().isEmpty()) {
            throw new IllegalStateException("The input tokenizer does not have a " + tokenizer.getPadToken() + " token in its vocabulary");
        }
        return new NlpTask.Request(tokenization, jsonRequest(tokenization, tokenizer.getPadTokenId().getAsInt(), requestId));
    }

    static BytesReference jsonRequest(TokenizationResult tokenization, int padToken, String requestId) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.field(REQUEST_ID, requestId);

        NlpTask.RequestBuilder.writePaddedTokens(TOKENS, tokenization, padToken, (tokens, i) -> tokens.getTokenIds()[i], builder);
        NlpTask.RequestBuilder.writePaddedTokens(ARG1, tokenization, padToken, (tokens, i) -> 1, builder);
        int batchSize = tokenization.getTokenizations().size();
        NlpTask.RequestBuilder.writeNonPaddedArguments(ARG2, batchSize, tokenization.getLongestSequenceLength(), i -> 0, builder);
        NlpTask.RequestBuilder.writeNonPaddedArguments(ARG3, batchSize, tokenization.getLongestSequenceLength(), i -> i, builder);
        builder.endObject();

        // BytesReference.bytes closes the builder
        return BytesReference.bytes(builder);
    }

}
