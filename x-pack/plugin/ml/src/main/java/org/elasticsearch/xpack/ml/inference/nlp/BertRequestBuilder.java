/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizer;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.TokenizationResult;

import java.io.IOException;
import java.util.function.BiFunction;
import java.util.function.Function;

public class BertRequestBuilder implements NlpTask.RequestBuilder {

    static final String REQUEST_ID = "request_id";
    static final String TOKENS = "tokens";
    static final String ARG1 = "arg_1";
    static final String ARG2 = "arg_2";
    static final String ARG3 = "arg_3";

    private final BertTokenizer tokenizer;

    public BertRequestBuilder(BertTokenizer tokenizer) {
        this.tokenizer = tokenizer;
    }

    @Override
    public NlpTask.Request buildRequest(String input, String requestId) throws IOException {
        TokenizationResult tokenization = tokenizer.tokenize(input);
        if (tokenization.getTokenIds().length > maxSequenceLength) {
            throw ExceptionsHelper.badRequestException(
                "Input too large. The tokenized input length [{}] exceeds the maximum sequence length [{}]",
                tokenization.getLongestSequenceLength(), maxSequenceLength);
        }

        if (tokenizer.getPadToken() == null) {
            throw new IllegalStateException("The input tokenizer does not have a " + BertTokenizer.PAD_TOKEN +
                " token in its vocabulary");
        }

        return new NlpTask.Request(tokenization, jsonRequest(tokenization, tokenizer.getPadToken(), requestId));
    }

    static BytesReference jsonRequest(BertTokenizer.Tokenization tokenization,
                                      int padToken,
                                      String requestId) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.field(REQUEST_ID, requestId);

        writePaddedTokens(TOKENS, tokenization, padToken, (tokens, i) -> tokens.getTokenIds()[i], builder);
        writePaddedTokens(ARG1, tokenization, padToken, (tokens, i) -> 1, builder);
        writeNonPaddedIds(ARG2, tokenization.getTokenizations().size(), tokenization.getLongestSequenceLength(), i -> 0, builder);
        writeNonPaddedIds(ARG3, tokenization.getTokenizations().size(), tokenization.getLongestSequenceLength(), i -> i, builder);
        builder.endObject();

        // BytesReference.bytes closes the builder
        return BytesReference.bytes(builder);
    }

    static void writePaddedTokens(String fieldName,
                                  BertTokenizer.Tokenization tokenization,
                                  int padToken,
                                  BiFunction<BertTokenizer.TokenizationResult, Integer, Integer> generator,
                                  XContentBuilder builder) throws IOException {
        builder.startArray(fieldName);
        for (var inputTokens : tokenization.getTokenizations()) {
            builder.startArray();
            int i = 0;
            for (; i < inputTokens.getTokenIds().length; i++) {
                builder.value(generator.apply(inputTokens, i));
            }

            for (; i < tokenization.getLongestSequenceLength(); i++) {
                builder.value(padToken);
            }
            builder.endArray();
        }
        builder.endArray();
    }

    static void writeNonPaddedIds(String fieldName,
                                  int numTokenizations, int longestSequenceLength,
                                  Function<Integer, Integer> generator,
                                  XContentBuilder builder) throws IOException {
        builder.startArray(fieldName);
        for (int i = 0; i < numTokenizations; i++) {
            builder.startArray();
            for (int j = 0; j < longestSequenceLength; j++) {
                builder.value(generator.apply(j));
            }
            builder.endArray();
        }
        builder.endArray();
    }
}
