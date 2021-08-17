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
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizer;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.TokenizationResult;

import java.io.IOException;
import java.util.Arrays;

public class DistilBertRequestBuilder implements NlpTask.RequestBuilder {

    static final String REQUEST_ID = "request_id";
    static final String TOKENS = "tokens";
    static final String ARG1 = "arg_1";

    private final BertTokenizer tokenizer;
    private final NlpTask.ResultProcessorFactory resultProcessorFactory;

    public DistilBertRequestBuilder(BertTokenizer tokenizer, NlpTask.ResultProcessorFactory resultProcessorFactory) {
        this.tokenizer = tokenizer;
        this.resultProcessorFactory = resultProcessorFactory;
    }

    @Override
    public Tuple<BytesReference, NlpTask.ResultProcessor> buildRequest(String input, String requestId) throws IOException {
        TokenizationResult result = tokenizer.tokenize(input);
        return Tuple.tuple(jsonRequest(result.getTokenIds(), requestId), resultProcessorFactory.build(result));
    }

    static BytesReference jsonRequest(int[] tokens, String requestId) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.field(REQUEST_ID, requestId);
        builder.array(TOKENS, tokens);

        int[] inputMask = new int[tokens.length];
        Arrays.fill(inputMask, 1);

        builder.array(ARG1, inputMask);
        builder.endObject();

        // BytesReference.bytes closes the builder
        return BytesReference.bytes(builder);
    }
}
