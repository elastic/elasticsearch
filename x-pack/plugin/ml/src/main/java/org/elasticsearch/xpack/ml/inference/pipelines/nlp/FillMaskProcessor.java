/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pipelines.nlp;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.xpack.ml.inference.pipelines.nlp.tokenizers.BertTokenizer;

import java.io.IOException;
import java.util.Arrays;

public class FillMaskProcessor extends NlpPipeline.Processor {

    private final BertTokenizer tokenizer;
    private BertTokenizer.TokenizationResult tokenization;

    FillMaskProcessor(BertTokenizer tokenizer) {
        this.tokenizer = tokenizer;
    }

    private BytesReference buildRequest(String requestId, String input) throws IOException {
        tokenization = tokenizer.tokenize(input, true);
        return jsonRequest(tokenization.getTokenIds(), requestId);
    }

    @Override
    public NlpPipeline.RequestBuilder getRequestBuilder() {
        return this::buildRequest;
    }

    @Override
    public NlpPipeline.ResultProcessor getResultProcessor() {
        return new FillMaskResultProcessor(tokenization);
    }

    static BytesReference jsonRequest(int[] tokens, String requestId) throws IOException {
        // TODO the request here is identical is with NER
        // refactor to reuse code when a proper name
        // can be found for a base processor
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.field(REQUEST_ID, requestId);
        builder.array(TOKENS, tokens);

        int[] inputMask = new int[tokens.length];
        Arrays.fill(inputMask, 1);
        int[] segmentMask = new int[tokens.length];
        Arrays.fill(segmentMask, 0);
        int[] positionalIds = new int[tokens.length];
        Arrays.setAll(positionalIds, i -> i);
        builder.array(ARG1, inputMask);
        builder.array(ARG2, segmentMask);
        builder.array(ARG3, positionalIds);
        builder.endObject();

        // BytesReference.bytes closes the builder
        return BytesReference.bytes(builder);
    }
}
