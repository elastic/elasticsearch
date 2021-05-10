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
import org.elasticsearch.xpack.core.ml.inference.deployment.PyTorchResult;
import org.elasticsearch.xpack.ml.inference.pipelines.nlp.tokenizers.BertTokenizer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class NerProcessor extends NlpPipeline.Processor {

    public enum Entity {
        NONE, MISC, PERSON, ORGANISATION, LOCATION
    }

    // Inside-Outside-Beginning (IOB) tag
    private enum IobTag {
        O(Entity.NONE),                 // Outside of a named entity
        B_MISC(Entity.MISC),            // Beginning of a miscellaneous entity right after another miscellaneous entity
        I_MISC(Entity.MISC),            // Miscellaneous entity
        B_PER(Entity.PERSON),           // Beginning of a person's name right after another person's name
        I_PER(Entity.PERSON),           // Person's name
        B_ORG(Entity.ORGANISATION),     // Beginning of an organisation right after another organisation
        I_ORG(Entity.ORGANISATION),     // Organisation
        B_LOC(Entity.LOCATION),         // Beginning of a location right after another location
        I_LOC(Entity.LOCATION);         // Location

        private final Entity entity;

        IobTag(Entity entity) {
            this.entity = entity;
        }

        Entity getEntity() {
            return entity;
        }
    }


    private final BertTokenizer tokenizer;
    private BertTokenizer.TokenizationResult tokenization;

    NerProcessor(BertTokenizer tokenizer) {
        this.tokenizer = tokenizer;
    }

    private NerResult processResult(PyTorchResult pyTorchResult) {
        List<IobTag> tags = toClassification(pyTorchResult);
        return labelInputs(tags, tokenization);
    }

    private BytesReference buildRequest(String requestId, String inputs) throws IOException {
        tokenization = tokenizer.tokenize(inputs, true);
        return jsonRequest(tokenization.getTokenIds(), requestId);
    }

    @Override
    public NlpPipeline.RequestBuilder getRequestBuilder() {
        return this::buildRequest;
    }

    @Override
    public NlpPipeline.ResultProcessor getResultProcessor() {
        return this::processResult;
    }

    private NerResult labelInputs(List<IobTag> tags, BertTokenizer.TokenizationResult tokenization) {
        // TODO. The subword tokenisation means that words may have been split.
        // Split tokens need to be reassembled into the original text.
        // Each IOB tag maps to a split token, where a single word has
        // more than one token we need a method to resolve a single IOB
        // token type for the word. There are numerous ways to do this:
        // pick the first token, take the most common token or take the
        // IOB token with the highest probability.
        // For example Elasticsearch maybe split Elastic and ##search with
        // the tokens I-ORG and O
        return null;
    }

    private List<IobTag> toClassification(PyTorchResult pyTorchResult) {
        List<IobTag> tags = new ArrayList<>(pyTorchResult.getInferenceResult().length);
        IobTag[] enumMap = IobTag.values();
        for (double[] arr : pyTorchResult.getInferenceResult()) {
            int ordinalValue = argmax(arr);
            assert ordinalValue < IobTag.values().length;
            tags.add(enumMap[argmax(arr)]);
        }
        return tags;
    }

    private static int argmax(double[] arr) {
        int greatest = 0;
        for (int i=1; i<arr.length; i++) {
            if (arr[i] > arr[greatest]) {
                greatest = i;
            }
        }
        return greatest;
    }

    static BytesReference jsonRequest(int[] tokens, String requestId) throws IOException {
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
