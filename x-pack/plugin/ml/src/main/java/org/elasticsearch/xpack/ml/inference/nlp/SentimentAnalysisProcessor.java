/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.SentimentAnalysisResults;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.SentimentAnalysisConfig;
import org.elasticsearch.xpack.ml.inference.deployment.PyTorchResult;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class SentimentAnalysisProcessor implements NlpTask.Processor {

    private final BertTokenizer tokenizer;
    private final List<String> classLabels;

    SentimentAnalysisProcessor(BertTokenizer tokenizer, SentimentAnalysisConfig config) {
        this.tokenizer = tokenizer;
        List<String> classLabels = config.getClassificationLabels();
        if (classLabels == null || classLabels.isEmpty()) {
            this.classLabels = List.of("negative", "positive");
        } else {
            this.classLabels = classLabels;
        }

        validate();
    }

    private void validate() {
        if (classLabels.size() != 2) {
            throw new ValidationException().addValidationError(
                String.format(Locale.ROOT, "Sentiment analysis requires exactly 2 [%s]. Invalid labels %s",
                    SentimentAnalysisConfig.CLASSIFICATION_LABELS, classLabels)
            );
        }
    }

    @Override
    public void validateInputs(String inputs) {
        // nothing to validate
    }

    @Override
    public NlpTask.RequestBuilder getRequestBuilder() {
        return this::buildRequest;
    }

    BytesReference buildRequest(String input, String requestId) throws IOException {
        BertTokenizer.TokenizationResult tokenization = tokenizer.tokenize(input);
        return jsonRequest(tokenization.getTokenIds(), requestId);
    }

    @Override
    public NlpTask.ResultProcessor getResultProcessor() {
        return this::processResult;
    }

    InferenceResults processResult(PyTorchResult pyTorchResult) {
        if (pyTorchResult.getInferenceResult().length < 1) {
            return new WarningInferenceResults("Sentiment analysis result has no data");
        }

        if (pyTorchResult.getInferenceResult()[0].length < 2) {
            return new WarningInferenceResults("Expected 2 values in sentiment analysis result");
        }

        double[] normalizedScores = NlpHelpers.convertToProbabilitiesBySoftMax(pyTorchResult.getInferenceResult()[0]);
        // the second score is usually the positive score so put that first
        // so it comes first in the results doc
        return new SentimentAnalysisResults(classLabels.get(1), normalizedScores[1],
            classLabels.get(0), normalizedScores[0]);
    }

    static BytesReference jsonRequest(int[] tokens, String requestId) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.field(BertRequestBuilder.REQUEST_ID, requestId);
        builder.array(BertRequestBuilder.TOKENS, tokens);

        int[] inputMask = new int[tokens.length];
        Arrays.fill(inputMask, 1);
        builder.array(BertRequestBuilder.ARG1, inputMask);
        builder.endObject();

        // BytesReference.bytes closes the builder
        return BytesReference.bytes(builder);
    }
}
