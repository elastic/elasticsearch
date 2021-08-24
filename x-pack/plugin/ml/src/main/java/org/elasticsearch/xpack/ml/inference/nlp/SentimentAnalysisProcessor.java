/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.SentimentAnalysisResults;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.SentimentAnalysisConfig;
import org.elasticsearch.xpack.ml.inference.deployment.PyTorchResult;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.NlpTokenizer;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.TokenizationResult;

import java.util.List;
import java.util.Locale;

public class SentimentAnalysisProcessor implements NlpTask.Processor {

    private final NlpTask.RequestBuilder requestBuilder;
    private final List<String> classLabels;

    SentimentAnalysisProcessor(NlpTokenizer tokenizer, SentimentAnalysisConfig config) {
        this.requestBuilder = tokenizer.requestBuilder();
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
    public void validateInputs(List<String> inputs) {
        // nothing to validate
    }

    @Override
    public NlpTask.RequestBuilder getRequestBuilder() {
        return requestBuilder;
    }

    @Override
    public NlpTask.ResultProcessor getResultProcessor() {
        return this::processResult;
    }

    InferenceResults processResult(TokenizationResult tokenization, PyTorchResult pyTorchResult) {
        if (pyTorchResult.getInferenceResult().length < 1) {
            return new WarningInferenceResults("Sentiment analysis result has no data");
        }

        if (pyTorchResult.getInferenceResult()[0].length < 2) {
            return new WarningInferenceResults("Expected 2 values in sentiment analysis result");
        }

        double[] normalizedScores = NlpHelpers.convertToProbabilitiesBySoftMax(pyTorchResult.getInferenceResult()[0][0]);
        // the second score is usually the positive score so put that first
        // so it comes first in the results doc
        return new SentimentAnalysisResults(classLabels.get(1), normalizedScores[1],
            classLabels.get(0), normalizedScores[0]);
    }
}
