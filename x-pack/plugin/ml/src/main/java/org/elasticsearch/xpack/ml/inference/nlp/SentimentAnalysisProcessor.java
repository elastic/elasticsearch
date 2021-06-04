/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.xpack.core.ml.inference.deployment.PyTorchResult;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.SentimentAnalysisResults;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizer;

public class SentimentAnalysisProcessor implements NlpTask.Processor {

    private final BertRequestBuilder bertRequestBuilder;

    SentimentAnalysisProcessor(BertTokenizer tokenizer) {
        this.bertRequestBuilder = new BertRequestBuilder(tokenizer);
    }
    @Override
    public void validateInputs(String inputs) {
        // nothing to validate
    }

    @Override
    public NlpTask.RequestBuilder getRequestBuilder() {
        return bertRequestBuilder;
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
        return new SentimentAnalysisResults(normalizedScores[0], normalizedScores[1]);
    }
}
