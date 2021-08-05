/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.xpack.core.ml.inference.results.FillMaskResults;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.FillMaskConfig;
import org.elasticsearch.xpack.ml.inference.deployment.PyTorchResult;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizer;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.NlpTokenizer;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.TokenizationResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FillMaskProcessor implements NlpTask.Processor {

    private static final int NUM_RESULTS = 5;

    private final NlpTask.RequestBuilder requestBuilder;

    FillMaskProcessor(NlpTokenizer tokenizer, FillMaskConfig config) {
        this.requestBuilder = tokenizer.requestBuilder();
    }

    @Override
    public void validateInputs(List<String> inputs) {
        if (inputs.isEmpty()) {
            throw new IllegalArgumentException("input request is empty");
        }

        for (String input : inputs) {
            int maskIndex = input.indexOf(BertTokenizer.MASK_TOKEN);
            if (maskIndex < 0) {
                throw new IllegalArgumentException("no " + BertTokenizer.MASK_TOKEN + " token could be found");
            }

            maskIndex = input.indexOf(BertTokenizer.MASK_TOKEN, maskIndex + BertTokenizer.MASK_TOKEN.length());
            if (maskIndex > 0) {
                throw new IllegalArgumentException("only one " + BertTokenizer.MASK_TOKEN + " token should exist in the input");
            }
        }
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

        if (tokenization.getTokenizations().isEmpty()) {
            return new FillMaskResults(Collections.emptyList());
        }

        int maskTokenIndex = tokenization.getTokenizations().get(0).getTokens().indexOf(BertTokenizer.MASK_TOKEN);
        double[] normalizedScores = NlpHelpers.convertToProbabilitiesBySoftMax(pyTorchResult.getInferenceResult()[0][maskTokenIndex]);

        NlpHelpers.ScoreAndIndex[] scoreAndIndices = NlpHelpers.topK(NUM_RESULTS, normalizedScores);
        List<FillMaskResults.Prediction> results = new ArrayList<>(NUM_RESULTS);
        for (NlpHelpers.ScoreAndIndex scoreAndIndex : scoreAndIndices) {
            String predictedToken = tokenization.getFromVocab(scoreAndIndex.index);
            String sequence = tokenization.getTokenizations().get(0).getInput().replace(BertTokenizer.MASK_TOKEN, predictedToken);
            results.add(new FillMaskResults.Prediction(predictedToken, scoreAndIndex.score, sequence));
        }
        return new FillMaskResults(results);
    }
}
