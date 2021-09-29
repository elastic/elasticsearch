/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.xpack.core.ml.inference.results.FillMaskResults;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.TopClassEntry;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.FillMaskConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig;
import org.elasticsearch.xpack.ml.inference.deployment.PyTorchResult;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizer;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.NlpTokenizer;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.TokenizationResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig.DEFAULT_RESULTS_FIELD;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig.DEFAULT_TOP_CLASSES_RESULTS_FIELD;

public class FillMaskProcessor implements NlpTask.Processor {

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
    public NlpTask.RequestBuilder getRequestBuilder(NlpConfig config) {
        return requestBuilder;
    }

    @Override
    public NlpTask.ResultProcessor getResultProcessor(NlpConfig config) {
        if (config instanceof FillMaskConfig) {
            FillMaskConfig fillMaskConfig = (FillMaskConfig) config;
            return (tokenization, result) -> processResult(
                tokenization,
                result,
                fillMaskConfig.getNumTopClasses(),
                fillMaskConfig.getResultsField()
            );
        } else {
            return (tokenization, result) -> processResult(tokenization, result, FillMaskConfig.DEFAULT_NUM_RESULTS, DEFAULT_RESULTS_FIELD);
        }
    }

    static InferenceResults processResult(
        TokenizationResult tokenization,
        PyTorchResult pyTorchResult,
        int numResults,
        String resultsField
    ) {
        if (tokenization.getTokenizations().isEmpty() ||
            tokenization.getTokenizations().get(0).getTokens().length == 0) {
            return new WarningInferenceResults("No valid tokens for inference");
        }

        int maskTokenIndex = Arrays.asList(tokenization.getTokenizations().get(0).getTokens()).indexOf(BertTokenizer.MASK_TOKEN);
        // TODO - process all results in the batch
        double[] normalizedScores = NlpHelpers.convertToProbabilitiesBySoftMax(pyTorchResult.getInferenceResult()[0][maskTokenIndex]);

        NlpHelpers.ScoreAndIndex[] scoreAndIndices = NlpHelpers.topK(
            // We need at least one to record the result
            numResults == -1 ? Integer.MAX_VALUE : Math.max(numResults, 1),
            normalizedScores
        );
        List<TopClassEntry> results = new ArrayList<>(scoreAndIndices.length);
        if (numResults != 0) {
            for (NlpHelpers.ScoreAndIndex scoreAndIndex : scoreAndIndices) {
                String predictedToken = tokenization.getFromVocab(scoreAndIndex.index);
                results.add(new TopClassEntry(predictedToken, scoreAndIndex.score, scoreAndIndex.score));
            }
        }
        return new FillMaskResults(
            scoreAndIndices[0].index,
            tokenization.getFromVocab(scoreAndIndices[0].index),
            tokenization.getTokenizations().get(0).getInput().replace(
                BertTokenizer.MASK_TOKEN,
                tokenization.getFromVocab(scoreAndIndices[0].index)
            ),
            results,
            DEFAULT_TOP_CLASSES_RESULTS_FIELD,
            Optional.ofNullable(resultsField).orElse(DEFAULT_RESULTS_FIELD),
            scoreAndIndices[0].score
        );
    }
}
