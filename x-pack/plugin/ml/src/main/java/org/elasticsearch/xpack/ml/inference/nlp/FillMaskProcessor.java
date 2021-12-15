/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ml.inference.results.FillMaskResults;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.TopClassEntry;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.FillMaskConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.NlpTokenizer;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.TokenizationResult;
import org.elasticsearch.xpack.ml.inference.pytorch.results.PyTorchInferenceResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig.DEFAULT_RESULTS_FIELD;

public class FillMaskProcessor implements NlpTask.Processor {

    private final NlpTokenizer tokenizer;

    FillMaskProcessor(NlpTokenizer tokenizer, FillMaskConfig config) {
        this.tokenizer = tokenizer;
    }

    @Override
    public void validateInputs(List<String> inputs) {
        if (inputs.isEmpty()) {
            throw ExceptionsHelper.badRequestException("input request is empty");
        }

        final String mask = tokenizer.getMaskToken();
        for (String input : inputs) {
            int maskIndex = input.indexOf(mask);
            if (maskIndex < 0) {
                throw ExceptionsHelper.badRequestException("no {} token could be found", mask);
            }

            maskIndex = input.indexOf(mask, maskIndex + mask.length());
            if (maskIndex > 0) {
                throw ExceptionsHelper.badRequestException("only one {} token should exist in the input", mask);
            }
        }
    }

    @Override
    public NlpTask.RequestBuilder getRequestBuilder(NlpConfig config) {
        return tokenizer.requestBuilder();
    }

    @Override
    public NlpTask.ResultProcessor getResultProcessor(NlpConfig config) {
        if (config instanceof FillMaskConfig fillMaskConfig) {
            return (tokenization, result) -> processResult(
                tokenization,
                result,
                tokenizer,
                fillMaskConfig.getNumTopClasses(),
                fillMaskConfig.getResultsField()
            );
        } else {
            return (tokenization, result) -> processResult(
                tokenization,
                result,
                tokenizer,
                FillMaskConfig.DEFAULT_NUM_RESULTS,
                DEFAULT_RESULTS_FIELD
            );
        }
    }

    static InferenceResults processResult(
        TokenizationResult tokenization,
        PyTorchInferenceResult pyTorchResult,
        NlpTokenizer tokenizer,
        int numResults,
        String resultsField
    ) {
        if (tokenization.getTokenizations().isEmpty() || tokenization.getTokenizations().get(0).getTokenIds().length == 0) {
            throw new ElasticsearchStatusException("tokenization is empty", RestStatus.INTERNAL_SERVER_ERROR);
        }

        if (tokenizer.getMaskTokenId().isEmpty()) {
            throw ExceptionsHelper.conflictStatusException(
                "The token id for the mask token {} is not known in the tokenizer. Check the vocabulary contains the mask token",
                tokenizer.getMaskToken()
            );
        }

        int maskTokenIndex = -1;
        int maskTokenId = tokenizer.getMaskTokenId().getAsInt();
        for (int i = 0; i < tokenization.getTokenizations().get(0).getTokenIds().length; i++) {
            if (tokenization.getTokenizations().get(0).getTokenIds()[i] == maskTokenId) {
                maskTokenIndex = i;
                break;
            }
        }
        if (maskTokenIndex == -1) {
            throw new ElasticsearchStatusException(
                "mask token id [{}] not found in the tokenization {}",
                RestStatus.INTERNAL_SERVER_ERROR,
                maskTokenId,
                List.of(tokenization.getTokenizations().get(0).getTokenIds())
            );
        }

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
            tokenization.getFromVocab(scoreAndIndices[0].index),
            tokenization.getTokenizations()
                .get(0)
                .getInput()
                .replace(tokenizer.getMaskToken(), tokenization.getFromVocab(scoreAndIndices[0].index)),
            results,
            Optional.ofNullable(resultsField).orElse(DEFAULT_RESULTS_FIELD),
            scoreAndIndices[0].score,
            tokenization.anyTruncated()
        );
    }
}
