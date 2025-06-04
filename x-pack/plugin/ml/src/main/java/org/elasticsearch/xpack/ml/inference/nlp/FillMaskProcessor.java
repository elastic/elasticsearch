/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ml.inference.results.FillMaskResults;
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
import java.util.OptionalInt;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig.DEFAULT_RESULTS_FIELD;

public class FillMaskProcessor extends NlpTask.Processor {

    FillMaskProcessor(NlpTokenizer tokenizer) {
        super(tokenizer);
    }

    @Override
    public void validateInputs(List<String> inputs) {
        ValidationException ve = new ValidationException();
        if (inputs.isEmpty()) {
            ve.addValidationError("input request is empty");
        }

        final String mask = tokenizer.getMaskToken();
        for (String input : inputs) {
            int maskIndex = input.indexOf(mask);
            if (maskIndex < 0) {
                ve.addValidationError("no " + mask + " token could be found in the input");
            }

            maskIndex = input.indexOf(mask, maskIndex + mask.length());
            if (maskIndex > 0) {
                throw ExceptionsHelper.badRequestException("only one {} token should exist in the input", mask);
            }
        }

        if (ve.validationErrors().isEmpty() == false) {
            throw ve;
        }
    }

    @Override
    public NlpTask.RequestBuilder getRequestBuilder(NlpConfig config) {
        return tokenizer.requestBuilder();
    }

    @Override
    public NlpTask.ResultProcessor getResultProcessor(NlpConfig config) {
        if (config instanceof FillMaskConfig fillMaskConfig) {
            return (tokenization, result, chunkResults) -> processResult(
                tokenization,
                result,
                tokenizer,
                fillMaskConfig.getNumTopClasses(),
                fillMaskConfig.getResultsField(),
                chunkResults
            );
        } else {
            return (tokenization, result, chunkResults) -> processResult(
                tokenization,
                result,
                tokenizer,
                FillMaskConfig.DEFAULT_NUM_RESULTS,
                DEFAULT_RESULTS_FIELD,
                chunkResults
            );
        }
    }

    static InferenceResults processResult(
        TokenizationResult tokenization,
        PyTorchInferenceResult pyTorchResult,
        NlpTokenizer tokenizer,
        int numResults,
        String resultsField,
        boolean chunkResults
    ) {
        if (tokenization.isEmpty()) {
            throw new ElasticsearchStatusException("tokenization is empty", RestStatus.INTERNAL_SERVER_ERROR);
        }
        if (chunkResults) {
            throw chunkingNotSupportedException(TaskType.NER);
        }

        if (tokenizer.getMaskTokenId().isEmpty()) {
            throw ExceptionsHelper.conflictStatusException(
                "The token id for the mask token {} is not known in the tokenizer. Check the vocabulary contains the mask token",
                tokenizer.getMaskToken()
            );
        }

        int maskTokenId = tokenizer.getMaskTokenId().getAsInt();
        OptionalInt maskTokenIndex = tokenization.getTokenization(0).getTokenIndex(maskTokenId);
        if (maskTokenIndex.isEmpty()) {
            throw new ElasticsearchStatusException(
                "mask token id [{}] not found in the tokenization",
                RestStatus.INTERNAL_SERVER_ERROR,
                maskTokenId
            );
        }

        // TODO - process all results in the batch
        double[] normalizedScores = NlpHelpers.convertToProbabilitiesBySoftMax(
            pyTorchResult.getInferenceResult()[0][maskTokenIndex.getAsInt()]
        );

        NlpHelpers.ScoreAndIndex[] scoreAndIndices = NlpHelpers.topK(
            // We need at least one to record the result
            numResults == -1 ? Integer.MAX_VALUE : Math.max(numResults, 1),
            normalizedScores
        );
        List<TopClassEntry> results = new ArrayList<>(scoreAndIndices.length);
        if (numResults != 0) {
            for (NlpHelpers.ScoreAndIndex scoreAndIndex : scoreAndIndices) {
                String predictedToken = tokenization.decode(tokenization.getFromVocab(scoreAndIndex.index));
                results.add(new TopClassEntry(predictedToken, scoreAndIndex.score, scoreAndIndex.score));
            }
        }
        String predictedValue = tokenization.decode(tokenization.getFromVocab(scoreAndIndices[0].index));
        return new FillMaskResults(
            predictedValue,
            tokenization.getTokenization(0).input().get(0).replace(tokenizer.getMaskToken(), predictedValue),
            results,
            Optional.ofNullable(resultsField).orElse(DEFAULT_RESULTS_FIELD),
            scoreAndIndices[0].score,
            tokenization.anyTruncated()
        );
    }
}
