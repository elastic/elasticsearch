/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pipelines.nlp;

import org.elasticsearch.xpack.core.ml.inference.deployment.PyTorchResult;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.ml.inference.pipelines.nlp.tokenizers.BertTokenizer;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

class FillMaskResultProcessor implements NlpPipeline.ResultProcessor {

    private final BertTokenizer.TokenizationResult tokenization;

    FillMaskResultProcessor(BertTokenizer.TokenizationResult tokenization) {
        this.tokenization = Objects.requireNonNull(tokenization);
    }

    @Override
    public InferenceResults processResult(PyTorchResult pyTorchResult) {
        if (tokenization.getTokens().isEmpty()) {
            return new FillMaskResult(Collections.emptyList());
        }
        List<String> maskTokens = tokenization.getTokens().stream()
            .filter(t -> BertTokenizer.MASK_TOKEN.equals(t))
            .collect(Collectors.toList());
        if (maskTokens.isEmpty()) {
            throw new IllegalArgumentException("no [MASK] token could be found");
        }
        if (maskTokens.size() > 1) {
            throw new IllegalArgumentException("only one [MASK] token should exist in the input");
        }
        int maskTokenIndex = tokenization.getTokens().indexOf(BertTokenizer.MASK_TOKEN);
        double[][] normalizedScores = NlpHelpers.convertToProbabilitesBySoftMax(pyTorchResult.getInferenceResult());
        int predictionTokenId = NlpHelpers.argmax(normalizedScores[maskTokenIndex]);
        String predictedToken = tokenization.getFromVocab(predictionTokenId);
        double score = normalizedScores[maskTokenIndex][predictionTokenId];
        String sequence = tokenization.getInput().replace(BertTokenizer.MASK_TOKEN, predictedToken);
        FillMaskResult.Result result = new FillMaskResult.Result(predictedToken, score, sequence);
        return new FillMaskResult(Collections.singletonList(result));
    }
}
