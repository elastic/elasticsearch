/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.xpack.core.ml.inference.deployment.PyTorchResult;
import org.elasticsearch.xpack.core.ml.inference.results.FillMaskResults;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class FillMaskProcessor implements NlpTask.Processor {

    private static final int NUM_RESULTS = 5;

    private final BertRequestBuilder bertRequestBuilder;

    FillMaskProcessor(BertTokenizer tokenizer) {
        this.bertRequestBuilder = new BertRequestBuilder(tokenizer);
    }

    @Override
    public NlpTask.RequestBuilder getRequestBuilder() {
        return bertRequestBuilder;
    }

    @Override
    public NlpTask.ResultProcessor getResultProcessor() {
        return this::processResult;
    }

    private InferenceResults processResult(PyTorchResult pyTorchResult) {
        BertTokenizer.TokenizationResult tokenization = bertRequestBuilder.getTokenization();

        if (tokenization.getTokens().isEmpty()) {
            return new FillMaskResults(Collections.emptyList());
        }
        List<String> maskTokens = tokenization.getTokens().stream()
            .filter(BertTokenizer.MASK_TOKEN::equals)
            .collect(Collectors.toList());
        if (maskTokens.isEmpty()) {
            throw new IllegalArgumentException("no [MASK] token could be found");
        }
        if (maskTokens.size() > 1) {
            throw new IllegalArgumentException("only one [MASK] token should exist in the input");
        }
        int maskTokenIndex = tokenization.getTokens().indexOf(BertTokenizer.MASK_TOKEN);
        double[] normalizedScores = NlpHelpers.convertToProbabilitiesBySoftMax(pyTorchResult.getInferenceResult()[maskTokenIndex]);

        NlpHelpers.ScoreAndIndex[] scoreAndIndices = NlpHelpers.topK(NUM_RESULTS, normalizedScores);
        List<FillMaskResults.Result> results = new ArrayList<>(NUM_RESULTS);
        for (NlpHelpers.ScoreAndIndex scoreAndIndex : scoreAndIndices) {
            String predictedToken = tokenization.getFromVocab(scoreAndIndex.index);
            String sequence = tokenization.getInput().replace(BertTokenizer.MASK_TOKEN, predictedToken);
            results.add(new FillMaskResults.Result(predictedToken, scoreAndIndex.score, sequence));
        }
        return new FillMaskResults(results);
    }
}
