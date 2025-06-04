/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ml.inference.results.NlpClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.TopClassEntry;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceHelpers;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextClassificationConfig;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.NlpTokenizer;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.TokenizationResult;
import org.elasticsearch.xpack.ml.inference.pytorch.results.PyTorchInferenceResult;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig.DEFAULT_RESULTS_FIELD;

public class TextClassificationProcessor extends NlpTask.Processor {

    private final NlpTask.RequestBuilder requestBuilder;
    private final String[] classLabels;
    private final int numTopClasses;

    TextClassificationProcessor(NlpTokenizer tokenizer, TextClassificationConfig config) {
        super(tokenizer);
        this.requestBuilder = tokenizer.requestBuilder();
        List<String> classLabels = config.getClassificationLabels();
        this.classLabels = classLabels.toArray(String[]::new);
        // negative values are a special case of asking for ALL classes. Since we require the output size to equal the classLabel size
        // This is a nice way of setting the value
        this.numTopClasses = config.getNumTopClasses() < 0 ? this.classLabels.length : config.getNumTopClasses();
    }

    @Override
    public void validateInputs(List<String> inputs) {
        // nothing to validate
    }

    @Override
    public NlpTask.RequestBuilder getRequestBuilder(NlpConfig config) {
        return requestBuilder;
    }

    @Override
    public NlpTask.ResultProcessor getResultProcessor(NlpConfig config) {
        if (config instanceof TextClassificationConfig textClassificationConfig) {
            return (tokenization, pytorchResult, chunkResult) -> processResult(
                tokenization,
                pytorchResult,
                textClassificationConfig.getNumTopClasses() < 0
                    ? textClassificationConfig.getClassificationLabels().size()
                    : textClassificationConfig.getNumTopClasses(),
                textClassificationConfig.getClassificationLabels(),
                textClassificationConfig.getResultsField(),
                chunkResult
            );
        }
        return (tokenization, pytorchResult, chunkResult) -> processResult(
            tokenization,
            pytorchResult,
            numTopClasses,
            Arrays.asList(classLabels),
            DEFAULT_RESULTS_FIELD,
            chunkResult
        );
    }

    static InferenceResults processResult(
        TokenizationResult tokenization,
        PyTorchInferenceResult pyTorchResult,
        int numTopClasses,
        List<String> labels,
        String resultsField,
        boolean chunkResult
    ) {
        if (chunkResult) {
            throw chunkingNotSupportedException(TaskType.NER);
        }

        if (pyTorchResult.getInferenceResult().length < 1) {
            throw new ElasticsearchStatusException("Text classification result has no data", RestStatus.INTERNAL_SERVER_ERROR);
        }

        for (double[] result : pyTorchResult.getInferenceResult()[0]) {
            if (result.length != labels.size()) {
                throw new ElasticsearchStatusException(
                    "Expected exactly [{}] values in text classification result; got [{}]",
                    RestStatus.CONFLICT,
                    labels.size(),
                    result.length
                );
            }
        }
        Map<Integer, List<TokenizationResult.Tokens>> windowedSeq = tokenization.getTokensBySequenceId();
        // TODO adjust logic when batch is allowed
        if (windowedSeq.size() > 1) {
            throw new ElasticsearchStatusException("Unexpected batch input for text classification", RestStatus.CONFLICT);
        }
        double[] normalizedScores = new double[labels.size()];
        for (int i = 0; i < pyTorchResult.getInferenceResult()[0].length; i++) {
            double[] scores = NlpHelpers.convertToProbabilitiesBySoftMax(pyTorchResult.getInferenceResult()[0][i]);
            InferenceHelpers.sumDoubleArrays(normalizedScores, scores);
        }
        InferenceHelpers.divMut(normalizedScores, pyTorchResult.getInferenceResult()[0].length);

        int[] sortedIndices = IntStream.range(0, normalizedScores.length)
            .boxed()
            .sorted(Comparator.comparing(i -> normalizedScores[(Integer) i]).reversed())
            .mapToInt(i -> i)
            .toArray();

        return new NlpClassificationInferenceResults(
            labels.get(sortedIndices[0]),
            Arrays.stream(sortedIndices)
                .mapToObj(i -> new TopClassEntry(labels.get(i), normalizedScores[i]))
                .limit(numTopClasses)
                .collect(Collectors.toList()),
            Optional.ofNullable(resultsField).orElse(DEFAULT_RESULTS_FIELD),
            normalizedScores[sortedIndices[0]],
            tokenization.anyTruncated()
        );
    }
}
