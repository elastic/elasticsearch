/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.NlpClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.TopClassEntry;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.Tokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ZeroShotClassificationConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.inference.deployment.PyTorchResult;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.NlpTokenizer;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.TokenizationResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig.DEFAULT_RESULTS_FIELD;

public class ZeroShotClassificationProcessor implements NlpTask.Processor {

    private final NlpTokenizer tokenizer;
    private final int entailmentPos;
    private final int contraPos;
    private final String[] labels;
    private final String hypothesisTemplate;
    private final boolean isMultiLabel;
    private final String resultsField;

    ZeroShotClassificationProcessor(NlpTokenizer tokenizer, ZeroShotClassificationConfig config) {
        this.tokenizer = tokenizer;
        List<String> lowerCased = config.getClassificationLabels()
            .stream()
            .map(s -> s.toLowerCase(Locale.ROOT))
            .collect(Collectors.toList());
        this.entailmentPos = lowerCased.indexOf("entailment");
        this.contraPos = lowerCased.indexOf("contradiction");
        if (entailmentPos == -1 || contraPos == -1) {
            throw ExceptionsHelper.badRequestException(
                "zero_shot_classification requires [entailment] and [contradiction] in classification_labels"
            );
        }
        this.labels = Optional.ofNullable(config.getLabels()).orElse(List.of()).toArray(String[]::new);
        this.hypothesisTemplate = config.getHypothesisTemplate();
        this.isMultiLabel = config.isMultiLabel();
        this.resultsField = config.getResultsField();
    }

    @Override
    public void validateInputs(List<String> inputs) {
        // nothing to validate
    }

    @Override
    public NlpTask.RequestBuilder getRequestBuilder(NlpConfig nlpConfig) {
        final String[] labelsValue;
        if (nlpConfig instanceof ZeroShotClassificationConfig) {
            ZeroShotClassificationConfig zeroShotConfig = (ZeroShotClassificationConfig) nlpConfig;
            labelsValue = zeroShotConfig.getLabels().toArray(new String[0]);
        } else {
            labelsValue = this.labels;
        }
        if (labelsValue == null || labelsValue.length == 0) {
            throw ExceptionsHelper.badRequestException("zero_shot_classification requires non-empty [labels]");
        }
        return new RequestBuilder(tokenizer, labelsValue, hypothesisTemplate);
    }

    @Override
    public NlpTask.ResultProcessor getResultProcessor(NlpConfig nlpConfig) {
        final String[] labelsValue;
        final boolean isMultiLabelValue;
        final String resultsFieldValue;
        if (nlpConfig instanceof ZeroShotClassificationConfig) {
            ZeroShotClassificationConfig zeroShotConfig = (ZeroShotClassificationConfig) nlpConfig;
            labelsValue = zeroShotConfig.getLabels().toArray(new String[0]);
            isMultiLabelValue = zeroShotConfig.isMultiLabel();
            resultsFieldValue = zeroShotConfig.getResultsField();
        } else {
            labelsValue = this.labels;
            isMultiLabelValue = this.isMultiLabel;
            resultsFieldValue = this.resultsField;
        }
        return new ResultProcessor(entailmentPos, contraPos, labelsValue, isMultiLabelValue, resultsFieldValue);
    }

    static class RequestBuilder implements NlpTask.RequestBuilder {

        private final NlpTokenizer tokenizer;
        private final String[] labels;
        private final String hypothesisTemplate;

        RequestBuilder(NlpTokenizer tokenizer, String[] labels, String hypothesisTemplate) {
            this.tokenizer = tokenizer;
            this.labels = labels;
            this.hypothesisTemplate = hypothesisTemplate;
        }

        @Override
        public NlpTask.Request buildRequest(List<String> inputs, String requestId, Tokenization.Truncate truncate) throws IOException {
            if (inputs.size() > 1) {
                throw new IllegalArgumentException("Unable to do zero-shot classification on more than one text input at a time");
            }
            List<TokenizationResult.Tokenization> tokenizations = new ArrayList<>(labels.length);
            for (String label : labels) {
                tokenizations.add(tokenizer.tokenize(inputs.get(0), LoggerMessageFormat.format(null, hypothesisTemplate, label), truncate));
            }
            TokenizationResult result = tokenizer.buildTokenizationResult(tokenizations);
            return buildRequest(result, requestId);
        }

        @Override
        public NlpTask.Request buildRequest(TokenizationResult tokenizationResult, String requestId) throws IOException {
            return tokenizer.requestBuilder().buildRequest(tokenizationResult, requestId);
        }
    }

    static class ResultProcessor implements NlpTask.ResultProcessor {
        private final int entailmentPos;
        private final int contraPos;
        private final String[] labels;
        private final boolean isMultiLabel;
        private final String resultsField;

        ResultProcessor(int entailmentPos, int contraPos, String[] labels, boolean isMultiLabel, String resultsField) {
            this.entailmentPos = entailmentPos;
            this.contraPos = contraPos;
            this.labels = labels;
            this.isMultiLabel = isMultiLabel;
            this.resultsField = resultsField;
        }

        @Override
        public InferenceResults processResult(TokenizationResult tokenization, PyTorchResult pyTorchResult) {
            if (pyTorchResult.getInferenceResult().length < 1) {
                return new WarningInferenceResults("Zero shot classification result has no data");
            }
            // TODO only the first entry in the batch result is verified and
            // checked. Implement for all in batch
            if (pyTorchResult.getInferenceResult()[0].length != labels.length) {
                return new WarningInferenceResults(
                    "Expected exactly [{}] values in zero shot classification result; got [{}]",
                    labels.length,
                    pyTorchResult.getInferenceResult().length
                );
            }
            final double[] normalizedScores;
            if (isMultiLabel) {
                normalizedScores = new double[pyTorchResult.getInferenceResult()[0].length];
                int v = 0;
                for (double[] vals : pyTorchResult.getInferenceResult()[0]) {
                    if (vals.length != 3) {
                        return new WarningInferenceResults(
                            "Expected exactly [{}] values in inner zero shot classification result; got [{}]",
                            3,
                            vals.length
                        );
                    }
                    // assume entailment is `0`, softmax between entailment and contradiction
                    normalizedScores[v++] = NlpHelpers.convertToProbabilitiesBySoftMax(
                        new double[] { vals[entailmentPos], vals[contraPos] }
                    )[0];
                }
            } else {
                double[] entailmentScores = new double[pyTorchResult.getInferenceResult()[0].length];
                int v = 0;
                for (double[] vals : pyTorchResult.getInferenceResult()[0]) {
                    if (vals.length != 3) {
                        return new WarningInferenceResults(
                            "Expected exactly [{}] values in inner zero shot classification result; got [{}]",
                            3,
                            vals.length
                        );
                    }
                    entailmentScores[v++] = vals[entailmentPos];
                }
                normalizedScores = NlpHelpers.convertToProbabilitiesBySoftMax(entailmentScores);
            }
            int[] sortedIndices = IntStream.range(0, normalizedScores.length)
                .boxed()
                .sorted(Comparator.comparing(i -> normalizedScores[(Integer) i]).reversed())
                .mapToInt(i -> i)
                .toArray();

            return new NlpClassificationInferenceResults(
                labels[sortedIndices[0]],
                Arrays.stream(sortedIndices).mapToObj(i -> new TopClassEntry(labels[i], normalizedScores[i])).collect(Collectors.toList()),
                Optional.ofNullable(resultsField).orElse(DEFAULT_RESULTS_FIELD),
                normalizedScores[sortedIndices[0]],
                tokenization.anyTruncated()
            );
        }
    }
}
