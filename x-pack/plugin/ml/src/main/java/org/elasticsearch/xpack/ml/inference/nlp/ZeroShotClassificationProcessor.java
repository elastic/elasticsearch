/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ml.inference.results.NlpClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.TopClassEntry;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.Tokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ZeroShotClassificationConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.NlpTokenizer;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.TokenizationResult;
import org.elasticsearch.xpack.ml.inference.pytorch.results.PyTorchInferenceResult;

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

public class ZeroShotClassificationProcessor extends NlpTask.Processor {

    private final int entailmentPos;
    private final int contraPos;
    private final String[] labels;
    private final String hypothesisTemplate;
    private final boolean isMultiLabel;
    private final String resultsField;

    ZeroShotClassificationProcessor(NlpTokenizer tokenizer, ZeroShotClassificationConfig config) {
        super(tokenizer);
        List<String> lowerCased = config.getClassificationLabels().stream().map(s -> s.toLowerCase(Locale.ROOT)).toList();
        this.entailmentPos = lowerCased.indexOf("entailment");
        this.contraPos = lowerCased.indexOf("contradiction");
        if (entailmentPos == -1 || contraPos == -1) {
            throw ExceptionsHelper.badRequestException(
                "zero_shot_classification requires [entailment] and [contradiction] in classification_labels"
            );
        }
        this.labels = config.getLabels().orElse(List.of()).toArray(String[]::new);
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
        if (nlpConfig instanceof ZeroShotClassificationConfig zeroShotConfig) {
            labelsValue = zeroShotConfig.getLabels().orElse(List.of()).toArray(new String[0]);
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
        if (nlpConfig instanceof ZeroShotClassificationConfig zeroShotConfig) {
            labelsValue = zeroShotConfig.getLabels().orElse(List.of()).toArray(new String[0]);
            isMultiLabelValue = zeroShotConfig.isMultiLabel();
            resultsFieldValue = zeroShotConfig.getResultsField();
        } else {
            labelsValue = this.labels;
            isMultiLabelValue = this.isMultiLabel;
            resultsFieldValue = this.resultsField;
        }
        return new ResultProcessor(entailmentPos, contraPos, labelsValue, isMultiLabelValue, resultsFieldValue);
    }

    record RequestBuilder(NlpTokenizer tokenizer, String[] labels, String hypothesisTemplate) implements NlpTask.RequestBuilder {

        @Override
        public NlpTask.Request buildRequest(
            List<String> inputs,
            String requestId,
            Tokenization.Truncate truncate,
            int span,
            Integer windowSize
        ) throws IOException {
            if (inputs.size() > 1) {
                throw ExceptionsHelper.badRequestException("Unable to do zero-shot classification on more than one text input at a time");
            }
            if (span > -1) {
                throw ExceptionsHelper.badRequestException("Unable to span zero-shot classification on long text input");
            }
            List<TokenizationResult.Tokens> tokenizations = new ArrayList<>(labels.length);
            int seqId = 0;
            NlpTokenizer.InnerTokenization firstSequenceTokenization = tokenizer.innerTokenize(inputs.get(0));
            for (String label : labels) {
                tokenizations.add(
                    tokenizer.tokenize(
                        inputs.get(0),
                        firstSequenceTokenization,
                        LoggerMessageFormat.format(null, hypothesisTemplate, label),
                        truncate,
                        seqId++
                    )
                );
            }
            TokenizationResult result = tokenizer.buildTokenizationResult(tokenizations);
            return result.buildRequest(requestId, truncate);
        }
    }

    record ResultProcessor(int entailmentPos, int contraPos, String[] labels, boolean isMultiLabel, String resultsField)
        implements
            NlpTask.ResultProcessor {

        @Override
        public InferenceResults processResult(TokenizationResult tokenization, PyTorchInferenceResult pyTorchResult, boolean chunkResult) {
            if (chunkResult) {
                throw chunkingNotSupportedException(TaskType.NER);
            }

            if (pyTorchResult.getInferenceResult().length < 1) {
                throw new ElasticsearchStatusException("Zero shot classification result has no data", RestStatus.INTERNAL_SERVER_ERROR);
            }
            // TODO only the first entry in the batch result is verified and
            // checked. Implement for all in batch
            if (pyTorchResult.getInferenceResult()[0].length != labels.length) {
                throw new ElasticsearchStatusException(
                    "Expected exactly [{}] values in zero shot classification result; got [{}]",
                    RestStatus.CONFLICT,
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
                        throw new ElasticsearchStatusException(
                            "Expected exactly [{}] values in inner zero shot classification result; got [{}]",
                            RestStatus.CONFLICT,
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
                        throw new ElasticsearchStatusException(
                            "Expected exactly [{}] values in inner zero shot classification result; got [{}]",
                            RestStatus.CONFLICT,
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
