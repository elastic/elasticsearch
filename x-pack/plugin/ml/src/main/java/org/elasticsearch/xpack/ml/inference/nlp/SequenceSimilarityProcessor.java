/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.SequenceSimilarityInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.SequenceSimilarityConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.Tokenization;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.NlpTokenizer;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.TokenizationResult;
import org.elasticsearch.xpack.ml.inference.pytorch.results.PyTorchInferenceResult;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig.DEFAULT_RESULTS_FIELD;

public class SequenceSimilarityProcessor extends NlpTask.Processor {

    SequenceSimilarityProcessor(NlpTokenizer tokenizer, SequenceSimilarityConfig config) {
        super(tokenizer);
    }

    @Override
    public void validateInputs(List<String> inputs) {
        // nothing to validate
    }

    @Override
    public NlpTask.RequestBuilder getRequestBuilder(NlpConfig nlpConfig) {
        if (nlpConfig instanceof SequenceSimilarityConfig sequenceSimilarityConfig) {
            return new RequestBuilder(tokenizer, sequenceSimilarityConfig.getSequence());
        }
        throw ExceptionsHelper.badRequestException(
            "please provide configuration update for sequence_similarity task including the desired [sequence]"
        );
    }

    @Override
    public NlpTask.ResultProcessor getResultProcessor(NlpConfig nlpConfig) {
        if (nlpConfig instanceof SequenceSimilarityConfig sequenceSimilarityConfig) {
            String resultsFieldValue = sequenceSimilarityConfig.getResultsField();
            return new ResultProcessor(
                sequenceSimilarityConfig.getSequence(),
                resultsFieldValue,
                sequenceSimilarityConfig.getSpanScoreFunction()
            );
        }
        throw ExceptionsHelper.badRequestException(
            "please provide configuration update for sequence_similarity task including the desired [sequence]"
        );
    }

    record RequestBuilder(NlpTokenizer tokenizer, String sequence) implements NlpTask.RequestBuilder {

        @Override
        public NlpTask.Request buildRequest(List<String> inputs, String requestId, Tokenization.Truncate truncate, int span)
            throws IOException {
            if (inputs.size() > 1) {
                throw ExceptionsHelper.badRequestException("Unable to do sequence_similarity on more than one text input at a time");
            }
            String context = inputs.get(0);
            List<TokenizationResult.Tokens> tokenizations = tokenizer.tokenize(sequence, context, truncate, span, 0);
            TokenizationResult result = tokenizer.buildTokenizationResult(tokenizations);
            return result.buildRequest(requestId, truncate);
        }
    }

    record ResultProcessor(String question, String resultsField, SequenceSimilarityConfig.SpanScoreFunction function)
        implements
            NlpTask.ResultProcessor {

        @Override
        public InferenceResults processResult(TokenizationResult tokenization, PyTorchInferenceResult pyTorchResult) {
            if (pyTorchResult.getInferenceResult().length < 1) {
                throw new ElasticsearchStatusException("sequence_similarity result has no data", RestStatus.INTERNAL_SERVER_ERROR);
            }
            SpanScoreFunction spanScoreFunction = fromConfig(function);
            for (int i = 0; i < pyTorchResult.getInferenceResult()[0].length; i++) {
                double[] result = pyTorchResult.getInferenceResult()[0][i];
                if (result.length != 1) {
                    throw new ElasticsearchStatusException(
                        "Expected exactly [1] value in sequence_similarity result; got [{}]",
                        RestStatus.INTERNAL_SERVER_ERROR,
                        result.length
                    );
                }
                spanScoreFunction.accept(result[0]);
            }
            return new SequenceSimilarityInferenceResults(
                Optional.ofNullable(resultsField).orElse(DEFAULT_RESULTS_FIELD),
                spanScoreFunction.score(),
                tokenization.anyTruncated()
            );
        }
    }

    static SpanScoreFunction fromConfig(SequenceSimilarityConfig.SpanScoreFunction spanScoreFunction) {
        return switch (spanScoreFunction) {
            case MAX -> new Max();
            case MEAN -> new Mean();
        };
    }

    private interface SpanScoreFunction {
        void accept(double v);

        double score();
    }

    private static class Max implements SpanScoreFunction {
        private double score = Double.NEGATIVE_INFINITY;

        @Override
        public void accept(double v) {
            score = Math.max(score, v);
        }

        @Override
        public double score() {
            return score;
        }
    }

    private static class Mean implements SpanScoreFunction {
        private double score = 0.0;
        private int count = 0;

        @Override
        public void accept(double v) {
            score += v;
            count++;
        }

        @Override
        public double score() {
            return score / count;
        }
    }
}
