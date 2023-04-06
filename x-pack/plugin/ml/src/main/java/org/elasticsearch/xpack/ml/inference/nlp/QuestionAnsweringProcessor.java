/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.QuestionAnsweringInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.QuestionAnsweringConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.Tokenization;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.NlpTokenizer;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.TokenizationResult;
import org.elasticsearch.xpack.ml.inference.pytorch.results.PyTorchInferenceResult;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.IntPredicate;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig.DEFAULT_RESULTS_FIELD;

public class QuestionAnsweringProcessor extends NlpTask.Processor {

    QuestionAnsweringProcessor(NlpTokenizer tokenizer) {
        super(tokenizer);
    }

    @Override
    public void validateInputs(List<String> inputs) {
        // nothing to validate
    }

    @Override
    public NlpTask.RequestBuilder getRequestBuilder(NlpConfig nlpConfig) {
        if (nlpConfig instanceof QuestionAnsweringConfig questionAnsweringConfig) {
            return new RequestBuilder(tokenizer, questionAnsweringConfig.getQuestion());
        }
        throw ExceptionsHelper.badRequestException(
            "please provide configuration update for question_answering task including the desired [question]"
        );
    }

    @Override
    public NlpTask.ResultProcessor getResultProcessor(NlpConfig nlpConfig) {
        if (nlpConfig instanceof QuestionAnsweringConfig questionAnsweringConfig) {
            int maxAnswerLength = questionAnsweringConfig.getMaxAnswerLength();
            int numTopClasses = questionAnsweringConfig.getNumTopClasses();
            String resultsFieldValue = questionAnsweringConfig.getResultsField();
            return new ResultProcessor(questionAnsweringConfig.getQuestion(), maxAnswerLength, numTopClasses, resultsFieldValue);
        }
        throw ExceptionsHelper.badRequestException(
            "please provide configuration update for question_answering task including the desired [question]"
        );
    }

    record RequestBuilder(NlpTokenizer tokenizer, String question) implements NlpTask.RequestBuilder {

        @Override
        public NlpTask.Request buildRequest(List<String> inputs, String requestId, Tokenization.Truncate truncate, int span)
            throws IOException {
            if (inputs.size() > 1) {
                throw ExceptionsHelper.badRequestException("Unable to do question answering on more than one text input at a time");
            }
            String context = inputs.get(0);
            List<TokenizationResult.Tokens> tokenizations = tokenizer.tokenize(question, context, truncate, span, 0);
            TokenizationResult result = tokenizer.buildTokenizationResult(tokenizations);
            return result.buildRequest(requestId, truncate);
        }
    }

    record ResultProcessor(String question, int maxAnswerLength, int numTopClasses, String resultsField)
        implements
            NlpTask.ResultProcessor {

        @Override
        public InferenceResults processResult(TokenizationResult tokenization, PyTorchInferenceResult pyTorchResult) {
            if (pyTorchResult.getInferenceResult().length < 1) {
                throw new ElasticsearchStatusException("question answering result has no data", RestStatus.INTERNAL_SERVER_ERROR);
            }
            // Should be a collection of "starts" and "ends"
            if (pyTorchResult.getInferenceResult().length != 2) {
                throw new ElasticsearchStatusException(
                    "question answering result has invalid dimension, expected 2 found [{}]",
                    RestStatus.INTERNAL_SERVER_ERROR,
                    pyTorchResult.getInferenceResult().length
                );
            }
            double[][] starts = pyTorchResult.getInferenceResult()[0];
            double[][] ends = pyTorchResult.getInferenceResult()[1];
            if (starts.length != ends.length) {
                throw new ElasticsearchStatusException(
                    "question answering result has invalid dimensions; start positions [{}] must equal potential end [{}]",
                    RestStatus.INTERNAL_SERVER_ERROR,
                    starts.length,
                    ends.length
                );
            }
            List<TokenizationResult.Tokens> tokensList = tokenization.getTokensBySequenceId().get(0);
            if (starts.length != tokensList.size()) {
                throw new ElasticsearchStatusException(
                    "question answering result has invalid dimensions; start positions number [{}] equal batched token size [{}]",
                    RestStatus.INTERNAL_SERVER_ERROR,
                    starts.length,
                    tokensList.size()
                );
            }
            final int numAnswersToGather = Math.max(numTopClasses, 1);

            ScoreAndIndicesPriorityQueue finalEntries = new ScoreAndIndicesPriorityQueue(numAnswersToGather);
            for (int i = 0; i < starts.length; i++) {
                topScores(
                    starts[i],
                    ends[i],
                    numAnswersToGather,
                    finalEntries::insertWithOverflow,
                    tokensList.get(i).seqPairOffset(),
                    tokensList.get(i).tokenIds().length,
                    maxAnswerLength,
                    i
                );
            }
            QuestionAnsweringInferenceResults.TopAnswerEntry[] topAnswerList =
                new QuestionAnsweringInferenceResults.TopAnswerEntry[numAnswersToGather];
            for (int i = numAnswersToGather - 1; i >= 0; i--) {
                ScoreAndIndices scoreAndIndices = finalEntries.pop();
                TokenizationResult.Tokens tokens = tokensList.get(scoreAndIndices.spanIndex());
                int startOffset = tokens.tokens().get(1).get(scoreAndIndices.startToken).startOffset();
                int endOffset = tokens.tokens().get(1).get(scoreAndIndices.endToken).endOffset();
                topAnswerList[i] = new QuestionAnsweringInferenceResults.TopAnswerEntry(
                    tokens.input().get(1).substring(startOffset, endOffset),
                    scoreAndIndices.score(),
                    startOffset,
                    endOffset
                );
            }
            QuestionAnsweringInferenceResults.TopAnswerEntry finalAnswer = topAnswerList[0];
            return new QuestionAnsweringInferenceResults(
                finalAnswer.answer(),
                finalAnswer.startOffset(),
                finalAnswer.endOffset(),
                numTopClasses > 0 ? Arrays.asList(topAnswerList) : List.of(),
                Optional.ofNullable(resultsField).orElse(DEFAULT_RESULTS_FIELD),
                finalAnswer.score(),
                tokenization.anyTruncated()
            );
        }
    }

    /**
     *
     * @param start The starting token index scores. May include padded tokens.
     * @param end The ending token index scores. May include padded tokens.
     * @param numAnswersToGather How many top answers to return
     * @param topScoresCollector Called when a score is collected. May be called many more times than numAnswersToGather
     * @param seq2Start The token position of where the context sequence starts. This is AFTER the sequence separation special tokens.
     * @param tokenSize The true total tokenization size. This should NOT include padded tokens.
     * @param maxAnswerLength The maximum answer length to consider.
     * @param spanIndex Which sequence span is this.
     */
    static void topScores(
        double[] start,
        double[] end,
        int numAnswersToGather,
        Consumer<ScoreAndIndices> topScoresCollector,
        int seq2Start,
        int tokenSize,
        int maxAnswerLength,
        int spanIndex
    ) {
        if (start.length != end.length) {
            throw new ElasticsearchStatusException(
                "question answering result has invalid dimensions; possible start tokens [{}] must equal possible end tokens [{}]",
                RestStatus.INTERNAL_SERVER_ERROR,
                start.length,
                end.length
            );
        }
        // This needs to be the start of the second sequence skipping the separator tokens
        // Example seq1 </s> </s> seq2, seq2Start should be (len(seq1) + 2)
        // This predicate ensures the following
        // - we include the cls token
        // - we exclude the first sequence, which is always the question
        // - we exclude the final token, which is a sep token
        double[] startNormalized = normalizeWith(start, i -> {
            if (i == 0) {
                return true;
            }
            return i >= seq2Start && i < tokenSize - 1;
        }, -10000.0);
        double[] endNormalized = normalizeWith(end, i -> {
            if (i == 0) {
                return true;
            }
            return i >= seq2Start && i < tokenSize - 1;
        }, -10000.0);
        // We use CLS in the softmax, but then remove it from being considered a possible position
        endNormalized[0] = startNormalized[0] = 0.0;
        if (numAnswersToGather == 1) {
            ScoreAndIndices toReturn = new ScoreAndIndices(0, 0, 0.0, spanIndex);
            double maxScore = 0.0;
            for (int i = seq2Start; i < tokenSize; i++) {
                if (startNormalized[i] == 0) {
                    continue;
                }
                for (int j = i; j < (maxAnswerLength + i) && j < tokenSize; j++) {
                    double score = startNormalized[i] * endNormalized[j];
                    if (score > maxScore) {
                        maxScore = score;
                        toReturn = new ScoreAndIndices(i - seq2Start, j - seq2Start, score, spanIndex);
                    }
                }
            }
            topScoresCollector.accept(toReturn);
            return;
        }
        for (int i = seq2Start; i < tokenSize; i++) {
            for (int j = i; j < (maxAnswerLength + i) && j < tokenSize; j++) {
                topScoresCollector.accept(
                    new ScoreAndIndices(i - seq2Start, j - seq2Start, startNormalized[i] * endNormalized[j], spanIndex)
                );
            }
        }
    }

    static double[] normalizeWith(double[] values, IntPredicate mutateIndex, double predicateValue) {
        double[] toReturn = new double[values.length];
        for (int i = 0; i < values.length; i++) {
            toReturn[i] = values[i];
            if (mutateIndex.test(i) == false) {
                toReturn[i] = predicateValue;
            }
        }
        double expSum = 0.0;
        for (double v : toReturn) {
            expSum += Math.exp(v);
        }
        double diff = Math.log(expSum);
        for (int i = 0; i < toReturn.length; i++) {
            toReturn[i] = Math.exp(toReturn[i] - diff);
        }
        return toReturn;
    }

    static class ScoreAndIndicesPriorityQueue extends PriorityQueue<ScoreAndIndices> {

        ScoreAndIndicesPriorityQueue(int maxSize) {
            super(maxSize);
        }

        @Override
        protected boolean lessThan(ScoreAndIndices a, ScoreAndIndices b) {
            return a.compareTo(b) < 0;
        }
    }

    record ScoreAndIndices(int startToken, int endToken, double score, int spanIndex) implements Comparable<ScoreAndIndices> {
        @Override
        public int compareTo(ScoreAndIndices o) {
            return Double.compare(score, o.score);
        }
    }
}
