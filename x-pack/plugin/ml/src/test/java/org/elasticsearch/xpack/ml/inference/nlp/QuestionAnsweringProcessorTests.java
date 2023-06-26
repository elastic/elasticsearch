/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.results.QuestionAnsweringInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.BertTokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.QuestionAnsweringConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.Tokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.VocabularyConfig;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizer;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.TokenizationResult;
import org.elasticsearch.xpack.ml.inference.pytorch.results.PyTorchInferenceResult;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizerTests.TEST_CASED_VOCAB;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class QuestionAnsweringProcessorTests extends ESTestCase {

    private static final double[] START_TOKEN_SCORES = new double[] {
        1.6665655,
        -7.988514,
        -8.249796,
        .529973,
        -8.46703,
        -8.345977,
        -8.459701,
        -8.260341,
        .071103,
        -7.339133,
        -7.647086,
        -8.165343,
        -8.277936,
        -8.156116,
        -8.104215,
        -8.45849,
        -8.249917,
        -2.0896196,
        -0.67172474 };

    private static final double[] END_TOKEN_SCORES = new double[] {
        1.0593028,
        -8.276232,
        -7.9352865,
        -8.340191,
        -8.326643,
        -8.225507,
        -8.548992,
        -8.50256,
        -8.716394,
        -8.0558195,
        -8.4110565,
        -6.564298,
        -8.570332,
        .01,
        -7.2465587,
        .6000237,
        -8.045577,
        -6.3548584,
        -3.5642238 };

    // The data here is nonsensical. We just want to make sure tokens chosen match up with our scores
    public void testProcessor() throws IOException {
        String question = "is Elasticsearch fun?";
        String input = "Pancake day is fun with Elasticsearch and little red car";
        BertTokenization tokenization = new BertTokenization(false, true, 384, Tokenization.Truncate.NONE, 128);
        BertTokenizer tokenizer = BertTokenizer.builder(TEST_CASED_VOCAB, tokenization).build();
        QuestionAnsweringConfig config = new QuestionAnsweringConfig(question, 1, 10, new VocabularyConfig(""), tokenization, "prediction");
        QuestionAnsweringProcessor processor = new QuestionAnsweringProcessor(tokenizer);
        TokenizationResult tokenizationResult = processor.getRequestBuilder(config)
            .buildRequest(List.of(input), "1", Tokenization.Truncate.NONE, 128)
            .tokenization();
        assertThat(tokenizationResult.anyTruncated(), is(false));
        assertThat(tokenizationResult.getTokenization(0).tokenIds().length, equalTo(END_TOKEN_SCORES.length));
        // tokenized question length with cls and sep token
        assertThat(tokenizationResult.getTokenization(0).seqPairOffset(), equalTo(7));
        double[][][] scores = { { START_TOKEN_SCORES }, { END_TOKEN_SCORES } };
        NlpTask.ResultProcessor resultProcessor = processor.getResultProcessor(config);
        PyTorchInferenceResult pyTorchResult = new PyTorchInferenceResult(scores);
        QuestionAnsweringInferenceResults result = (QuestionAnsweringInferenceResults) resultProcessor.processResult(
            tokenizationResult,
            pyTorchResult
        );

        // Note this is a different answer to testTopScores because of the question length
        assertThat(result.getScore(), closeTo(0.05264939, 1e-6));
        // These are the token offsets by char
        assertThat(result.getStartOffset(), equalTo(8));
        assertThat(result.getEndOffset(), equalTo(48));
        assertThat(result.getAnswer(), equalTo(input.substring(8, 48)));
    }

    public void testTopScoresRespectsAnswerLength() {
        int seq2Start = 8;
        int numAnswersToGather = 1;
        AtomicReference<QuestionAnsweringProcessor.ScoreAndIndices> result = new AtomicReference<>();
        QuestionAnsweringProcessor.topScores(
            START_TOKEN_SCORES,
            END_TOKEN_SCORES,
            numAnswersToGather,
            result::set,
            seq2Start,
            START_TOKEN_SCORES.length,
            10,
            0
        );
        assertThat(result.get().score(), closeTo(0.05265336, 1e-6));
        // The token positions as related to the second sequence start
        assertThat(result.get().startToken(), equalTo(0));
        assertThat(result.get().endToken(), equalTo(7));

        // Restrict to a shorter answer length
        QuestionAnsweringProcessor.topScores(
            START_TOKEN_SCORES,
            END_TOKEN_SCORES,
            numAnswersToGather,
            result::set,
            seq2Start,
            START_TOKEN_SCORES.length,
            6,
            0
        );
        assertThat(result.get().score(), closeTo(0.0291865, 1e-6));
        // The token positions as related to the second sequence start
        assertThat(result.get().startToken(), equalTo(0));
        assertThat(result.get().endToken(), equalTo(5));
    }

    public void testTopScoresMoreThanOne() {
        int seq2Start = 8;
        int numAnswersToGather = 2;
        QuestionAnsweringProcessor.ScoreAndIndicesPriorityQueue result = new QuestionAnsweringProcessor.ScoreAndIndicesPriorityQueue(2);
        QuestionAnsweringProcessor.topScores(
            START_TOKEN_SCORES,
            END_TOKEN_SCORES,
            numAnswersToGather,
            result::insertWithOverflow,
            seq2Start,
            START_TOKEN_SCORES.length,
            10,
            0
        );

        assertThat(result.size(), equalTo(numAnswersToGather));

        QuestionAnsweringProcessor.ScoreAndIndices[] topScores = new QuestionAnsweringProcessor.ScoreAndIndices[numAnswersToGather];
        for (int i = numAnswersToGather - 1; i >= 0; i--) {
            topScores[i] = result.pop();
        }

        assertThat(topScores[0].score(), closeTo(0.05265336, 1e-6));
        assertThat(topScores[0].startToken(), equalTo(0));
        assertThat(topScores[0].endToken(), equalTo(7));

        assertThat(topScores[1].score(), closeTo(0.0291865, 1e-6));
        assertThat(topScores[1].startToken(), equalTo(0));
        assertThat(topScores[1].endToken(), equalTo(5));
    }

}
