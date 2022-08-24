/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.results.TextSimilarityInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.BertTokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextSimilarityConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.Tokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.VocabularyConfig;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizationResult;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizer;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.TokenizationResult;
import org.elasticsearch.xpack.ml.inference.pytorch.results.PyTorchInferenceResult;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizerTests.TEST_CASED_VOCAB;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class TextSimilarityProcessorTests extends ESTestCase {

    // The data here is nonsensical. We just want to make sure tokens chosen match up with our scores
    public void testProcessor() throws IOException {
        String question = "is Elasticsearch fun?";
        String input = "Pancake day is fun with Elasticsearch and little red car";
        BertTokenization tokenization = new BertTokenization(false, true, 384, Tokenization.Truncate.NONE, 128);
        BertTokenizer tokenizer = BertTokenizer.builder(TEST_CASED_VOCAB, tokenization).build();
        TextSimilarityConfig textSimilarityConfig = new TextSimilarityConfig(
            question,
            new VocabularyConfig(""),
            tokenization,
            "result",
            TextSimilarityConfig.SpanScoreFunction.MAX
        );
        TextSimilarityProcessor processor = new TextSimilarityProcessor(tokenizer);
        TokenizationResult tokenizationResult = processor.getRequestBuilder(textSimilarityConfig)
            .buildRequest(List.of(input), "1", Tokenization.Truncate.NONE, 128)
            .tokenization();
        assertThat(tokenizationResult.anyTruncated(), is(false));
        assertThat(tokenizationResult.getTokenization(0).tokenIds().length, equalTo(19));
        // tokenized question length with cls and sep token
        assertThat(tokenizationResult.getTokenization(0).seqPairOffset(), equalTo(7));
        double[][][] scores = { { { 42 } } };
        NlpTask.ResultProcessor resultProcessor = processor.getResultProcessor(textSimilarityConfig);
        PyTorchInferenceResult pyTorchResult = new PyTorchInferenceResult(scores);
        TextSimilarityInferenceResults result = (TextSimilarityInferenceResults) resultProcessor.processResult(
            tokenizationResult,
            pyTorchResult
        );

        // Note this is a different answer to testTopScores because of the question length
        assertThat(result.predictedValue(), closeTo(42, 1e-6));
    }

    public void testResultFunctions() {
        BertTokenization tokenization = new BertTokenization(false, true, 384, Tokenization.Truncate.NONE, 128);
        BertTokenizer tokenizer = BertTokenizer.builder(TEST_CASED_VOCAB, tokenization).build();
        TextSimilarityConfig textSimilarityConfig = new TextSimilarityConfig(
            randomAlphaOfLength(10),
            new VocabularyConfig(""),
            tokenization,
            "result",
            TextSimilarityConfig.SpanScoreFunction.MAX
        );
        TextSimilarityProcessor processor = new TextSimilarityProcessor(tokenizer);
        NlpTask.ResultProcessor resultProcessor = processor.getResultProcessor(textSimilarityConfig);
        double[][][] scores = { { { 42 }, { 12 }, { 100 } } };
        PyTorchInferenceResult pyTorchResult = new PyTorchInferenceResult(scores);
        TextSimilarityInferenceResults result = (TextSimilarityInferenceResults) resultProcessor.processResult(
            new BertTokenizationResult(List.of(), List.of(), 1),
            pyTorchResult
        );
        assertThat(result.predictedValue(), equalTo(100.0));
        // Test mean
        textSimilarityConfig = new TextSimilarityConfig(
            randomAlphaOfLength(10),
            new VocabularyConfig(""),
            tokenization,
            "result",
            TextSimilarityConfig.SpanScoreFunction.MEAN
        );
        processor = new TextSimilarityProcessor(tokenizer);
        resultProcessor = processor.getResultProcessor(textSimilarityConfig);
        result = (TextSimilarityInferenceResults) resultProcessor.processResult(
            new BertTokenizationResult(List.of(), List.of(), 1),
            pyTorchResult
        );
        assertThat(result.predictedValue(), closeTo(51.333333333333, 1e-12));
    }

}
