/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.results.MlChunkedTextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.ml.inference.results.MlChunkedTextExpansionResults;
import org.elasticsearch.xpack.core.ml.inference.results.MlTextEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.BertTokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.Tokenization;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizationResult;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizer;
import org.elasticsearch.xpack.ml.inference.pytorch.results.PyTorchInferenceResult;

import java.util.Map;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.IsNot.not;

public class TextEmbeddingProcessorTests extends ESTestCase {

    public void testSingleResult() {
        try (
            BertTokenizer tokenizer = BertTokenizer.builder(
                TextExpansionProcessorTests.TEST_CASED_VOCAB,
                new BertTokenization(null, false, 5, Tokenization.Truncate.NONE, 0)
            ).build()
        ) {
            var pytorchResult = new PyTorchInferenceResult(new double[][][] { { { 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0 } } });

            var input = "Elasticsearch darts champion";
            var tokenization = tokenizer.tokenize(input, Tokenization.Truncate.NONE, 0, 0, null);
            var tokenizationResult = new BertTokenizationResult(TextExpansionProcessorTests.TEST_CASED_VOCAB, tokenization, 0);
            var inferenceResult = TextEmbeddingProcessor.processResult(tokenizationResult, pytorchResult, "foo", false);
            assertThat(inferenceResult, instanceOf(MlTextEmbeddingResults.class));

            var result = (MlTextEmbeddingResults) inferenceResult;
            assertThat(result.getInference().length, greaterThan(0));
        }
    }

    public void testChunking() {
        try (
            BertTokenizer tokenizer = BertTokenizer.builder(
                TextExpansionProcessorTests.TEST_CASED_VOCAB,
                new BertTokenization(null, false, 5, Tokenization.Truncate.NONE, 0)
            ).build()
        ) {
            var pytorchResult = new PyTorchInferenceResult(
                new double[][][] { { { 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0 }, { 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0 } } }
            );

            var input = "Elasticsearch darts champion little red is fun car";
            var tokenization = tokenizer.tokenize(input, Tokenization.Truncate.NONE, 0, 0, null);
            var tokenizationResult = new BertTokenizationResult(TextExpansionProcessorTests.TEST_CASED_VOCAB, tokenization, 0);
            var inferenceResult = TextEmbeddingProcessor.processResult(tokenizationResult, pytorchResult, "foo", true);
            assertThat(inferenceResult, instanceOf(MlChunkedTextEmbeddingFloatResults.class));

            var chunkedResult = (MlChunkedTextEmbeddingFloatResults) inferenceResult;
            assertThat(chunkedResult.getChunks(), hasSize(2));
            assertEquals("Elasticsearch darts champion little red", chunkedResult.getChunks().get(0).matchedText());
            assertEquals("is fun car", chunkedResult.getChunks().get(1).matchedText());
            assertThat(chunkedResult.getChunks().get(0).embedding().length, greaterThan(0));
            assertThat(chunkedResult.getChunks().get(1).embedding().length, greaterThan(0));
        }
    }

    public void testChunkingWithEmptyString() {
        try (
            BertTokenizer tokenizer = BertTokenizer.builder(
                TextExpansionProcessorTests.TEST_CASED_VOCAB,
                new BertTokenization(null, false, 5, Tokenization.Truncate.NONE, 0)
            ).build()
        ) {
            var pytorchResult = new PyTorchInferenceResult(new double[][][] { { { 1.0, 2.0, 3.0, 4.0, 5.0 } } });

            var input = "";
            var tokenization = tokenizer.tokenize(input, Tokenization.Truncate.NONE, 0, 0, null);
            var tokenizationResult = new BertTokenizationResult(TextExpansionProcessorTests.TEST_CASED_VOCAB, tokenization, 0);
            var inferenceResult = TextExpansionProcessor.processResult(tokenizationResult, pytorchResult, Map.of(), "foo", true);
            assertThat(inferenceResult, instanceOf(MlChunkedTextExpansionResults.class));

            var chunkedResult = (MlChunkedTextExpansionResults) inferenceResult;
            assertThat(chunkedResult.getChunks(), hasSize(1));
            assertEquals("", chunkedResult.getChunks().get(0).matchedText());
            assertThat(chunkedResult.getChunks().get(0).weightedTokens(), not(empty()));
        }
    }
}
