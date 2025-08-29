/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.inference.WeightedToken;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.results.MlChunkedTextExpansionResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.BertTokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextExpansionConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.Tokenization;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizationResult;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizer;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.TokenizationResult;
import org.elasticsearch.xpack.ml.inference.pytorch.results.PyTorchInferenceResult;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.IsNot.not;

public class TextExpansionProcessorTests extends ESTestCase {

    public static final List<String> TEST_CASED_VOCAB = List.of(
        BertTokenizer.PAD_TOKEN,
        "Elasticsearch",
        "is",
        "fun",
        "my",
        "little",
        "red",
        "car",
        "darts",
        "champion",
        BertTokenizer.CLASS_TOKEN,
        BertTokenizer.SEPARATOR_TOKEN,
        BertTokenizer.MASK_TOKEN,
        BertTokenizer.UNKNOWN_TOKEN
    );

    public void testProcessResult() {
        double[][][] pytorchResult = new double[][][] { { { 0.0, 1.0, 0.0, 3.0, 4.0, 0.0, 0.0 } } };

        TokenizationResult tokenizationResult = new BertTokenizationResult(List.of("a", "b", "c", "d", "e", "f", "g"), List.of(), 0);

        var inferenceResult = TextExpansionProcessor.processResult(
            tokenizationResult,
            new PyTorchInferenceResult(pytorchResult),
            Map.of(),
            "foo",
            false
        );
        assertThat(inferenceResult, instanceOf(TextExpansionResults.class));
        var results = (TextExpansionResults) inferenceResult;
        assertEquals(results.getResultsField(), "foo");

        var weightedTokens = results.getWeightedTokens();
        assertThat(weightedTokens, hasSize(3));
        assertEquals(new WeightedToken("e", 4.0f), weightedTokens.get(0));
        assertEquals(new WeightedToken("d", 3.0f), weightedTokens.get(1));
        assertEquals(new WeightedToken("b", 1.0f), weightedTokens.get(2));
    }

    public void testSanitiseVocab() {
        double[][][] pytorchResult = new double[][][] { { { 1.0, 2.0, 3.0, 4.0, 5.0, 6.0 } } };

        TokenizationResult tokenizationResult = new BertTokenizationResult(List.of("aaa", "bbb", "ccc", ".", "eee.", "fff"), List.of(), 0);

        var inferenceResult = TextExpansionProcessor.processResult(
            tokenizationResult,
            new PyTorchInferenceResult(pytorchResult),
            Map.of(4, "XXX", 3, "YYY"),
            "foo",
            false
        );
        assertThat(inferenceResult, instanceOf(TextExpansionResults.class));
        var results = (TextExpansionResults) inferenceResult;
        assertEquals(results.getResultsField(), "foo");

        var weightedTokens = results.getWeightedTokens();
        assertThat(weightedTokens, hasSize(6));
        assertEquals(new WeightedToken("fff", 6.0f), weightedTokens.get(0));
        assertEquals(new WeightedToken("XXX", 5.0f), weightedTokens.get(1));
        assertEquals(new WeightedToken("YYY", 4.0f), weightedTokens.get(2));
        assertEquals(new WeightedToken("ccc", 3.0f), weightedTokens.get(3));
        assertEquals(new WeightedToken("bbb", 2.0f), weightedTokens.get(4));
        assertEquals(new WeightedToken("aaa", 1.0f), weightedTokens.get(5));
    }

    public void testBuildSanitizedVocabMap() {
        var replacementMap = TextExpansionProcessor.buildSanitizedVocabMap(List.of("aa", "bb", "cc", ".d.", ".", "JJ"));
        assertThat(replacementMap.entrySet(), hasSize(2));
        assertEquals(replacementMap.get(3), "__d__");
        assertEquals(replacementMap.get(4), "__");
    }

    public void testSanitizeOutputTokens() {
        var vocab = List.of("aa", "bb", "cc", ".", "##.", BertTokenizer.UNKNOWN_TOKEN, BertTokenizer.PAD_TOKEN);
        var processor = new TextExpansionProcessor(
            BertTokenizer.builder(vocab, new BertTokenization(null, false, null, Tokenization.Truncate.NONE, -1)).build()
        );
        var resultProcessor = processor.getResultProcessor(new TextExpansionConfig(null, null, null));

        var pytorchResult = new PyTorchInferenceResult(new double[][][] { { { 1.0, 2.0, 3.0, 4.0, 5.0 } } });
        TokenizationResult tokenizationResult = new BertTokenizationResult(vocab, List.of(), 0);

        TextExpansionResults results = (TextExpansionResults) resultProcessor.processResult(tokenizationResult, pytorchResult, false);
        var weightedTokens = results.getWeightedTokens();
        assertThat(weightedTokens, hasSize(5));
        assertEquals(new WeightedToken("##__", 5.0f), weightedTokens.get(0));
        assertEquals(new WeightedToken("__", 4.0f), weightedTokens.get(1));
        assertEquals(new WeightedToken("cc", 3.0f), weightedTokens.get(2));
        assertEquals(new WeightedToken("bb", 2.0f), weightedTokens.get(3));
        assertEquals(new WeightedToken("aa", 1.0f), weightedTokens.get(4));
    }

    public void testChunking() {
        try (
            BertTokenizer tokenizer = BertTokenizer.builder(
                TEST_CASED_VOCAB,
                new BertTokenization(null, false, 5, Tokenization.Truncate.NONE, 0)
            ).build()
        ) {
            var pytorchResult = new PyTorchInferenceResult(
                new double[][][] { { { 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0 }, { 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0 } } }
            );

            var input = "Elasticsearch darts champion little red is fun car";
            var tokenization = tokenizer.tokenize(input, Tokenization.Truncate.NONE, 0, 0, null);
            var tokenizationResult = new BertTokenizationResult(TEST_CASED_VOCAB, tokenization, 0);
            var inferenceResult = TextExpansionProcessor.processResult(tokenizationResult, pytorchResult, Map.of(), "foo", true);
            assertThat(inferenceResult, instanceOf(MlChunkedTextExpansionResults.class));

            var chunkedResult = (MlChunkedTextExpansionResults) inferenceResult;
            assertThat(chunkedResult.getChunks(), hasSize(2));
            assertEquals("Elasticsearch darts champion little red", chunkedResult.getChunks().get(0).matchedText());
            assertEquals("is fun car", chunkedResult.getChunks().get(1).matchedText());
            assertThat(chunkedResult.getChunks().get(0).weightedTokens(), not(empty()));
            assertThat(chunkedResult.getChunks().get(1).weightedTokens(), not(empty()));
        }
    }

    public void testChunkingWithEmptyString() {
        try (
            BertTokenizer tokenizer = BertTokenizer.builder(
                TEST_CASED_VOCAB,
                new BertTokenization(null, false, 5, Tokenization.Truncate.NONE, 0)
            ).build()
        ) {
            var pytorchResult = new PyTorchInferenceResult(new double[][][] { { { 1.0, 2.0, 3.0, 4.0, 5.0 } } });

            var input = "";
            var tokenization = tokenizer.tokenize(input, Tokenization.Truncate.NONE, 0, 0, null);
            var tokenizationResult = new BertTokenizationResult(TEST_CASED_VOCAB, tokenization, 0);
            var inferenceResult = TextExpansionProcessor.processResult(tokenizationResult, pytorchResult, Map.of(), "foo", true);
            assertThat(inferenceResult, instanceOf(MlChunkedTextExpansionResults.class));

            var chunkedResult = (MlChunkedTextExpansionResults) inferenceResult;
            assertThat(chunkedResult.getChunks(), hasSize(1));
            assertEquals("", chunkedResult.getChunks().get(0).matchedText());
            assertThat(chunkedResult.getChunks().get(0).weightedTokens(), not(empty()));
        }
    }
}
