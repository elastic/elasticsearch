/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.test.ESTestCase;
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

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class TextExpansionProcessorTests extends ESTestCase {

    public void testProcessResult() {
        double[][][] pytorchResult = new double[][][] { { { 0.0, 1.0, 0.0, 3.0, 4.0, 0.0, 0.0 } } };

        TokenizationResult tokenizationResult = new BertTokenizationResult(List.of("a", "b", "c", "d", "e", "f", "g"), List.of(), 0);

        var inferenceResult = TextExpansionProcessor.processResult(
            tokenizationResult,
            new PyTorchInferenceResult(pytorchResult),
            Map.of(),
            "foo"
        );
        assertThat(inferenceResult, instanceOf(TextExpansionResults.class));
        var results = (TextExpansionResults) inferenceResult;
        assertEquals(results.getResultsField(), "foo");

        var weightedTokens = results.getWeightedTokens();
        assertThat(weightedTokens, hasSize(3));
        assertEquals(new TextExpansionResults.WeightedToken("e", 4.0f), weightedTokens.get(0));
        assertEquals(new TextExpansionResults.WeightedToken("d", 3.0f), weightedTokens.get(1));
        assertEquals(new TextExpansionResults.WeightedToken("b", 1.0f), weightedTokens.get(2));
    }

    public void testProcessResultMultipleVectors() {
        double[][][] pytorchResult = new double[][][] { { { 0.0, 1.0, 0.0, 1.0, 4.0, 0.0, 0.0 }, { 1.0, 2.0, 0.0, 3.0, 4.0, 0.0, 0.1 } } };

        TokenizationResult tokenizationResult = new BertTokenizationResult(List.of("a", "b", "c", "d", "e", "f", "g"), List.of(), 0);

        var inferenceResult = TextExpansionProcessor.processResult(
            tokenizationResult,
            new PyTorchInferenceResult(pytorchResult),
            Map.of(),
            "foo"
        );
        assertThat(inferenceResult, instanceOf(TextExpansionResults.class));
        var results = (TextExpansionResults) inferenceResult;
        assertEquals(results.getResultsField(), "foo");

        var weightedTokens = results.getWeightedTokens();
        assertThat(weightedTokens, hasSize(5));
        assertEquals(new TextExpansionResults.WeightedToken("e", 4.0f), weightedTokens.get(0));
        assertEquals(new TextExpansionResults.WeightedToken("d", 3.0f), weightedTokens.get(1));
        assertEquals(new TextExpansionResults.WeightedToken("b", 2.0f), weightedTokens.get(2));
        assertEquals(new TextExpansionResults.WeightedToken("a", 1.0f), weightedTokens.get(3));
        assertEquals(new TextExpansionResults.WeightedToken("g", 0.1f), weightedTokens.get(4));
    }

    public void testSanitiseVocab() {
        double[][][] pytorchResult = new double[][][] { { { 1.0, 2.0, 3.0, 4.0, 5.0, 6.0 } } };

        TokenizationResult tokenizationResult = new BertTokenizationResult(List.of("aaa", "bbb", "ccc", ".", "eee.", "fff"), List.of(), 0);

        var inferenceResult = TextExpansionProcessor.processResult(
            tokenizationResult,
            new PyTorchInferenceResult(pytorchResult),
            Map.of(4, "XXX", 3, "YYY"),
            "foo"
        );
        assertThat(inferenceResult, instanceOf(TextExpansionResults.class));
        var results = (TextExpansionResults) inferenceResult;
        assertEquals(results.getResultsField(), "foo");

        var weightedTokens = results.getWeightedTokens();
        assertThat(weightedTokens, hasSize(6));
        assertEquals(new TextExpansionResults.WeightedToken("fff", 6.0f), weightedTokens.get(0));
        assertEquals(new TextExpansionResults.WeightedToken("XXX", 5.0f), weightedTokens.get(1));
        assertEquals(new TextExpansionResults.WeightedToken("YYY", 4.0f), weightedTokens.get(2));
        assertEquals(new TextExpansionResults.WeightedToken("ccc", 3.0f), weightedTokens.get(3));
        assertEquals(new TextExpansionResults.WeightedToken("bbb", 2.0f), weightedTokens.get(4));
        assertEquals(new TextExpansionResults.WeightedToken("aaa", 1.0f), weightedTokens.get(5));
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

        TextExpansionResults results = (TextExpansionResults) resultProcessor.processResult(tokenizationResult, pytorchResult);
        var weightedTokens = results.getWeightedTokens();
        assertThat(weightedTokens, hasSize(5));
        assertEquals(new TextExpansionResults.WeightedToken("##__", 5.0f), weightedTokens.get(0));
        assertEquals(new TextExpansionResults.WeightedToken("__", 4.0f), weightedTokens.get(1));
        assertEquals(new TextExpansionResults.WeightedToken("cc", 3.0f), weightedTokens.get(2));
        assertEquals(new TextExpansionResults.WeightedToken("bb", 2.0f), weightedTokens.get(3));
        assertEquals(new TextExpansionResults.WeightedToken("aa", 1.0f), weightedTokens.get(4));
    }
}
