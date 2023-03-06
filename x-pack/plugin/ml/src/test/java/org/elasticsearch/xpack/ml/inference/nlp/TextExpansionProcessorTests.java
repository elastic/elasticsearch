/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizationResult;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.TokenizationResult;
import org.elasticsearch.xpack.ml.inference.pytorch.results.PyTorchInferenceResult;

import java.util.List;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class TextExpansionProcessorTests extends ESTestCase {

    public void testProcessResult() {
        double[][][] pytorchResult = new double[][][] { { { 0.0, 1.0, 0.0, 3.0, 4.0, 0.0, 0.0 } } };

        TokenizationResult tokenizationResult = new BertTokenizationResult(List.of(), List.of(), 0);

        var inferenceResult = TextExpansionProcessor.processResult(tokenizationResult, new PyTorchInferenceResult(pytorchResult), "foo");
        assertThat(inferenceResult, instanceOf(TextExpansionResults.class));
        var results = (TextExpansionResults) inferenceResult;
        assertEquals(results.getResultsField(), "foo");

        var weightedTokens = results.getWeightedTokens();
        assertThat(weightedTokens, hasSize(3));
        assertEquals(new TextExpansionResults.WeightedToken(1, 1.0f), weightedTokens.get(0));
        assertEquals(new TextExpansionResults.WeightedToken(3, 3.0f), weightedTokens.get(1));
        assertEquals(new TextExpansionResults.WeightedToken(4, 4.0f), weightedTokens.get(2));
    }

    public void testProcessResultMultipleVectors() {
        double[][][] pytorchResult = new double[][][] { { { 0.0, 1.0, 0.0, 1.0, 4.0, 0.0, 0.0 }, { 1.0, 2.0, 0.0, 3.0, 4.0, 0.0, 0.1 } } };

        TokenizationResult tokenizationResult = new BertTokenizationResult(List.of(), List.of(), 0);

        var inferenceResult = TextExpansionProcessor.processResult(tokenizationResult, new PyTorchInferenceResult(pytorchResult), "foo");
        assertThat(inferenceResult, instanceOf(TextExpansionResults.class));
        var results = (TextExpansionResults) inferenceResult;
        assertEquals(results.getResultsField(), "foo");

        var weightedTokens = results.getWeightedTokens();
        assertThat(weightedTokens, hasSize(5));
        assertEquals(new TextExpansionResults.WeightedToken(0, 1.0f), weightedTokens.get(0));
        assertEquals(new TextExpansionResults.WeightedToken(1, 2.0f), weightedTokens.get(1));
        assertEquals(new TextExpansionResults.WeightedToken(3, 3.0f), weightedTokens.get(2));
        assertEquals(new TextExpansionResults.WeightedToken(4, 4.0f), weightedTokens.get(3));
        assertEquals(new TextExpansionResults.WeightedToken(6, 0.1f), weightedTokens.get(4));
    }
}
