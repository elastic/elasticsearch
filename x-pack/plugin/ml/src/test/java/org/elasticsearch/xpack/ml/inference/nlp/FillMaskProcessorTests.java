/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.results.FillMaskResults;
import org.elasticsearch.xpack.ml.inference.deployment.PyTorchResult;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizer;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;

public class FillMaskProcessorTests extends ESTestCase {

    public void testProcessResults() {
        // only the scores of the MASK index array
        // are used the rest is filler
        double[][] scores = {
            { 0, 0, 0, 0, 0, 0, 0}, // The
            { 0, 0, 0, 0, 0, 0, 0}, // capital
            { 0, 0, 0, 0, 0, 0, 0}, // of
            { 0.01, 0.01, 0.3, 0.1, 0.01, 0.2, 1.2}, // MASK
            { 0, 0, 0, 0, 0, 0, 0}, // is
            { 0, 0, 0, 0, 0, 0, 0} // paris
        };

        String input = "The capital of " + BertTokenizer.MASK_TOKEN + " is Paris";

        List<String> vocab = Arrays.asList("The", "capital", "of", BertTokenizer.MASK_TOKEN, "is", "Paris", "France");
        List<String> tokens = Arrays.asList(input.split(" "));
        int[] tokenMap = new int[] {0, 1, 2, 3, 4, 5};
        int[] tokenIds = new int[] {0, 1, 2, 3, 4, 5};

        BertTokenizer.TokenizationResult tokenization = new BertTokenizer.TokenizationResult(input, vocab, tokens,
            tokenIds, tokenMap);

        FillMaskProcessor processor = new FillMaskProcessor(mock(BertTokenizer.class), mock(NlpTaskConfig.class));
        FillMaskResults result = (FillMaskResults) processor.processResult(tokenization, new PyTorchResult("1", scores, 0L, null));
        assertThat(result.getPredictions(), hasSize(5));
        FillMaskResults.Prediction prediction = result.getPredictions().get(0);
        assertEquals("France", prediction.getToken());
        assertEquals("The capital of France is Paris", prediction.getSequence());

        prediction = result.getPredictions().get(1);
        assertEquals("of", prediction.getToken());
        assertEquals("The capital of of is Paris", prediction.getSequence());

        prediction = result.getPredictions().get(2);
        assertEquals("Paris", prediction.getToken());
        assertEquals("The capital of Paris is Paris", prediction.getSequence());
    }

    public void testProcessResults_GivenMissingTokens() {
        BertTokenizer.TokenizationResult tokenization =
            new BertTokenizer.TokenizationResult("", Collections.emptyList(), Collections.emptyList(),
            new int[] {}, new int[] {});

        FillMaskProcessor processor = new FillMaskProcessor(mock(BertTokenizer.class), mock(NlpTaskConfig.class));
        PyTorchResult pyTorchResult = new PyTorchResult("1", new double[][]{{}}, 0L, null);
        FillMaskResults result = (FillMaskResults) processor.processResult(tokenization, pyTorchResult);
        assertThat(result.getPredictions(), empty());
    }

    public void testValidate_GivenMissingMaskToken() {
        String input = "The capital of France is Paris";

        FillMaskProcessor processor = new FillMaskProcessor(mock(BertTokenizer.class), mock(NlpTaskConfig.class));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> processor.validateInputs(input));
        assertThat(e.getMessage(), containsString("no [MASK] token could be found"));
    }


    public void testProcessResults_GivenMultipleMaskTokens() {
        String input = "The capital of [MASK] is [MASK]";

        FillMaskProcessor processor = new FillMaskProcessor(mock(BertTokenizer.class), mock(NlpTaskConfig.class));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> processor.validateInputs(input));
        assertThat(e.getMessage(), containsString("only one [MASK] token should exist in the input"));
    }
}
