/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.results.FillMaskResults;
import org.elasticsearch.xpack.core.ml.inference.results.TopClassEntry;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.FillMaskConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.VocabularyConfig;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BasicTokenizer;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizer;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.DelimitedToken;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.TokenizationResult;
import org.elasticsearch.xpack.ml.inference.pytorch.results.PyTorchInferenceResult;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.OptionalInt;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FillMaskProcessorTests extends ESTestCase {

    public void testProcessResults() {
        // only the scores of the MASK index array
        // are used the rest is filler
        double[][][] scores = {
            {
                { 0, 0, 0, 0, 0, 0, 0 }, // The
                { 0, 0, 0, 0, 0, 0, 0 }, // capital
                { 0, 0, 0, 0, 0, 0, 0 }, // of
                { 0.01, 0.01, 0.3, 0.1, 0.01, 0.2, 1.2 }, // MASK
                { 0, 0, 0, 0, 0, 0, 0 }, // is
                { 0, 0, 0, 0, 0, 0, 0 } // paris
            } };

        String input = "The capital of " + BertTokenizer.MASK_TOKEN + " is Paris";

        List<String> vocab = Arrays.asList("The", "capital", "of", BertTokenizer.MASK_TOKEN, "is", "Paris", "France");
        List<DelimitedToken> tokens = new BasicTokenizer(randomBoolean(), randomBoolean(), randomBoolean()).tokenize(input);

        int[] tokenMap = new int[] { 0, 1, 2, 3, 4, 5 };
        int[] tokenIds = new int[] { 0, 1, 2, 3, 4, 5 };

        TokenizationResult tokenization = new TokenizationResult(vocab);
        tokenization.addTokenization(input, false, tokens, tokenIds, tokenMap);

        BertTokenizer tokenizer = mock(BertTokenizer.class);
        when(tokenizer.getMaskToken()).thenReturn(BertTokenizer.MASK_TOKEN);
        when(tokenizer.getMaskTokenId()).thenReturn(OptionalInt.of(3));

        String resultsField = randomAlphaOfLength(10);
        FillMaskResults result = (FillMaskResults) FillMaskProcessor.processResult(
            tokenization,
            new PyTorchInferenceResult("1", scores, 0L, null),
            tokenizer,
            4,
            resultsField
        );
        assertThat(result.asMap().get(resultsField), equalTo("France"));
        assertThat(result.getTopClasses(), hasSize(4));
        assertEquals("France", result.getClassificationLabel());
        assertEquals("The capital of France is Paris", result.getPredictedSequence());

        TopClassEntry prediction = result.getTopClasses().get(1);
        assertEquals("of", prediction.getClassification());

        prediction = result.getTopClasses().get(2);
        assertEquals("Paris", prediction.getClassification());
    }

    public void testProcessResults_GivenMissingTokens() {
        BertTokenizer tokenizer = mock(BertTokenizer.class);
        when(tokenizer.getMaskToken()).thenReturn("[MASK]");

        TokenizationResult tokenization = new TokenizationResult(Collections.emptyList());
        tokenization.addTokenization("", false, Collections.emptyList(), new int[] {}, new int[] {});

        PyTorchInferenceResult pyTorchResult = new PyTorchInferenceResult("1", new double[][][] { { {} } }, 0L, null);
        expectThrows(
            ElasticsearchStatusException.class,
            () -> FillMaskProcessor.processResult(tokenization, pyTorchResult, tokenizer, 5, randomAlphaOfLength(10))
        );
    }

    public void testValidate_GivenMissingMaskToken() {
        List<String> input = List.of("The capital of France is Paris");

        BertTokenizer tokenizer = mock(BertTokenizer.class);
        when(tokenizer.getMaskToken()).thenReturn("[MASK]");
        FillMaskConfig config = new FillMaskConfig(new VocabularyConfig("test-index"), null, null, null);
        FillMaskProcessor processor = new FillMaskProcessor(tokenizer, config);

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> processor.validateInputs(input));
        assertThat(e.getMessage(), containsString("no [MASK] token could be found"));
    }

    public void testProcessResults_GivenMultipleMaskTokens() {
        List<String> input = List.of("The capital of [MASK] is [MASK]");

        BertTokenizer tokenizer = mock(BertTokenizer.class);
        when(tokenizer.getMaskToken()).thenReturn("[MASK]");

        FillMaskConfig config = new FillMaskConfig(new VocabularyConfig("test-index"), null, null, null);
        FillMaskProcessor processor = new FillMaskProcessor(tokenizer, config);

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> processor.validateInputs(input));
        assertThat(e.getMessage(), containsString("only one [MASK] token should exist in the input"));
    }
}
