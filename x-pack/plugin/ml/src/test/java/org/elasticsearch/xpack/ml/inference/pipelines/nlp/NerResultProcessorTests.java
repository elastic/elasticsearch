/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pipelines.nlp;

import org.elasticsearch.search.aggregations.pipeline.MovingFunctions;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.deployment.PyTorchResult;
import org.elasticsearch.xpack.ml.inference.pipelines.nlp.tokenizers.BertTokenizer;
import org.elasticsearch.xpack.ml.inference.pipelines.nlp.tokenizers.BertTokenizer.TokenizationResult;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class NerResultProcessorTests extends ESTestCase {

    public void testProcessResults_GivenNoTokens() {
        NerResultProcessor processor = new NerResultProcessor(new TokenizationResult(Collections.emptyList(), new int[0], new int[0]));
        NerResult result = (NerResult) processor.processResult(new PyTorchResult("test", null, null));
        assertThat(result.getEntityGroups(), is(empty()));
    }

    public void testProcessResults() {
        NerResultProcessor processor = createProcessor(Arrays.asList("el", "##astic", "##search", "many", "use", "in", "london"),
            "Many use Elasticsearch in London");
        double[][] scores = {
            { 7, 0, 0, 0, 0, 0, 0, 0, 0}, // many
            { 7, 0, 0, 0, 0, 0, 0, 0, 0}, // use
            { 0.01, 0.01, 0, 0.01, 0, 7, 0, 3, 0}, // el
            { 0.01, 0.01, 0, 0, 0, 0, 0, 0, 0}, // ##astic
            { 0, 0, 0, 0, 0, 0, 0, 0, 0}, // ##search
            { 0, 0, 0, 0, 0, 0, 0, 0, 0}, // in
            { 0, 0, 0, 0, 0, 0, 0, 6, 0} // london
        };
        NerResult result = (NerResult) processor.processResult(new PyTorchResult("1", scores, null));

        assertThat(result.getEntityGroups().size(), equalTo(2));
        assertThat(result.getEntityGroups().get(0).getWord(), equalTo("elasticsearch"));
        assertThat(result.getEntityGroups().get(0).getLabel(), equalTo(NerProcessor.Entity.ORGANISATION));
        assertThat(result.getEntityGroups().get(1).getWord(), equalTo("london"));
        assertThat(result.getEntityGroups().get(1).getLabel(), equalTo(NerProcessor.Entity.LOCATION));
    }

    public void testConvertToProbabilitiesBySoftMax_GivenConcreteExample() {
        double[][] scores = {
            { 0.1, 0.2, 3},
            { 6, 0.2, 0.1}
        };

        double[][] probabilities = NerResultProcessor.convertToProbabilitesBySoftMax(scores);

        assertThat(probabilities[0][0], closeTo(0.04931133, 0.00000001));
        assertThat(probabilities[0][1], closeTo(0.05449744, 0.00000001));
        assertThat(probabilities[0][2], closeTo(0.89619123, 0.00000001));
        assertThat(probabilities[1][0], closeTo(0.99426607, 0.00000001));
        assertThat(probabilities[1][1], closeTo(0.00301019, 0.00000001));
        assertThat(probabilities[1][2], closeTo(0.00272374, 0.00000001));
    }

    public void testConvertToProbabilitiesBySoftMax_GivenRandom() {
        double[][] scores = new double[100][100];
        for (int i = 0; i < scores.length; i++) {
            for (int j = 0; j < scores[i].length; j++) {
                scores[i][j] = randomDoubleBetween(-10, 10, true);
            }
        }

        double[][] probabilities = NerResultProcessor.convertToProbabilitesBySoftMax(scores);

        // Assert invariants that
        //   1. each row sums to 1
        //   2. all values are in [0-1]
        assertThat(probabilities.length, equalTo(scores.length));
        for (int i = 0; i < probabilities.length; i++) {
            assertThat(probabilities[i].length, equalTo(scores[i].length));
            double rowSum = MovingFunctions.sum(probabilities[i]);
            assertThat(rowSum, closeTo(1.0, 0.01));
            for (int j = 0; j < probabilities[i].length; j++) {
                assertThat(probabilities[i][j], greaterThanOrEqualTo(0.0));
                assertThat(probabilities[i][j], lessThanOrEqualTo(1.0));
            }
        }
    }

    private static NerResultProcessor createProcessor(List<String> vocab, String input){
        BertTokenizer tokenizer = BertTokenizer.builder(vocab)
            .setDoLowerCase(true)
            .build();
        TokenizationResult tokenizationResult = tokenizer.tokenize(input, false);
        return new NerResultProcessor(tokenizationResult);
    }
}
