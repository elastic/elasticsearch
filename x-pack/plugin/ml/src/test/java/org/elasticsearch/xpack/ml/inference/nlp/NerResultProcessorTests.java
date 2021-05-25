/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.deployment.PyTorchResult;
import org.elasticsearch.xpack.core.ml.inference.results.NerResults;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizer;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class NerResultProcessorTests extends ESTestCase {

    public void testProcessResults_GivenNoTokens() {
        NerResultProcessor processor = createProcessor(Collections.emptyList(), "");
        NerResults result = (NerResults) processor.processResult(new PyTorchResult("test", null, null));
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
        NerResults result = (NerResults) processor.processResult(new PyTorchResult("1", scores, null));

        assertThat(result.getEntityGroups().size(), equalTo(2));
        assertThat(result.getEntityGroups().get(0).getWord(), equalTo("elasticsearch"));
        assertThat(result.getEntityGroups().get(0).getLabel(), equalTo(NerProcessor.Entity.ORGANISATION.toString()));
        assertThat(result.getEntityGroups().get(1).getWord(), equalTo("london"));
        assertThat(result.getEntityGroups().get(1).getLabel(), equalTo(NerProcessor.Entity.LOCATION.toString()));
    }

    private static NerResultProcessor createProcessor(List<String> vocab, String input){
        BertTokenizer tokenizer = BertTokenizer.builder(vocab)
            .setDoLowerCase(true)
            .build();
        BertTokenizer.TokenizationResult tokenizationResult = tokenizer.tokenize(input, false);
        return new NerResultProcessor(tokenizationResult);
    }
}
