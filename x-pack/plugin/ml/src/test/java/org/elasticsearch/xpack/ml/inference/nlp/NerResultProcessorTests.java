/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.inference.deployment.PyTorchResult;
import org.elasticsearch.xpack.core.ml.inference.results.NerResults;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
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

    public void testGroupTaggedTokens() {
        List<NerResultProcessor.TaggedToken> tokens = new ArrayList<>();
        tokens.add(new NerResultProcessor.TaggedToken("Hi", NerProcessor.IobTag.O, 1.0));
        tokens.add(new NerResultProcessor.TaggedToken("Sarah", NerProcessor.IobTag.B_PER, 1.0));
        tokens.add(new NerResultProcessor.TaggedToken("Jessica", NerProcessor.IobTag.I_PER, 1.0));
        tokens.add(new NerResultProcessor.TaggedToken("I", NerProcessor.IobTag.O, 1.0));
        tokens.add(new NerResultProcessor.TaggedToken("live", NerProcessor.IobTag.O, 1.0));
        tokens.add(new NerResultProcessor.TaggedToken("in", NerProcessor.IobTag.O, 1.0));
        tokens.add(new NerResultProcessor.TaggedToken("Manchester", NerProcessor.IobTag.B_LOC, 1.0));
        tokens.add(new NerResultProcessor.TaggedToken("and", NerProcessor.IobTag.O, 1.0));
        tokens.add(new NerResultProcessor.TaggedToken("work", NerProcessor.IobTag.O, 1.0));
        tokens.add(new NerResultProcessor.TaggedToken("for", NerProcessor.IobTag.O, 1.0));
        tokens.add(new NerResultProcessor.TaggedToken("Elastic", NerProcessor.IobTag.B_ORG, 1.0));

        List<NerResults.EntityGroup> entityGroups = NerResultProcessor.groupTaggedTokens(tokens);
        assertThat(entityGroups, hasSize(3));
        assertThat(entityGroups.get(0).getLabel(), equalTo("person"));
        assertThat(entityGroups.get(0).getWord(), equalTo("Sarah Jessica"));
        assertThat(entityGroups.get(1).getLabel(), equalTo("location"));
        assertThat(entityGroups.get(1).getWord(), equalTo("Manchester"));
        assertThat(entityGroups.get(2).getLabel(), equalTo("organisation"));
        assertThat(entityGroups.get(2).getWord(), equalTo("Elastic"));
    }

    public void testGroupTaggedTokens_GivenNoEntities() {
        List<NerResultProcessor.TaggedToken> tokens = new ArrayList<>();
        tokens.add(new NerResultProcessor.TaggedToken("Hi", NerProcessor.IobTag.O, 1.0));
        tokens.add(new NerResultProcessor.TaggedToken("there", NerProcessor.IobTag.O, 1.0));

        List<NerResults.EntityGroup> entityGroups = NerResultProcessor.groupTaggedTokens(tokens);
        assertThat(entityGroups, is(empty()));
    }

    public void testGroupTaggedTokens_GivenConsecutiveEntities() {
        List<NerResultProcessor.TaggedToken> tokens = new ArrayList<>();
        tokens.add(new NerResultProcessor.TaggedToken("Rita", NerProcessor.IobTag.B_PER, 1.0));
        tokens.add(new NerResultProcessor.TaggedToken("Sue", NerProcessor.IobTag.B_PER, 1.0));
        tokens.add(new NerResultProcessor.TaggedToken("and", NerProcessor.IobTag.O, 1.0));
        tokens.add(new NerResultProcessor.TaggedToken("Bob", NerProcessor.IobTag.B_PER, 1.0));
        tokens.add(new NerResultProcessor.TaggedToken("to", NerProcessor.IobTag.O, 1.0));

        List<NerResults.EntityGroup> entityGroups = NerResultProcessor.groupTaggedTokens(tokens);
        assertThat(entityGroups, hasSize(3));
        assertThat(entityGroups.get(0).getLabel(), equalTo("person"));
        assertThat(entityGroups.get(0).getWord(), equalTo("Rita"));
        assertThat(entityGroups.get(1).getLabel(), equalTo("person"));
        assertThat(entityGroups.get(1).getWord(), equalTo("Sue"));
        assertThat(entityGroups.get(2).getLabel(), equalTo("person"));
        assertThat(entityGroups.get(2).getWord(), equalTo("Bob"));
    }

    public void testGroupTaggedTokens_GivenConsecutiveContinuingEntities() {
        List<NerResultProcessor.TaggedToken> tokens = new ArrayList<>();
        tokens.add(new NerResultProcessor.TaggedToken("FirstName", NerProcessor.IobTag.B_PER, 1.0));
        tokens.add(new NerResultProcessor.TaggedToken("SecondName", NerProcessor.IobTag.I_PER, 1.0));
        tokens.add(new NerResultProcessor.TaggedToken("NextPerson", NerProcessor.IobTag.B_PER, 1.0));
        tokens.add(new NerResultProcessor.TaggedToken("NextPersonSecondName", NerProcessor.IobTag.I_PER, 1.0));
        tokens.add(new NerResultProcessor.TaggedToken("something_else", NerProcessor.IobTag.B_ORG, 1.0));

        List<NerResults.EntityGroup> entityGroups = NerResultProcessor.groupTaggedTokens(tokens);
        assertThat(entityGroups, hasSize(3));
        assertThat(entityGroups.get(0).getLabel(), equalTo("person"));
        assertThat(entityGroups.get(0).getWord(), equalTo("FirstName SecondName"));
        assertThat(entityGroups.get(1).getLabel(), equalTo("person"));
        assertThat(entityGroups.get(1).getWord(), equalTo("NextPerson NextPersonSecondName"));
        assertThat(entityGroups.get(2).getLabel(), equalTo("organisation"));
    }

    private static NerResultProcessor createProcessor(List<String> vocab, String input){
        BertTokenizer tokenizer = BertTokenizer.builder(vocab)
            .setDoLowerCase(true)
            .setWithSpecialTokens(false)
            .build();
        BertTokenizer.TokenizationResult tokenizationResult = tokenizer.tokenize(input);
        return new NerResultProcessor(tokenizationResult);
    }
}
