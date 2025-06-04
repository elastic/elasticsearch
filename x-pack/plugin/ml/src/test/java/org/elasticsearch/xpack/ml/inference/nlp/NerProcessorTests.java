/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.results.NerResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.BertTokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NerConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.Tokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.VocabularyConfig;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizer;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.DelimitedToken;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.TokenizationResult;
import org.elasticsearch.xpack.ml.inference.pytorch.results.PyTorchInferenceResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BasicTokenFilterTests.basicTokenize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class NerProcessorTests extends ESTestCase {

    public void testBuildIobMap_Reordered() {
        NerProcessor.IobTag[] tags = new NerProcessor.IobTag[] {
            NerProcessor.IobTag.fromTag("I_MISC"),
            NerProcessor.IobTag.fromTag("O"),
            NerProcessor.IobTag.fromTag("B_MISC"),
            NerProcessor.IobTag.fromTag("I_PER") };

        List<String> classLabels = Arrays.stream(tags).map(NerProcessor.IobTag::toString).collect(Collectors.toList());
        NerProcessor.IobTag[] map = NerProcessor.buildIobMap(classLabels);
        assertArrayEquals(tags, map);
    }

    public void testValidate_DuplicateLabels() {
        NerProcessor.IobTag[] tags = new NerProcessor.IobTag[] {
            NerProcessor.IobTag.fromTag("I_MISC"),
            NerProcessor.IobTag.fromTag("B_MISC"),
            NerProcessor.IobTag.fromTag("B_MISC"),
            NerProcessor.IobTag.fromTag("O"), };

        List<String> classLabels = Arrays.stream(tags).map(NerProcessor.IobTag::toString).collect(Collectors.toList());
        NerConfig nerConfig = new NerConfig(new VocabularyConfig("test-index"), null, classLabels, null);

        ValidationException ve = expectThrows(ValidationException.class, () -> new NerProcessor(mock(BertTokenizer.class), nerConfig));
        assertThat(
            ve.getMessage(),
            containsString("the classification label [B_MISC] is duplicated in the list [I_MISC, B_MISC, B_MISC, O]")
        );
    }

    public void testProcessResults_GivenNoTokens() {
        NerProcessor.NerResultProcessor processor = new NerProcessor.NerResultProcessor(NerProcessor.DEFAULT_IOB_TAGS, null, false);
        TokenizationResult tokenization = tokenize(List.of(BertTokenizer.PAD_TOKEN, BertTokenizer.UNKNOWN_TOKEN), "");

        var e = expectThrows(
            ElasticsearchStatusException.class,
            () -> processor.processResult(tokenization, new PyTorchInferenceResult(null), false)
        );
        assertThat(e, instanceOf(ElasticsearchStatusException.class));
    }

    public void testProcessResultsWithSpecialTokens() {
        NerProcessor.NerResultProcessor processor = new NerProcessor.NerResultProcessor(NerProcessor.DEFAULT_IOB_TAGS, null, true);
        try (
            BertTokenizer tokenizer = BertTokenizer.builder(
                List.of(
                    "el",
                    "##astic",
                    "##search",
                    "many",
                    "use",
                    "in",
                    "london",
                    BertTokenizer.PAD_TOKEN,
                    BertTokenizer.UNKNOWN_TOKEN,
                    BertTokenizer.SEPARATOR_TOKEN,
                    BertTokenizer.CLASS_TOKEN
                ),
                new BertTokenization(true, true, null, Tokenization.Truncate.NONE, -1)
            ).build()
        ) {
            TokenizationResult tokenization = tokenizer.buildTokenizationResult(
                List.of(tokenizer.tokenize("Many use Elasticsearch in London", Tokenization.Truncate.NONE, -1, 1, null).get(0))
            );

            double[][][] scores = {
                {
                    { 7, 0, 0, 0, 0, 0, 0, 0, 0 }, // cls
                    { 7, 0, 0, 0, 0, 0, 0, 0, 0 }, // many
                    { 7, 0, 0, 0, 0, 0, 0, 0, 0 }, // use
                    { 0.01, 0.01, 0, 0.01, 0, 7, 0, 3, 0 }, // el
                    { 0.01, 0.01, 0, 0, 0, 0, 0, 0, 0 }, // ##astic
                    { 0, 0, 0, 0, 0, 0, 0, 0, 0 }, // ##search
                    { 0, 0, 0, 0, 0, 0, 0, 0, 0 }, // in
                    { 0, 0, 0, 0, 0, 0, 0, 6, 0 }, // london
                    { 7, 0, 0, 0, 0, 0, 0, 0, 0 } // sep
                } };
            NerResults result = (NerResults) processor.processResult(tokenization, new PyTorchInferenceResult(scores), false);

            assertThat(result.getAnnotatedResult(), equalTo("Many use [Elasticsearch](ORG&Elasticsearch) in [London](LOC&London)"));
            assertThat(result.getEntityGroups().size(), equalTo(2));
            assertThat(result.getEntityGroups().get(0).getEntity(), equalTo("elasticsearch"));
            assertThat(result.getEntityGroups().get(0).getClassName(), equalTo("ORG"));
            assertThat(result.getEntityGroups().get(1).getEntity(), equalTo("london"));
            assertThat(result.getEntityGroups().get(1).getClassName(), equalTo("LOC"));
        }
    }

    public void testProcessResults() {
        NerProcessor.NerResultProcessor processor = new NerProcessor.NerResultProcessor(NerProcessor.DEFAULT_IOB_TAGS, null, true);
        TokenizationResult tokenization = tokenize(
            Arrays.asList("el", "##astic", "##search", "many", "use", "in", "london", BertTokenizer.PAD_TOKEN, BertTokenizer.UNKNOWN_TOKEN),
            "Many use Elasticsearch in London"
        );

        double[][][] scores = {
            {
                { 7, 0, 0, 0, 0, 0, 0, 0, 0 }, // many
                { 7, 0, 0, 0, 0, 0, 0, 0, 0 }, // use
                { 0.01, 0.01, 0, 0.01, 0, 7, 0, 3, 0 }, // el
                { 0.01, 0.01, 0, 0, 0, 0, 0, 0, 0 }, // ##astic
                { 0, 0, 0, 0, 0, 0, 0, 0, 0 }, // ##search
                { 0, 0, 0, 0, 0, 0, 0, 0, 0 }, // in
                { 0, 0, 0, 0, 0, 0, 0, 6, 0 } // london
            } };
        NerResults result = (NerResults) processor.processResult(tokenization, new PyTorchInferenceResult(scores), false);

        assertThat(result.getAnnotatedResult(), equalTo("Many use [Elasticsearch](ORG&Elasticsearch) in [London](LOC&London)"));
        assertThat(result.getEntityGroups().size(), equalTo(2));
        assertThat(result.getEntityGroups().get(0).getEntity(), equalTo("elasticsearch"));
        assertThat(result.getEntityGroups().get(0).getClassName(), equalTo("ORG"));
        assertThat(result.getEntityGroups().get(1).getEntity(), equalTo("london"));
        assertThat(result.getEntityGroups().get(1).getClassName(), equalTo("LOC"));
    }

    public void testProcessResults_withIobMap() {

        NerProcessor.IobTag[] iobMap = new NerProcessor.IobTag[] {
            NerProcessor.IobTag.fromTag("B_LOC"),
            NerProcessor.IobTag.fromTag("I_LOC"),
            NerProcessor.IobTag.fromTag("B_MISC"),
            NerProcessor.IobTag.fromTag("I_MISC"),
            NerProcessor.IobTag.fromTag("B_PER"),
            NerProcessor.IobTag.fromTag("I_PER"),
            NerProcessor.IobTag.fromTag("B_ORG"),
            NerProcessor.IobTag.fromTag("I_ORG"),
            NerProcessor.IobTag.fromTag("O") };

        NerProcessor.NerResultProcessor processor = new NerProcessor.NerResultProcessor(iobMap, null, true);
        TokenizationResult tokenization = tokenize(
            Arrays.asList("el", "##astic", "##search", "many", "use", "in", "london", BertTokenizer.UNKNOWN_TOKEN, BertTokenizer.PAD_TOKEN),
            "Elasticsearch in London"
        );

        double[][][] scores = {
            {
                { 0.01, 0.01, 0, 0.01, 0, 0, 7, 3, 0 }, // el
                { 0.01, 0.01, 0, 0, 0, 0, 0, 0, 0 }, // ##astic
                { 0, 0, 0, 0, 0, 0, 0, 0, 0 }, // ##search
                { 0, 0, 0, 0, 0, 0, 0, 0, 5 }, // in
                { 6, 0, 0, 0, 0, 0, 0, 0, 0 } // london
            } };
        NerResults result = (NerResults) processor.processResult(tokenization, new PyTorchInferenceResult(scores), false);

        assertThat(result.getAnnotatedResult(), equalTo("[Elasticsearch](ORG&Elasticsearch) in [London](LOC&London)"));
        assertThat(result.getEntityGroups().size(), equalTo(2));
        assertThat(result.getEntityGroups().get(0).getEntity(), equalTo("elasticsearch"));
        assertThat(result.getEntityGroups().get(0).getClassName(), equalTo("ORG"));
        assertThat(result.getEntityGroups().get(1).getEntity(), equalTo("london"));
        assertThat(result.getEntityGroups().get(1).getClassName(), equalTo("LOC"));
    }

    public void testProcessResults_withCustomIobMap() {

        NerProcessor.IobTag[] iobMap = new NerProcessor.IobTag[] {
            NerProcessor.IobTag.fromTag("B_LOC"),
            NerProcessor.IobTag.fromTag("I_LOC"),
            NerProcessor.IobTag.fromTag("B_SOFTWARE"),
            NerProcessor.IobTag.fromTag("I_SOFTWARE"),
            NerProcessor.IobTag.fromTag("O") };

        NerProcessor.NerResultProcessor processor = new NerProcessor.NerResultProcessor(iobMap, null, true);
        TokenizationResult tokenization = tokenize(
            Arrays.asList("el", "##astic", "##search", "many", "use", "in", "london", BertTokenizer.UNKNOWN_TOKEN, BertTokenizer.PAD_TOKEN),
            "Elasticsearch in London"
        );

        double[][][] scores = {
            {
                { 0.01, 0.01, 7, 3, 0 }, // el
                { 0.01, 0.01, 0, 0, 0 }, // ##astic
                { 0, 0, 0, 0, 0 }, // ##search
                { 0, 0, 0, 0, 5 }, // in
                { 6, 0, 0, 0, 0 } // london
            } };
        NerResults result = (NerResults) processor.processResult(tokenization, new PyTorchInferenceResult(scores), false);

        assertThat(result.getAnnotatedResult(), equalTo("[Elasticsearch](SOFTWARE&Elasticsearch) in [London](LOC&London)"));
        assertThat(result.getEntityGroups().size(), equalTo(2));
        assertThat(result.getEntityGroups().get(0).getEntity(), equalTo("elasticsearch"));
        assertThat(result.getEntityGroups().get(0).getClassName(), equalTo("SOFTWARE"));
        assertThat(result.getEntityGroups().get(1).getEntity(), equalTo("london"));
        assertThat(result.getEntityGroups().get(1).getClassName(), equalTo("LOC"));
    }

    public void testGroupTaggedTokens() throws IOException {
        String input = "Hi Sarah Jessica, I live in Manchester and work for Elastic";
        List<DelimitedToken> tokens = basicTokenize(randomBoolean(), randomBoolean(), List.of(), input);
        assertThat(tokens, hasSize(12));

        List<NerProcessor.NerResultProcessor.TaggedToken> taggedTokens = new ArrayList<>();
        int i = 0;
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(i++), NerProcessor.IobTag.fromTag("O"), 1.0));
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(i++), NerProcessor.IobTag.fromTag("B_PER"), 1.0));
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(i++), NerProcessor.IobTag.fromTag("I_PER"), 1.0));
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(i++), NerProcessor.IobTag.fromTag("O"), 1.0));
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(i++), NerProcessor.IobTag.fromTag("O"), 1.0));
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(i++), NerProcessor.IobTag.fromTag("O"), 1.0));
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(i++), NerProcessor.IobTag.fromTag("O"), 1.0));
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(i++), NerProcessor.IobTag.fromTag("B_LOC"), 1.0));
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(i++), NerProcessor.IobTag.fromTag("O"), 1.0));
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(i++), NerProcessor.IobTag.fromTag("O"), 1.0));
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(i++), NerProcessor.IobTag.fromTag("O"), 1.0));
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(i), NerProcessor.IobTag.fromTag("B_ORG"), 1.0));

        List<NerResults.EntityGroup> entityGroups = NerProcessor.NerResultProcessor.groupTaggedTokens(taggedTokens, input);
        assertThat(entityGroups, hasSize(3));
        assertThat(entityGroups.get(0).getClassName(), equalTo("PER"));
        assertThat(entityGroups.get(0).getEntity(), equalTo("Sarah Jessica"));
        assertThat(entityGroups.get(1).getClassName(), equalTo("LOC"));
        assertThat(entityGroups.get(1).getEntity(), equalTo("Manchester"));
        assertThat(entityGroups.get(2).getClassName(), equalTo("ORG"));
        assertThat(entityGroups.get(2).getEntity(), equalTo("Elastic"));
    }

    public void testGroupTaggedTokens_GivenNoEntities() throws IOException {
        String input = "Hi there";
        List<DelimitedToken> tokens = basicTokenize(randomBoolean(), randomBoolean(), List.of(), input);

        List<NerProcessor.NerResultProcessor.TaggedToken> taggedTokens = new ArrayList<>();
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(0), NerProcessor.IobTag.fromTag("O"), 1.0));
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(1), NerProcessor.IobTag.fromTag("O"), 1.0));

        List<NerResults.EntityGroup> entityGroups = NerProcessor.NerResultProcessor.groupTaggedTokens(taggedTokens, input);
        assertThat(entityGroups, is(empty()));
    }

    public void testGroupTaggedTokens_GivenConsecutiveEntities() throws IOException {
        String input = "Rita, Sue, and Bob too";
        List<DelimitedToken> tokens = basicTokenize(randomBoolean(), randomBoolean(), List.of(), input);

        List<NerProcessor.NerResultProcessor.TaggedToken> taggedTokens = new ArrayList<>();
        int i = 0;
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(i++), NerProcessor.IobTag.fromTag("B_PER"), 1.0));
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(i++), NerProcessor.IobTag.fromTag("O"), 1.0));
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(i++), NerProcessor.IobTag.fromTag("B_PER"), 1.0));
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(i++), NerProcessor.IobTag.fromTag("O"), 1.0));
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(i++), NerProcessor.IobTag.fromTag("O"), 1.0));
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(i++), NerProcessor.IobTag.fromTag("B_PER"), 1.0));
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(i), NerProcessor.IobTag.fromTag("O"), 1.0));

        List<NerResults.EntityGroup> entityGroups = NerProcessor.NerResultProcessor.groupTaggedTokens(taggedTokens, input);
        assertThat(entityGroups, hasSize(3));
        assertThat(entityGroups.get(0).getClassName(), equalTo("PER"));
        assertThat(entityGroups.get(0).getEntity(), equalTo("Rita"));
        assertThat(entityGroups.get(1).getClassName(), equalTo("PER"));
        assertThat(entityGroups.get(1).getEntity(), equalTo("Sue"));
        assertThat(entityGroups.get(2).getClassName(), equalTo("PER"));
        assertThat(entityGroups.get(2).getEntity(), equalTo("Bob"));
    }

    public void testGroupTaggedTokens_GivenConsecutiveContinuingEntities() throws IOException {
        String input = "FirstName SecondName, NextPerson NextPersonSecondName. something_else";
        List<DelimitedToken> tokens = basicTokenize(randomBoolean(), randomBoolean(), List.of(), input);

        List<NerProcessor.NerResultProcessor.TaggedToken> taggedTokens = new ArrayList<>();
        int i = 0;
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(i++), NerProcessor.IobTag.fromTag("B_PER"), 1.0));
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(i++), NerProcessor.IobTag.fromTag("I_PER"), 1.0));
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(i++), NerProcessor.IobTag.fromTag("O"), 1.0));
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(i++), NerProcessor.IobTag.fromTag("B_PER"), 1.0));
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(i++), NerProcessor.IobTag.fromTag("I_PER"), 1.0));
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(i), NerProcessor.IobTag.fromTag("B_ORG"), 1.0));

        List<NerResults.EntityGroup> entityGroups = NerProcessor.NerResultProcessor.groupTaggedTokens(taggedTokens, input);
        assertThat(entityGroups, hasSize(3));
        assertThat(entityGroups.get(0).getClassName(), equalTo("PER"));
        assertThat(entityGroups.get(0).getEntity(), equalTo("FirstName SecondName"));
        assertThat(entityGroups.get(1).getClassName(), equalTo("PER"));
        assertThat(entityGroups.get(1).getEntity(), equalTo("NextPerson NextPersonSecondName"));
        assertThat(entityGroups.get(2).getClassName(), equalTo("ORG"));
    }

    public void testEntityContainsPunctuation() throws IOException {
        String input = "Alexander, my name is Benjamin Trent, I work at Acme Inc..";
        List<DelimitedToken> tokens = basicTokenize(randomBoolean(), randomBoolean(), List.of(), input);

        List<NerProcessor.NerResultProcessor.TaggedToken> taggedTokens = new ArrayList<>();
        int i = 0;
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(i++), NerProcessor.IobTag.fromTag("I_PER"), 1.0));
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(i++), NerProcessor.IobTag.fromTag("O"), 1.0));
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(i++), NerProcessor.IobTag.fromTag("O"), 1.0));
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(i++), NerProcessor.IobTag.fromTag("O"), 1.0));
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(i++), NerProcessor.IobTag.fromTag("O"), 1.0));
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(i++), NerProcessor.IobTag.fromTag("I_PER"), 1.0));
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(i++), NerProcessor.IobTag.fromTag("I_PER"), 1.0));
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(i++), NerProcessor.IobTag.fromTag("O"), 1.0));
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(i++), NerProcessor.IobTag.fromTag("O"), 1.0));
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(i++), NerProcessor.IobTag.fromTag("O"), 1.0));
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(i++), NerProcessor.IobTag.fromTag("O"), 1.0));
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(i++), NerProcessor.IobTag.fromTag("I_ORG"), 1.0));
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(i++), NerProcessor.IobTag.fromTag("I_ORG"), 1.0));
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(i++), NerProcessor.IobTag.fromTag("I_ORG"), 1.0));
        taggedTokens.add(new NerProcessor.NerResultProcessor.TaggedToken(tokens.get(i), NerProcessor.IobTag.fromTag("O"), 1.0));
        assertEquals(tokens.size(), taggedTokens.size());

        List<NerResults.EntityGroup> entityGroups = NerProcessor.NerResultProcessor.groupTaggedTokens(taggedTokens, input);
        assertThat(entityGroups, hasSize(3));
        assertThat(entityGroups.get(0).getClassName(), equalTo("PER"));
        assertThat(entityGroups.get(0).getEntity(), equalTo("Alexander"));
        assertThat(entityGroups.get(1).getClassName(), equalTo("PER"));
        assertThat(entityGroups.get(1).getEntity(), equalTo("Benjamin Trent"));
        assertThat(entityGroups.get(2).getClassName(), equalTo("ORG"));
        assertThat(entityGroups.get(2).getEntity(), equalTo("Acme Inc."));
    }

    public void testAnnotatedTextBuilder() {
        String input = "Alexander, my name is Benjamin Trent, I work at Acme Inc.";
        List<NerResults.EntityGroup> entities = List.of(
            new NerResults.EntityGroup("alexander", "PER", 0.9963429980065166, 0, 9),
            new NerResults.EntityGroup("benjamin trent", "PER", 0.9972042749283819, 22, 36),
            new NerResults.EntityGroup("acme inc", "ORG", 0.9982026600781208, 48, 56)
        );
        assertThat(
            NerProcessor.buildAnnotatedText(input, entities),
            equalTo(
                "[Alexander](PER&Alexander), " + "my name is [Benjamin Trent](PER&Benjamin+Trent), " + "I work at [Acme Inc](ORG&Acme+Inc)."
            )
        );
    }

    public void testAnnotatedTextBuilder_empty() {
        String input = "There are no entities";
        List<NerResults.EntityGroup> entities = List.of();
        assertThat(NerProcessor.buildAnnotatedText(input, entities), equalTo(input));
    }

    private static TokenizationResult tokenize(List<String> vocab, String input) {
        try (
            BertTokenizer tokenizer = BertTokenizer.builder(vocab, new BertTokenization(true, false, null, Tokenization.Truncate.NONE, -1))
                .setDoLowerCase(true)
                .setWithSpecialTokens(false)
                .build()
        ) {
            return tokenizer.buildTokenizationResult(tokenizer.tokenize(input, Tokenization.Truncate.NONE, -1, 0, null));
        }
    }
}
