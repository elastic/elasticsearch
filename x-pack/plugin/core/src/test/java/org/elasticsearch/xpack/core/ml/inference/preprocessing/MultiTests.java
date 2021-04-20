/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.preprocessing;

import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;

public class MultiTests extends PreProcessingTests<Multi> {

    @Override
    protected Multi doParseInstance(XContentParser parser) throws IOException {
        return lenient ?
            Multi.fromXContentLenient(parser, PreProcessor.PreProcessorParseContext.DEFAULT) :
            Multi.fromXContentStrict(parser, PreProcessor.PreProcessorParseContext.DEFAULT);
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.isEmpty() == false;
    }

    @Override
    protected Multi createTestInstance() {
        return createRandom();
    }

    public static Multi createRandom() {
        return createRandom(randomBoolean() ? null : randomBoolean());
    }

    public static Multi createRandom(Boolean isCustom) {
        final PreProcessor[] processors;
        if (isCustom == null || isCustom == false) {
            NGram nGram = NGramTests.createRandom(isCustom);
            List<PreProcessor> preProcessorList = new ArrayList<>();
            preProcessorList.add(nGram);
            Stream.generate(() -> randomFrom(
                FrequencyEncodingTests.createRandom(isCustom, randomFrom(nGram.outputFields())),
                TargetMeanEncodingTests.createRandom(isCustom, randomFrom(nGram.outputFields())),
                OneHotEncodingTests.createRandom(isCustom, randomFrom(nGram.outputFields()))
            )).limit(randomIntBetween(1, 10)).forEach(preProcessorList::add);
            processors = preProcessorList.toArray(PreProcessor[]::new);
        } else {
            processors = randomArray(
                2,
                10,
                PreProcessor[]::new,
                () -> randomFrom(
                    FrequencyEncodingTests.createRandom(isCustom),
                    TargetMeanEncodingTests.createRandom(isCustom),
                    OneHotEncodingTests.createRandom(isCustom),
                    NGramTests.createRandom(isCustom)
                )
            );
        }
        return new Multi(processors, isCustom);
    }

    @Override
    protected Writeable.Reader<Multi> instanceReader() {
        return Multi::new;
    }

    public void testReverseLookup() {
        String field = "text";
        NGram nGram = new NGram(field, Collections.singletonList(1), 0, 2, null, "f");
        OneHotEncoding oneHotEncoding = new OneHotEncoding("f.10",
            MapBuilder.<String, String>newMapBuilder()
                .put("a", "has_a")
                .put("b", "has_b")
                .map(),
            true);
        Multi multi = new Multi(new PreProcessor[]{nGram, oneHotEncoding}, true);
        assertThat(multi.reverseLookup(), allOf(hasEntry("has_a", field), hasEntry("has_b", field), hasEntry("f.11", field)));

        OneHotEncoding oneHotEncodingOutside = new OneHotEncoding("some_other",
            MapBuilder.<String, String>newMapBuilder()
                .put("a", "has_3_a")
                .put("b", "has_3_b")
                .map(),
            true);
        multi = new Multi(new PreProcessor[]{nGram, oneHotEncoding, oneHotEncodingOutside}, true);
        expectThrows(IllegalArgumentException.class, multi::reverseLookup);
    }

    public void testProcessWithFieldPresent() {
        String field = "text";
        NGram nGram = new NGram(field, Collections.singletonList(1), 0, 2, null, "f");
        OneHotEncoding oneHotEncoding1 = new OneHotEncoding("f.10",
            MapBuilder.<String, String>newMapBuilder()
                .put("a", "has_a")
                .put("b", "has_b")
                .map(),
            true);
        OneHotEncoding oneHotEncoding2 = new OneHotEncoding("f.11",
            MapBuilder.<String, String>newMapBuilder()
                .put("a", "has_2_a")
                .put("b", "has_2_b")
                .map(),
            true);
        Multi multi = new Multi(new PreProcessor[]{nGram, oneHotEncoding1, oneHotEncoding2}, true);
        Map<String, Object> fields = randomFieldValues("text", "cat");
        multi.process(fields);
        assertThat(fields, hasEntry("has_a", 0));
        assertThat(fields, hasEntry("has_b", 0));
        assertThat(fields, hasEntry("has_2_a", 1));
        assertThat(fields, hasEntry("has_2_b", 0));
    }

    public void testInputOutputFields() {
        String field = "text";
        NGram nGram = new NGram(field, Collections.singletonList(1), 0, 3, null, "f");
        OneHotEncoding oneHotEncoding1 = new OneHotEncoding("f.10",
            MapBuilder.<String, String>newMapBuilder()
                .put("a", "has_a")
                .put("b", "has_b")
                .map(),
            true);
        OneHotEncoding oneHotEncoding2 = new OneHotEncoding("f.11",
            MapBuilder.<String, String>newMapBuilder()
                .put("a", "has_2_a")
                .put("b", "has_2_b")
                .map(),
            true);
        OneHotEncoding oneHotEncoding3 = new OneHotEncoding("some_other",
            MapBuilder.<String, String>newMapBuilder()
                .put("a", "has_3_a")
                .put("b", "has_3_b")
                .map(),
            true);
        Multi multi = new Multi(new PreProcessor[]{nGram, oneHotEncoding1, oneHotEncoding2, oneHotEncoding3}, true);
        assertThat(multi.inputFields(), contains(field, "some_other"));
        assertThat(multi.outputFields(),
            contains(
                "f.12",
                "has_a",
                "has_b",
                "has_2_a",
                "has_2_b",
                "has_3_a",
                "has_3_b")
        );
        assertThat(multi.getOutputFieldType("f.12"), equalTo("text"));
        for (String fieldName : new String[]{"has_a", "has_b", "has_2_a", "has_2_b", "has_3_a", "has_3_b"}) {
            assertThat(multi.getOutputFieldType(fieldName), equalTo("integer"));
        }
    }

}
