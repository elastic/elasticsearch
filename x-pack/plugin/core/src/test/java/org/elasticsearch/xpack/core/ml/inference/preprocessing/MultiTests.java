/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.preprocessing;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.function.Predicate;

import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;

public class MultiTests extends PreProcessingTests<Multi> {

    @Override
    protected Multi doParseInstance(XContentParser parser) throws IOException {
        return lenient ?
            Multi.fromXContentLenient(parser, PreProcessor.PreProcessorParseContext.DEFAULT) :
            Multi.fromXContentStrict(parser, PreProcessor.PreProcessorParseContext.DEFAULT);
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> !field.isEmpty();
    }

    @Override
    protected Multi createTestInstance() {
        return createRandom();
    }

    public static Multi createRandom() {
        return createRandom(randomBoolean() ? null : randomBoolean());
    }

    public static Multi createRandom(Boolean isCustom) {
        return new Multi(
            randomArray(
                2,
                10,
                PreProcessor[]::new,
                () -> randomFrom(
                    FrequencyEncodingTests.createRandom(isCustom),
                    TargetMeanEncodingTests.createRandom(isCustom),
                    OneHotEncodingTests.createRandom(isCustom),
                    NGramTests.createRandom(isCustom)
                )
            ),
            isCustom
        );
    }

    @Override
    protected Writeable.Reader<Multi> instanceReader() {
        return Multi::new;
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
