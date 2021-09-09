/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.preprocessing;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.contains;


public class NGramTests extends PreProcessingTests<NGram> {

    @Override
    protected NGram doParseInstance(XContentParser parser) throws IOException {
        return lenient ?
            NGram.fromXContentLenient(parser, PreProcessor.PreProcessorParseContext.DEFAULT) :
            NGram.fromXContentStrict(parser, PreProcessor.PreProcessorParseContext.DEFAULT);
    }

    @Override
    protected NGram createTestInstance() {
        return createRandom();
    }

    public static NGram createRandom() {
        return createRandom(randomBoolean() ? randomBoolean() : null);
    }

    public static NGram createRandom(Boolean isCustom) {
        int possibleLength = randomIntBetween(1, 10);
        return new NGram(
            randomAlphaOfLength(10),
            IntStream.generate(() -> randomIntBetween(1, Math.min(possibleLength, 5))).limit(5).boxed().collect(Collectors.toList()),
            randomBoolean() ? null : randomIntBetween(0, 10),
            randomBoolean() ? null : possibleLength,
            isCustom,
            randomBoolean() ? null : randomAlphaOfLength(10));
    }

    @Override
    protected Writeable.Reader<NGram> instanceReader() {
        return NGram::new;
    }

    public void testProcessNGramPrefix() {
        String field = "text";
        String fieldValue = "this is the value";
        NGram encoding = new NGram(field, "f", new int[]{1, 4}, 0, 5, false);
        Map<String, Object> fieldValues = randomFieldValues(field, fieldValue);

        Map<String, Matcher<? super Object>> matchers = new HashMap<>();
        matchers.put("f.10", equalTo("t"));
        matchers.put("f.11", equalTo("h"));
        matchers.put("f.12", equalTo("i"));
        matchers.put("f.13", equalTo("s"));
        matchers.put("f.14", equalTo(" "));
        matchers.put("f.40", equalTo("this"));
        matchers.put("f.41", equalTo("his "));
        testProcess(encoding, fieldValues, matchers);
    }

    public void testProcessNGramSuffix() {
        String field = "text";
        String fieldValue = "this is the value";

        NGram encoding = new NGram(field, "f", new int[]{1, 3}, -3, 3, false);
        Map<String, Object> fieldValues = randomFieldValues(field, fieldValue);
        Map<String, Matcher<? super Object>> matchers = new HashMap<>();
        matchers.put("f.10", equalTo("l"));
        matchers.put("f.11", equalTo("u"));
        matchers.put("f.12", equalTo("e"));
        matchers.put("f.30", equalTo("lue"));
        matchers.put("f.31", is(nullValue()));
        testProcess(encoding, fieldValues, matchers);
    }

    public void testProcessNGramInfix() {
        String field = "text";
        String fieldValue = "this is the value";

        NGram encoding = new NGram(field, "f", new int[]{1, 3}, 3, 3, false);
        Map<String, Object> fieldValues = randomFieldValues(field, fieldValue);
        Map<String, Matcher<? super Object>> matchers = new HashMap<>();
        matchers.put("f.10", equalTo("s"));
        matchers.put("f.11", equalTo(" "));
        matchers.put("f.12", equalTo("i"));
        matchers.put("f.30", equalTo("s i"));
        matchers.put("f.31", is(nullValue()));
        testProcess(encoding, fieldValues, matchers);
    }

    public void testProcessNGramLengthOverrun() {
        String field = "text";
        String fieldValue = "this is the value";

        NGram encoding = new NGram(field, "f", new int[]{1, 3}, 12, 10, false);
        Map<String, Object> fieldValues = randomFieldValues(field, fieldValue);
        Map<String, Matcher<? super Object>> matchers = new HashMap<>();
        matchers.put("f.10", equalTo("v"));
        matchers.put("f.11", equalTo("a"));
        matchers.put("f.12", equalTo("l"));
        matchers.put("f.13", equalTo("u"));
        matchers.put("f.14", equalTo("e"));
        matchers.put("f.30", equalTo("val"));
        matchers.put("f.31", equalTo("alu"));
        matchers.put("f.32", equalTo("lue"));
        testProcess(encoding, fieldValues, matchers);
    }

    public void testInputOutputFields() {
        String field = randomAlphaOfLength(10);
        NGram encoding = new NGram(field, "f", new int[]{1, 4}, 0, 5, false);
        assertThat(encoding.inputFields(), containsInAnyOrder(field));
        assertThat(encoding.outputFields(),
            contains("f.10", "f.11","f.12","f.13","f.14","f.40", "f.41"));

        encoding = new NGram(field, Arrays.asList(1, 4), 0, 5, false, null);
        assertThat(encoding.inputFields(), containsInAnyOrder(field));
        assertThat(encoding.outputFields(),
            contains(
                "ngram_0_5.10",
                "ngram_0_5.11",
                "ngram_0_5.12",
                "ngram_0_5.13",
                "ngram_0_5.14",
                "ngram_0_5.40",
                "ngram_0_5.41"));
    }

}
