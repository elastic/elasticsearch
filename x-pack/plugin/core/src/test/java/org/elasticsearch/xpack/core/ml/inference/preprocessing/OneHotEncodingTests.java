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
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class OneHotEncodingTests extends PreProcessingTests<OneHotEncoding> {

    @Override
    protected OneHotEncoding doParseInstance(XContentParser parser) throws IOException {
        return lenient ?
            OneHotEncoding.fromXContentLenient(parser, PreProcessor.PreProcessorParseContext.DEFAULT) :
            OneHotEncoding.fromXContentStrict(parser, PreProcessor.PreProcessorParseContext.DEFAULT);
    }

    @Override
    protected OneHotEncoding createTestInstance() {
        return createRandom();
    }

    public static OneHotEncoding createRandom() {
        return createRandom(randomBoolean() ? randomBoolean() : null);
    }

    public static OneHotEncoding createRandom(Boolean isCustom) {
        return createRandom(isCustom, randomAlphaOfLength(10));
    }

    public static OneHotEncoding createRandom(Boolean isCustom, String inputField) {
        int valuesSize = randomIntBetween(1, 10);
        Map<String, String> valueMap = new HashMap<>();
        for (int i = 0; i < valuesSize; i++) {
            valueMap.put(randomAlphaOfLength(10), randomAlphaOfLength(10));
        }
        return new OneHotEncoding(inputField,
            valueMap,
            isCustom);
    }

    @Override
    protected Writeable.Reader<OneHotEncoding> instanceReader() {
        return OneHotEncoding::new;
    }

    public void testProcessWithFieldPresent() {
        String field = "categorical";
        List<Object> values = Arrays.asList("foo", "bar", "foobar", "baz", "farequote", 1.0);
        Map<String, String> valueMap = values.stream().collect(Collectors.toMap(Object::toString, v -> "Column_" + v.toString()));
        OneHotEncoding encoding = new OneHotEncoding(field, valueMap, false);
        Object fieldValue = randomFrom(values);
        Map<String, Object> fieldValues = randomFieldValues(field, fieldValue);

        Map<String, Matcher<? super Object>> matchers = values.stream().map(v -> "Column_" + v)
            .collect(Collectors.toMap(
                Function.identity(),
                v -> v.equals("Column_" + fieldValue) ? equalTo(1) : equalTo(0)));

        fieldValues.put(field, fieldValue);
        testProcess(encoding, fieldValues, matchers);

        // Test where the value is some unknown Value
        fieldValues = randomFieldValues(field, "unknownValue");
        matchers.put("Column_" + fieldValue, equalTo(0));
        testProcess(encoding, fieldValues, matchers);
    }

    public void testInputOutputFields() {
        String field = randomAlphaOfLength(10);
        List<Object> values = Arrays.asList("foo", "bar", "foobar", "baz", "farequote", 1.0);
        Map<String, String> valueMap = values.stream().collect(Collectors.toMap(Object::toString, v -> "Column_" + v.toString()));
        OneHotEncoding encoding = new OneHotEncoding(field, valueMap, false);
        assertThat(encoding.inputFields(), containsInAnyOrder(field));
        assertThat(encoding.outputFields(),
            containsInAnyOrder(values.stream().map(v -> "Column_" + v.toString()).toArray(String[]::new)));
    }

}
