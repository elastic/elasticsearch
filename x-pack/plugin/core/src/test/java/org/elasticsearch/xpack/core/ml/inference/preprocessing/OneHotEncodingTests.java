/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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

import static org.hamcrest.Matchers.equalTo;

public class OneHotEncodingTests extends PreProcessingTests<OneHotEncoding> {

    @Override
    protected OneHotEncoding doParseInstance(XContentParser parser) throws IOException {
        return lenient ? OneHotEncoding.fromXContentLenient(parser) : OneHotEncoding.fromXContentStrict(parser);
    }

    @Override
    protected OneHotEncoding createTestInstance() {
        return createRandom();
    }

    public static OneHotEncoding createRandom() {
        int valuesSize = randomIntBetween(1, 10);
        Map<String, String> valueMap = new HashMap<>();
        for (int i = 0; i < valuesSize; i++) {
            valueMap.put(randomAlphaOfLength(10), randomAlphaOfLength(10));
        }
        return new OneHotEncoding(randomAlphaOfLength(10), valueMap);
    }

    @Override
    protected Writeable.Reader<OneHotEncoding> instanceReader() {
        return OneHotEncoding::new;
    }

    public void testProcessWithFieldPresent() {
        String field = "categorical";
        List<Object> values = Arrays.asList("foo", "bar", "foobar", "baz", "farequote", 1.0);
        Map<String, String> valueMap = values.stream().collect(Collectors.toMap(Object::toString, v -> "Column_" + v.toString()));
        OneHotEncoding encoding = new OneHotEncoding(field, valueMap);
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

    public void testProcessWithNestedField() {
        String field = "categorical.child";
        List<Object> values = Arrays.asList("foo", "bar", "foobar", "baz", "farequote", 1.5);
        Map<String, String> valueMap = values.stream().collect(Collectors.toMap(Object::toString, v -> "Column_" + v.toString()));
        OneHotEncoding encoding = new OneHotEncoding(field, valueMap);
        Map<String, Object> fieldValues = new HashMap<>() {{
            put("categorical", new HashMap<>(){{
                put("child", "farequote");
            }});
        }};

        encoding.process(fieldValues);
        assertThat(fieldValues.get("Column_farequote"), equalTo(1));
    }

}
