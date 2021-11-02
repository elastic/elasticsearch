/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.preprocessing;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.XContentParser;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class FrequencyEncodingTests extends PreProcessingTests<FrequencyEncoding> {

    @Override
    protected FrequencyEncoding doParseInstance(XContentParser parser) throws IOException {
        return lenient
            ? FrequencyEncoding.fromXContentLenient(parser, PreProcessor.PreProcessorParseContext.DEFAULT)
            : FrequencyEncoding.fromXContentStrict(parser, PreProcessor.PreProcessorParseContext.DEFAULT);
    }

    @Override
    protected FrequencyEncoding createTestInstance() {
        return createRandom();
    }

    public static FrequencyEncoding createRandom() {
        return createRandom(randomBoolean() ? null : randomBoolean());
    }

    public static FrequencyEncoding createRandom(Boolean isCustom) {
        return createRandom(isCustom, randomAlphaOfLength(10));
    }

    public static FrequencyEncoding createRandom(Boolean isCustom, String inputField) {
        int valuesSize = randomIntBetween(1, 10);
        Map<String, Double> valueMap = new HashMap<>();
        for (int i = 0; i < valuesSize; i++) {
            valueMap.put(randomAlphaOfLength(10), randomDoubleBetween(0.0, 1.0, false));
        }
        return new FrequencyEncoding(inputField, randomAlphaOfLength(10), valueMap, isCustom);
    }

    @Override
    protected Writeable.Reader<FrequencyEncoding> instanceReader() {
        return FrequencyEncoding::new;
    }

    public void testProcessWithFieldPresent() {
        String field = "categorical";
        List<Object> values = Arrays.asList("foo", "bar", "foobar", "baz", "farequote", 1.5);
        Map<String, Double> valueMap = values.stream()
            .collect(Collectors.toMap(Object::toString, v -> randomDoubleBetween(0.0, 1.0, false)));
        String encodedFeatureName = "encoded";
        FrequencyEncoding encoding = new FrequencyEncoding(field, encodedFeatureName, valueMap, false);
        Object fieldValue = randomFrom(values);
        Map<String, Matcher<? super Object>> matchers = Collections.singletonMap(
            encodedFeatureName,
            equalTo(valueMap.get(fieldValue.toString()))
        );
        Map<String, Object> fieldValues = randomFieldValues(field, fieldValue);
        testProcess(encoding, fieldValues, matchers);

        // Test where the value is some unknown Value
        fieldValues = randomFieldValues(field, "unknownValue");
        fieldValues.put(field, "unknownValue");
        matchers = Collections.singletonMap(encodedFeatureName, equalTo(0.0));
        testProcess(encoding, fieldValues, matchers);
    }

    public void testInputOutputFields() {
        String field = randomAlphaOfLength(10);
        List<Object> values = Arrays.asList("foo", "bar", "foobar", "baz", "farequote", 1.5);
        Map<String, Double> valueMap = values.stream()
            .collect(Collectors.toMap(Object::toString, v -> randomDoubleBetween(0.0, 1.0, false)));
        String encodedFeatureName = randomAlphaOfLength(10);
        FrequencyEncoding encoding = new FrequencyEncoding(field, encodedFeatureName, valueMap, false);
        assertThat(encoding.inputFields(), containsInAnyOrder(field));
        assertThat(encoding.outputFields(), containsInAnyOrder(encodedFeatureName));
    }

}
