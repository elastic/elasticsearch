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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class FrequencyEncodingTests extends PreProcessingTests<FrequencyEncoding> {

    @Override
    protected FrequencyEncoding doParseInstance(XContentParser parser) throws IOException {
        return lenient ? FrequencyEncoding.fromXContentLenient(parser) : FrequencyEncoding.fromXContentStrict(parser);
    }

    @Override
    protected FrequencyEncoding createTestInstance() {
        return createRandom();
    }

    public static FrequencyEncoding createRandom() {
        int valuesSize = randomIntBetween(1, 10);
        Map<String, Double> valueMap = new HashMap<>();
        for (int i = 0; i < valuesSize; i++) {
            valueMap.put(randomAlphaOfLength(10), randomDoubleBetween(0.0, 1.0, false));
        }
        return new FrequencyEncoding(randomAlphaOfLength(10), randomAlphaOfLength(10), valueMap);
    }

    @Override
    protected Writeable.Reader<FrequencyEncoding> instanceReader() {
        return FrequencyEncoding::new;
    }

    public void testProcessWithFieldPresent() {
        String field = "categorical";
        List<Object> values = Arrays.asList("foo", "bar", "foobar", "baz", "farequote", 1.5);
        Map<String, Double> valueMap = values.stream().collect(Collectors.toMap(Object::toString,
            v -> randomDoubleBetween(0.0, 1.0, false)));
        String encodedFeatureName = "encoded";
        FrequencyEncoding encoding = new FrequencyEncoding(field, encodedFeatureName, valueMap);
        Object fieldValue = randomFrom(values);
        Map<String, Matcher<? super Object>> matchers = Collections.singletonMap(encodedFeatureName,
            equalTo(valueMap.get(fieldValue.toString())));
        Map<String, Object> fieldValues = randomFieldValues(field, fieldValue);
        testProcess(encoding, fieldValues, matchers);

        // Test where the value is some unknown Value
        fieldValues = randomFieldValues(field, "unknownValue");
        fieldValues.put(field, "unknownValue");
        matchers = Collections.singletonMap(encodedFeatureName, equalTo(0.0));
        testProcess(encoding, fieldValues, matchers);
    }

    public void testProcessWithNestedField() {
        String field = "categorical.child";
        List<Object> values = Arrays.asList("foo", "bar", "foobar", "baz", "farequote", 1.5);
        Map<String, Double> valueMap = values.stream().collect(Collectors.toMap(Object::toString,
            v -> randomDoubleBetween(0.0, 1.0, false)));
        String encodedFeatureName = "encoded";
        FrequencyEncoding encoding = new FrequencyEncoding(field, encodedFeatureName, valueMap);

        Map<String, Object> fieldValues = new HashMap<>() {{
            put("categorical", new HashMap<>(){{
                put("child", "farequote");
            }});
        }};

        encoding.process(fieldValues);
        assertThat(fieldValues.get("encoded"), equalTo(valueMap.get("farequote")));
    }

}
