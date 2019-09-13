/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.preprocessing;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class FrequencyEncodingTests extends AbstractSerializingTestCase<FrequencyEncoding> {

    @Override
    protected FrequencyEncoding doParseInstance(XContentParser parser) throws IOException {
        return FrequencyEncoding.fromXContent(parser);
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
        return new FrequencyEncoding(randomAlphaOfLength(10), valueMap);
    }

    @Override
    protected Writeable.Reader<FrequencyEncoding> instanceReader() {
        return FrequencyEncoding::new;
    }

    public void testProcess() {
        String field = "categorical";
        List<String> values = Arrays.asList("foo", "bar", "foobar", "baz", "farequote");
        Map<String, Double> valueMap = values.stream().collect(Collectors.toMap(Function.identity(),
            v -> randomDoubleBetween(0.0, 1.0, false)));
        FrequencyEncoding encoding = new FrequencyEncoding(field, valueMap);
        for(int i = 0; i < NUMBER_OF_TEST_RUNS; i++) {
            int numFields = randomIntBetween(1, 5);
            Map<String, Object> fieldValues = new HashMap<>(numFields);
            for (int k = 0; k < numFields; k++) {
                fieldValues.put(randomAlphaOfLength(10), randomAlphaOfLength(10));
            }
            boolean addedField = randomBoolean();
            String addedValue = null;
            if (addedField) {
                addedValue = randomBoolean() ? "unknownValue" : randomFrom(values);
                fieldValues.put(field, addedValue);
            }
            Map<String, Object> resultFields = encoding.process(fieldValues);
            if (addedField) {
                assertThat(resultFields.get(field), equalTo(valueMap.getOrDefault(addedValue, 0.0)));
            }
        }
    }
}
